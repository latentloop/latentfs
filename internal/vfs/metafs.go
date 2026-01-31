// Copyright 2024 LatentFS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vfs

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/macos-fuse-t/go-smb2/vfs"
	log "github.com/sirupsen/logrus"

	"latentfs/internal/storage"
)

// recoverPanic recovers from panics and converts them to errors
// This is CRITICAL for preventing SMB server disconnections
func recoverPanic(operation string, err *error) {
	if r := recover(); r != nil {
		log.Errorf("[MetaFS] PANIC RECOVERED in %s: %v\nStack:\n%s", operation, r, debug.Stack())
		if err != nil {
			*err = fmt.Errorf("internal error in %s: %v", operation, r)
		}
	}
}

// MetaFS implements vfs.VFSFileSystem for the central metadata mount
// It delegates operations to sub-VFSes based on path mappings
type MetaFS struct {
	mu        sync.RWMutex
	metaFile  *storage.MetaFile
	subFS     map[string]*LatentFS // subpath -> VFS
	handles   *MetaHandleManager
	resolver  *SourceResolver // Shared resolver for all soft-fork VFSes
	startTime time.Time       // Stable timestamp for root/mount attributes (NFS clients interpret changing mtime as directory modification)
}

// metaHandle represents an open handle in MetaFS
type metaHandle struct {
	isRoot      bool          // Is this the root directory?
	subPath     string        // Which sub-mount this belongs to (empty for root)
	subFS       *LatentFS     // The sub-VFS (nil for root)
	subHandle   vfs.VfsHandle // Handle in the sub-VFS
	dirEnumDone bool          // For root dir: has enumeration completed?
}

// MetaHandleManager manages handles for MetaFS
type MetaHandleManager struct {
	mu         sync.RWMutex
	handles    map[HandleID]*metaHandle
	nextHandle HandleID
}

// NewMetaHandleManager creates a new handle manager
func NewMetaHandleManager() *MetaHandleManager {
	return &MetaHandleManager{
		handles:    make(map[HandleID]*metaHandle),
		nextHandle: 1,
	}
}

func (hm *MetaHandleManager) Allocate(h *metaHandle) HandleID {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	id := hm.nextHandle
	hm.nextHandle++
	hm.handles[id] = h
	return id
}

func (hm *MetaHandleManager) Get(id HandleID) (*metaHandle, bool) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	h, ok := hm.handles[id]
	return h, ok
}

func (hm *MetaHandleManager) Release(id HandleID) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	delete(hm.handles, id)
}

func (hm *MetaHandleManager) IsDirEnumDone(id HandleID) bool {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	h, ok := hm.handles[id]
	if !ok {
		return false
	}
	return h.dirEnumDone
}

func (hm *MetaHandleManager) SetDirEnumDone(id HandleID, done bool) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	if h, ok := hm.handles[id]; ok {
		h.dirEnumDone = done
	}
}

// NewMetaFS creates a new MetaFS backed by the given metadata file
func NewMetaFS(metaFile *storage.MetaFile) *MetaFS {
	fs := &MetaFS{
		metaFile:  metaFile,
		subFS:     make(map[string]*LatentFS),
		handles:   NewMetaHandleManager(),
		startTime: time.Now(),
	}
	// Create shared resolver for all soft-fork VFSes
	fs.resolver = NewSourceResolver(fs)
	return fs
}

// AddSubMount adds a sub-mount to the MetaFS
func (fs *MetaFS) AddSubMount(subPath string, dataFile *storage.DataFile) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Add to metadata file
	if err := fs.metaFile.AddMount(subPath, dataFile.Path()); err != nil {
		return err
	}

	// Create sub-VFS
	fs.subFS[subPath] = NewLatentFS(dataFile)
	return nil
}

// AddForkMount adds a fork mount to the MetaFS
// sourceFolder is stored for metadata (origin tracking)
// forkType determines if COW is enabled (only for soft-forks)
// symlinkTarget is stored and used by VFS to compute hidden paths for loop prevention
func (fs *MetaFS) AddForkMount(subPath string, dataFile *storage.DataFile, sourceFolder, forkType, symlinkTarget string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Add to metadata file with fork info
	if err := fs.metaFile.AddForkMount(subPath, dataFile.Path(), sourceFolder, forkType, symlinkTarget); err != nil {
		return err
	}

	// Create sub-VFS with copy-on-write semantics only for soft-forks with a source folder
	if forkType == storage.ForkTypeSoft && sourceFolder != "" {
		// Use resolver-based constructor - resolver handles cascaded fork detection dynamically
		// No need to detect cascaded forks at mount time anymore
		fs.subFS[subPath] = NewForkLatentFSWithResolver(dataFile, sourceFolder, symlinkTarget, fs.resolver)
	} else {
		// For hard-forks and regular mounts, no COW - just use the data file directly
		fs.subFS[subPath] = NewLatentFS(dataFile)
	}
	return nil
}

// IsDataFileMounted checks if a data file is already mounted
// Returns the existing mount entry if found, nil otherwise
func (fs *MetaFS) IsDataFileMounted(dataFilePath string) (*storage.MountEntry, error) {
	return fs.metaFile.GetMountByDataFile(dataFilePath)
}

// GetMountBySymlinkTarget finds a mount entry by symlink target path
// Returns nil if no mount is found for the given symlink target
func (fs *MetaFS) GetMountBySymlinkTarget(symlinkTarget string) (*storage.MountEntry, error) {
	return fs.metaFile.GetMountBySymlinkTarget(symlinkTarget)
}

// GetVFSByDataFile returns the LatentFS for a given data file path
// Returns nil if the data file is not mounted
func (fs *MetaFS) GetVFSByDataFile(dataFilePath string) *LatentFS {
	// First find the mount entry to get the subPath
	entry, err := fs.metaFile.GetMountByDataFile(dataFilePath)
	if err != nil || entry == nil {
		return nil
	}

	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.subFS[entry.SubPath]
}

// GetVFSBySymlinkTarget returns the LatentFS for a given symlink target path
// Used by SourceResolver to check if a source path is a mounted VFS
// Returns nil if the path is not a mounted VFS
func (fs *MetaFS) GetVFSBySymlinkTarget(symlinkTarget string) *LatentFS {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	log.Tracef("[MetaFS] GetVFSBySymlinkTarget: looking for %q, subFS count=%d", symlinkTarget, len(fs.subFS))
	for subPath, subFS := range fs.subFS {
		log.Tracef("[MetaFS] GetVFSBySymlinkTarget: checking subPath=%q symlinkTarget=%q sourceFolder=%q",
			subPath, subFS.symlinkTarget, subFS.sourceFolder)
		if subFS.symlinkTarget != "" && subFS.symlinkTarget == symlinkTarget {
			log.Tracef("[MetaFS] GetVFSBySymlinkTarget: FOUND match for %q", symlinkTarget)
			return subFS
		}
	}
	log.Tracef("[MetaFS] GetVFSBySymlinkTarget: no match found for %q", symlinkTarget)
	return nil
}


// ListMounts returns all mount entries from the metadata file
func (fs *MetaFS) ListMounts() ([]storage.MountEntry, error) {
	return fs.metaFile.ListMounts()
}

// InvalidateCacheForDataFile invalidates all caches for a specific data file's LatentFS.
// This should be called after external modifications (e.g., restore command)
// to ensure the NFS mount reflects the latest state from the DataFile.
// Clears: attrCache, lookupCache, parentInodeCache, and refreshes epochs.
func (fs *MetaFS) InvalidateCacheForDataFile(dataFilePath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find the subFS for this data file
	for subPath, subFS := range fs.subFS {
		if subFS.DataFile().Path() == dataFilePath {
			log.Debugf("[MetaFS] InvalidateCacheForDataFile: found mount at %s, invalidating caches", subPath)
			subFS.InvalidateCache()
			return nil
		}
	}

	// Not found in cache - check if it's mounted but not loaded
	entry, err := fs.metaFile.GetMountByDataFile(dataFilePath)
	if err != nil {
		return err
	}
	if entry == nil {
		return fmt.Errorf("data file not mounted: %s", dataFilePath)
	}

	// Mount exists but LatentFS not loaded - nothing to invalidate
	log.Debugf("[MetaFS] InvalidateCacheForDataFile: mount exists but not loaded, nothing to invalidate")
	return nil
}

// RemoveSubMount removes a sub-mount from the MetaFS
func (fs *MetaFS) RemoveSubMount(subPath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Remove from metadata file
	if err := fs.metaFile.RemoveMount(subPath); err != nil {
		return err
	}

	// Close and remove sub-VFS
	if subFS, ok := fs.subFS[subPath]; ok {
		subFS.DataFile().Close()
		delete(fs.subFS, subPath)
	}
	return nil
}

// CloseSubMountPreserveMetadata closes a sub-mount's VFS but keeps the metadata entry.
// Used when mount_on_start is enabled to preserve mount info across daemon restarts.
func (fs *MetaFS) CloseSubMountPreserveMetadata(subPath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Only close and remove sub-VFS from memory, keep metadata intact
	if subFS, ok := fs.subFS[subPath]; ok {
		subFS.DataFile().Close()
		delete(fs.subFS, subPath)
	}
	return nil
}

// LoadSubMountFromMetadata loads a mount from existing metadata entry.
// Used when mount_on_start is enabled to restore mounts on daemon startup.
// Returns the mount entry if successfully loaded, nil if data file cannot be opened.
func (fs *MetaFS) LoadSubMountFromMetadata(entry *storage.MountEntry) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check if already loaded
	if _, ok := fs.subFS[entry.SubPath]; ok {
		return nil // Already loaded
	}

	// Open the data file with daemon context
	dataFile, err := storage.OpenWithContext(entry.DataFile, storage.DBContextDaemon)
	if err != nil {
		return fmt.Errorf("failed to open data file %s: %w", entry.DataFile, err)
	}

	// Create sub-VFS (use fork VFS only for soft-forks with source folder)
	var subFS *LatentFS
	if entry.ForkType == storage.ForkTypeSoft && entry.SourceFolder != "" {
		subFS = NewForkLatentFSWithResolver(dataFile, entry.SourceFolder, entry.SymlinkTarget, fs.resolver)
	} else {
		subFS = NewLatentFS(dataFile)
	}
	fs.subFS[entry.SubPath] = subFS
	return nil
}

// GetSubFS returns the sub-VFS for a path, opening it if needed
func (fs *MetaFS) GetSubFS(subPath string) (*LatentFS, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if subFS, ok := fs.subFS[subPath]; ok {
		return subFS, nil
	}

	// Look up in metadata file
	entry, _, err := fs.metaFile.LookupMount(subPath)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, ENOENT
	}

	// Open the data file with daemon context
	dataFile, err := storage.OpenWithContext(entry.DataFile, storage.DBContextDaemon)
	if err != nil {
		return nil, EIO
	}

	// Create and cache sub-VFS (use fork VFS only for soft-forks with source folder)
	var subFS *LatentFS
	if entry.ForkType == storage.ForkTypeSoft && entry.SourceFolder != "" {
		// Use resolver-based constructor - resolver handles cascaded fork detection
		subFS = NewForkLatentFSWithResolver(dataFile, entry.SourceFolder, entry.SymlinkTarget, fs.resolver)
	} else {
		// For hard-forks and regular mounts, no COW
		subFS = NewLatentFS(dataFile)
	}
	fs.subFS[subPath] = subFS
	return subFS, nil
}

// resolvePath splits a path into subPath and remaining path
func (fs *MetaFS) resolvePath(path string) (subPath string, remaining string, subFS *LatentFS, err error) {
	// Normalize path
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return "", "", nil, nil // Root
	}

	// Find first component
	parts := strings.SplitN(path, "/", 2)
	subPath = parts[0]
	if len(parts) > 1 {
		remaining = parts[1]
	}

	// Get sub-VFS
	subFS, err = fs.GetSubFS(subPath)
	if err != nil {
		return "", "", nil, err
	}

	return subPath, remaining, subFS, nil
}

// --- VFS Operations ---
// All operations have panic recovery to prevent SMB server disconnections

func (fs *MetaFS) Open(path string, flags int, mode int) (handle vfs.VfsHandle, err error) {
	defer recoverPanic("Open", &err)
	log.Debugf("[MetaFS] Open: path=%q flags=%d mode=%d", path, flags, mode)

	subPath, remaining, subFS, err := fs.resolvePath(path)
	if err != nil {
		return 0, err
	}
	if subFS == nil {
		return 0, EISDIR // Root is a directory
	}

	// Delegate to sub-VFS
	subHandle, err := subFS.Open("/"+remaining, flags, mode)
	if err != nil {
		return 0, err
	}

	h := fs.handles.Allocate(&metaHandle{
		subPath:   subPath,
		subFS:     subFS,
		subHandle: subHandle,
	})
	return vfs.VfsHandle(h), nil
}

func (fs *MetaFS) Close(handle vfs.VfsHandle) (err error) {
	defer recoverPanic("Close", &err)
	log.Debugf("[MetaFS] Close: handle=%d", handle)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return EBADF
	}
	defer fs.handles.Release(HandleID(handle))

	if h.isRoot || h.subFS == nil {
		return nil
	}
	return h.subFS.Close(h.subHandle)
}

func (fs *MetaFS) Read(handle vfs.VfsHandle, buf []byte, offset uint64, flags int) (n int, err error) {
	defer recoverPanic("Read", &err)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return 0, EBADF
	}
	if h.isRoot || h.subFS == nil {
		return 0, EISDIR
	}
	return h.subFS.Read(h.subHandle, buf, offset, flags)
}

func (fs *MetaFS) Write(handle vfs.VfsHandle, buf []byte, offset uint64, flags int) (n int, err error) {
	defer recoverPanic("Write", &err)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return 0, EBADF
	}
	if h.isRoot || h.subFS == nil {
		return 0, EISDIR
	}
	return h.subFS.Write(h.subHandle, buf, offset, flags)
}

func (fs *MetaFS) Truncate(handle vfs.VfsHandle, size uint64) (err error) {
	defer recoverPanic("Truncate", &err)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return EBADF
	}
	if h.isRoot || h.subFS == nil {
		return EISDIR
	}
	return h.subFS.Truncate(h.subHandle, size)
}

func (fs *MetaFS) FSync(handle vfs.VfsHandle) (err error) {
	defer recoverPanic("FSync", &err)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return EBADF
	}
	if h.isRoot || h.subFS == nil {
		return nil
	}
	return h.subFS.FSync(h.subHandle)
}

func (fs *MetaFS) Flush(handle vfs.VfsHandle) (err error) {
	defer recoverPanic("Flush", &err)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return EBADF
	}
	if h.isRoot || h.subFS == nil {
		return nil
	}
	return h.subFS.Flush(h.subHandle)
}

func (fs *MetaFS) Mkdir(path string, mode int) (attrs *vfs.Attributes, err error) {
	defer recoverPanic("Mkdir", &err)
	log.Debugf("[MetaFS] Mkdir: path=%q mode=%o", path, mode)

	_, remaining, subFS, err := fs.resolvePath(path)
	if err != nil {
		return nil, err
	}
	if subFS == nil {
		return nil, EEXIST // Can't create in root (only sub-mounts)
	}

	return subFS.Mkdir("/"+remaining, mode)
}

func (fs *MetaFS) OpenDir(path string) (handle vfs.VfsHandle, err error) {
	defer recoverPanic("OpenDir", &err)
	log.Debugf("[MetaFS] OpenDir: path=%q", path)

	path = strings.TrimPrefix(path, "/")
	if path == "" {
		// Root directory
		h := fs.handles.Allocate(&metaHandle{isRoot: true})
		return vfs.VfsHandle(h), nil
	}

	mountPath, remaining, subFS, err := fs.resolvePath(path)
	if err != nil {
		return 0, err
	}
	if subFS == nil {
		return 0, ENOENT
	}

	// Delegate to sub-VFS
	subHandle, err := subFS.OpenDir("/" + remaining)
	if err != nil {
		return 0, err
	}

	h := fs.handles.Allocate(&metaHandle{
		subPath:   mountPath,
		subFS:     subFS,
		subHandle: subHandle,
	})
	return vfs.VfsHandle(h), nil
}

func (fs *MetaFS) ReadDir(handle vfs.VfsHandle, offset int, count int) (entries []vfs.DirInfo, err error) {
	defer recoverPanic("ReadDir", &err)
	log.Debugf("[MetaFS] ReadDir: handle=%d offset=%d count=%d", handle, offset, count)

	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return nil, EBADF
	}

	if h.isRoot {
		// SMB2 protocol: offset > 0 (RESTART_SCANS) means restart enumeration
		// offset == 0 means continue from where we left off
		if offset > 0 {
			fs.handles.SetDirEnumDone(HandleID(handle), false)
		}

		// If we already returned all entries, return EOF
		if fs.handles.IsDirEnumDone(HandleID(handle)) {
			return nil, io.EOF
		}

		// List all sub-mounts
		entries, err := fs.readRootDir(offset, count)
		if err != nil {
			return nil, err
		}

		// Mark enumeration as done after returning entries
		fs.handles.SetDirEnumDone(HandleID(handle), true)
		return entries, nil
	}

	if h.subFS == nil {
		return nil, ENOTDIR
	}
	return h.subFS.ReadDir(h.subHandle, offset, count)
}

func (fs *MetaFS) readRootDir(offset int, count int) ([]vfs.DirInfo, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	mounts, err := fs.metaFile.ListMounts()
	if err != nil {
		log.Errorf("[MetaFS] readRootDir: ListMounts error: %v", err)
		return nil, EIO
	}

	var entries []vfs.DirInfo

	// Add . and ..
	rootAttrs := fs.rootAttributes()
	entries = append(entries,
		vfs.DirInfo{Name: ".", Attributes: *rootAttrs},
		vfs.DirInfo{Name: "..", Attributes: *rootAttrs})

	// Add sub-mounts as directories
	// Use stable timestamp — NFS clients interpret changing mtime as directory modification
	for _, m := range mounts {
		di := vfs.DirInfo{Name: m.SubPath}
		di.SetFileType(vfs.FileTypeDirectory)
		di.SetPermissions(vfs.NewPermissionsFromMode(storage.DefaultDirMode))
		di.SetUnixMode(0755)
		di.SetLastDataModificationTime(fs.startTime)
		di.SetInodeNumber(hashStringToInode(m.SubPath)) // Consistent hash-based inode number
		entries = append(entries, di)
	}

	// Apply offset (SMB restart scans)
	if offset > 0 && offset < len(entries) {
		entries = entries[offset:]
	} else if offset > 0 {
		return nil, io.EOF
	}

	if count > 0 && count < len(entries) {
		return entries[:count], nil
	}
	return entries, nil
}

// GetAttrByPath returns attributes for a file by path, avoiding Open/GetAttr/Close round-trip.
// Used by BillyMetaAdapter.Lstat for NFS operations.
func (fs *MetaFS) GetAttrByPath(path string) (attrs *vfs.Attributes, err error) {
	defer recoverPanic("GetAttrByPath", &err)

	subPath, remaining, subFS, err := fs.resolvePath(path)
	if err != nil {
		return nil, err
	}
	if subFS == nil {
		// Root directory
		return fs.rootAttributes(), nil
	}

	// Delegate to sub-VFS GetAttrByPath
	_ = subPath
	return subFS.GetAttrByPath("/" + remaining)
}

func (fs *MetaFS) GetAttr(handle vfs.VfsHandle) (attrs *vfs.Attributes, err error) {
	defer recoverPanic("GetAttr", &err)
	log.Debugf("[MetaFS] GetAttr: handle=%d", handle)

	if handle == 0 {
		return fs.rootAttributes(), nil
	}

	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return nil, EBADF
	}

	if h.isRoot {
		return fs.rootAttributes(), nil
	}

	if h.subFS == nil {
		return nil, EIO
	}
	return h.subFS.GetAttr(h.subHandle)
}

func (fs *MetaFS) SetAttr(handle vfs.VfsHandle, inAttrs *vfs.Attributes) (attrs *vfs.Attributes, err error) {
	defer recoverPanic("SetAttr", &err)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return nil, EBADF
	}

	if h.isRoot {
		return fs.rootAttributes(), nil // Root is read-only
	}

	if h.subFS == nil {
		return nil, EIO
	}
	return h.subFS.SetAttr(h.subHandle, inAttrs)
}

func (fs *MetaFS) Lookup(dirHandle vfs.VfsHandle, name string) (attrs *vfs.Attributes, err error) {
	defer recoverPanic("Lookup", &err)
	log.Debugf("[MetaFS] Lookup: dirHandle=%d name=%q", dirHandle, name)

	// Handle root indicator names (looking up root itself)
	if name == "" || name == "/" || name == "." {
		return fs.rootAttributes(), nil
	}

	// Get handle info if dirHandle != 0
	var h *metaHandle
	if dirHandle != 0 {
		var ok bool
		h, ok = fs.handles.Get(HandleID(dirHandle))
		if !ok {
			return nil, EBADF
		}
	}

	// If dirHandle == 0 or handle points to root, look up in root directory
	if dirHandle == 0 || h == nil || h.isRoot {
		// Looking up in root - name should be a sub-mount
		name = strings.TrimPrefix(name, "/")
		if name == "" {
			return fs.rootAttributes(), nil
		}

		parts := strings.SplitN(name, "/", 2)
		subPath := parts[0]

		subFS, err := fs.GetSubFS(subPath)
		if err != nil {
			return nil, err
		}

		if len(parts) == 1 {
			// Just the sub-mount itself - return directory attrs
			resultAttrs := &vfs.Attributes{}
			resultAttrs.SetFileType(vfs.FileTypeDirectory)
			resultAttrs.SetPermissions(vfs.NewPermissionsFromMode(storage.DefaultDirMode))
			resultAttrs.SetUnixMode(0755)
			resultAttrs.SetLastDataModificationTime(fs.startTime)
			// Generate inode number from subPath hash (reserve low numbers for special use)
			resultAttrs.SetInodeNumber(hashStringToInode(subPath))
			return resultAttrs, nil
		}

		// Delegate to sub-VFS for remaining path
		return subFS.Lookup(0, "/"+parts[1])
	}

	if h.subFS == nil {
		return nil, EIO
	}
	return h.subFS.Lookup(h.subHandle, name)
}

func (fs *MetaFS) StatFS(handle vfs.VfsHandle) (fsAttrs *vfs.FSAttributes, err error) {
	defer recoverPanic("StatFS", &err)
	fsAttrs = &vfs.FSAttributes{}
	fsAttrs.SetBlockSize(4096)
	fsAttrs.SetIOSize(4096)
	fsAttrs.SetBlocks(1000000)
	fsAttrs.SetFreeBlocks(500000)
	fsAttrs.SetAvailableBlocks(500000)
	fsAttrs.SetFiles(100000)
	fsAttrs.SetFreeFiles(50000)
	return fsAttrs, nil
}

func (fs *MetaFS) Unlink(handle vfs.VfsHandle) (err error) {
	defer recoverPanic("Unlink", &err)
	log.Debugf("[MetaFS] Unlink: handle=%d", handle)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		log.Debugf("[MetaFS] Unlink: handle not found")
		return EBADF
	}
	log.Debugf("[MetaFS] Unlink: subPath=%q subHandle=%d isRoot=%v hasSubFS=%v", h.subPath, h.subHandle, h.isRoot, h.subFS != nil)
	if h.isRoot || h.subFS == nil {
		log.Debugf("[MetaFS] Unlink: returning ENOTSUP (isRoot=%v, subFS==nil=%v)", h.isRoot, h.subFS == nil)
		return ENOTSUP
	}
	err = h.subFS.Unlink(h.subHandle)
	if err != nil {
		log.Debugf("[MetaFS] Unlink: subFS.Unlink failed: %v", err)
	} else {
		log.Debugf("[MetaFS] Unlink: success")
	}
	return err
}

func (fs *MetaFS) Rename(handle vfs.VfsHandle, newName string, flags int) (err error) {
	defer recoverPanic("Rename", &err)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return EBADF
	}
	if h.isRoot || h.subFS == nil {
		return ENOTSUP
	}
	return h.subFS.Rename(h.subHandle, newName, flags)
}

func (fs *MetaFS) Readlink(handle vfs.VfsHandle) (target string, err error) {
	defer recoverPanic("Readlink", &err)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return "", EBADF
	}
	if h.isRoot || h.subFS == nil {
		return "", EINVAL
	}
	return h.subFS.Readlink(h.subHandle)
}

func (fs *MetaFS) Symlink(handle vfs.VfsHandle, target string, mode int) (attrs *vfs.Attributes, err error) {
	defer recoverPanic("Symlink", &err)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return nil, EBADF
	}
	if h.isRoot || h.subFS == nil {
		return nil, ENOTSUP
	}
	return h.subFS.Symlink(h.subHandle, target, mode)
}

func (fs *MetaFS) Link(srcNode vfs.VfsNode, dstNode vfs.VfsNode, name string) (attrs *vfs.Attributes, err error) {
	defer recoverPanic("Link", &err)
	return nil, ENOTSUP
}

func (fs *MetaFS) Listxattr(handle vfs.VfsHandle) (names []string, err error) {
	defer recoverPanic("Listxattr", &err)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return nil, EBADF
	}
	if h.isRoot || h.subFS == nil {
		return []string{}, nil
	}
	return h.subFS.Listxattr(h.subHandle)
}

func (fs *MetaFS) Getxattr(handle vfs.VfsHandle, name string, buf []byte) (n int, err error) {
	defer recoverPanic("Getxattr", &err)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return 0, EBADF
	}
	if h.isRoot || h.subFS == nil {
		return 0, ENOATTR
	}
	return h.subFS.Getxattr(h.subHandle, name, buf)
}

func (fs *MetaFS) Setxattr(handle vfs.VfsHandle, name string, value []byte) (err error) {
	defer recoverPanic("Setxattr", &err)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return EBADF
	}
	if h.isRoot || h.subFS == nil {
		return nil
	}
	return h.subFS.Setxattr(h.subHandle, name, value)
}

func (fs *MetaFS) Removexattr(handle vfs.VfsHandle, name string) (err error) {
	defer recoverPanic("Removexattr", &err)
	h, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return EBADF
	}
	if h.isRoot || h.subFS == nil {
		return nil
	}
	return h.subFS.Removexattr(h.subHandle, name)
}

// rootAttributes returns attributes for the root directory
func (fs *MetaFS) rootAttributes() *vfs.Attributes {
	attrs := &vfs.Attributes{}
	attrs.SetFileType(vfs.FileTypeDirectory)
	attrs.SetFileHandle(vfs.VfsNode(1))
	attrs.SetInodeNumber(1)
	attrs.SetPermissions(vfs.NewPermissionsFromMode(storage.DefaultDirMode))
	attrs.SetUnixMode(0755)
	attrs.SetLinkCount(2)
	attrs.SetUID(uint32(os.Getuid()))
	attrs.SetGID(uint32(os.Getgid()))
	// Use stable timestamp — NFS clients interpret changing mtime as directory
	// modification, which causes infinite path revalidation loops with noac mounts
	attrs.SetLastDataModificationTime(fs.startTime)
	attrs.SetLastStatusChangeTime(fs.startTime)
	attrs.SetAccessTime(fs.startTime)
	attrs.SetBirthTime(fs.startTime)
	return attrs
}

// hashStringToInode generates a deterministic inode number from a string.
// Uses FNV-1a hash with high bit set to avoid collision with regular inodes.
func hashStringToInode(s string) uint64 {
	const fnvPrime = 1099511628211
	const fnvOffset = 14695981039346656037
	hash := uint64(fnvOffset)
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= fnvPrime
	}
	// Set high bit to distinguish from regular inodes (which start from low numbers)
	return hash | 0x8000000000000000
}
