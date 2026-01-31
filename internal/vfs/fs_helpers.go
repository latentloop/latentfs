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
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uptrace/bun"

	"github.com/macos-fuse-t/go-smb2/vfs"

	"latentfs/internal/storage"
	"latentfs/internal/common"
)

// =============================================================================
// Panic Recovery
// =============================================================================

// recoverLatentFSPanic recovers from panics in LatentFS operations
// This is CRITICAL for preventing SMB server disconnections
func recoverLatentFSPanic(operation string, err *error) {
	if r := recover(); r != nil {
		log.Errorf("[LatentFS] PANIC RECOVERED in %s: %v\nStack:\n%s", operation, r, debug.Stack())
		if err != nil {
			*err = EIO
		}
	}
}

// =============================================================================
// Cache Helpers
// =============================================================================

// getCachedAttr returns cached attributes for a path, or nil if not found/expired.
func (fs *LatentFS) getCachedAttr(path string) *vfs.Attributes {
	if fs.attrCache == nil {
		return nil
	}
	return fs.attrCache.Get(path)
}

// cacheAttr stores attributes in the cache.
// isSource should be true if the attributes are from source folder (subject to TTL).
func (fs *LatentFS) cacheAttr(path string, attrs *vfs.Attributes, isSource bool) {
	if fs.attrCache != nil {
		fs.attrCache.Set(path, attrs, isSource)
	}
}

// invalidateCacheInternal refreshes epoch state without acquiring lock (caller must hold lock).
// Under WAL mode, the existing connection sees committed changes — no need to
// close/reopen the database. RefreshEpochs re-reads epoch state and clears
// lookup + parent inode caches.
func (fs *LatentFS) invalidateCacheInternal() {
	log.Infof("[VFS] invalidateCacheInternal: refreshing all caches for %s", fs.dataFile.Path())

	// Invalidate attribute cache (path → attributes)
	if fs.attrCache != nil {
		fs.attrCache.Invalidate()
	}

	// Invalidate DataFile caches (lookupCache, parentInodeCache)
	fs.dataFile.InvalidateCache()

	// Refresh epochs from database
	if err := fs.dataFile.RefreshEpochs(); err != nil {
		log.Errorf("[VFS] invalidateCacheInternal: RefreshEpochs failed: %v", err)
	}
}

// =============================================================================
// Source Folder Helpers - Path Checking
// =============================================================================

// isPathTombstoned checks if any component of vfsPath has a dentry tombstone in the overlay.
// This is used to block source-folder fallback for entries that were explicitly deleted.
// Must be called with fs.mu held (read or write).
func (fs *LatentFS) isPathTombstoned(vfsPath string) bool {
	parts := common.SplitPath(vfsPath)
	if len(parts) == 0 {
		return false
	}
	currentIno := int64(storage.RootIno)
	for _, part := range parts {
		if fs.dataFile.IsDentryTombstoned(currentIno, part) {
			return true
		}
		// Try to traverse to next component via overlay
		dentry, err := fs.dataFile.Lookup(currentIno, part)
		if err != nil {
			// Component not in overlay — not tombstoned at this level
			return false
		}
		currentIno = dentry.Ino
	}
	return false
}

// =============================================================================
// Source Folder Helpers - Open Operations
// =============================================================================

// openFromSource opens a file from the source folder (for soft-fork)
// Returns source-only handle if file exists in source
func (fs *LatentFS) openFromSource(vfsPath string, flags int, mode int) (vfs.VfsHandle, error) {
	// Use resolver only for cascaded forks (source folder is itself a VFS mount)
	if fs.isSourceCascaded() {
		return fs.openFromSourceWithResolver(vfsPath, flags)
	}

	// Fallback: Check if this path should be hidden (single-level)
	if fs.isPathHidden(vfsPath) {
		log.Debugf("[VFS] openFromSource: hiding %q (target inside source)", vfsPath)
		return 0, ENOENT
	}

	sourcePath := fs.sourceFolder + "/" + vfsPath

	// Use Lstat to not follow symlinks - preserves symlink as symlink
	info, err := os.Lstat(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ENOENT
		}
		return 0, EIO
	}

	// Symlinks and regular files are opened as source-only
	// Only directories need special handling
	if info.IsDir() {
		return 0, EISDIR
	}

	// If writing, need to copy-on-write first
	if flags&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_TRUNC) != 0 {
		// Copy file from source to overlay before writing
		if err := fs.copyOnWrite(vfsPath); err != nil {
			return 0, err
		}
		// Now open from overlay
		ino, err := fs.dataFile.ResolvePath(vfsPath)
		if err != nil {
			return 0, EIO
		}
		h := fs.handles.Allocate(ino, vfsPath, false, flags, fs.dataFile.GetReadableEpoch())
		return vfs.VfsHandle(h), nil
	}

	// Read-only: create source-only handle
	h := fs.handles.AllocateSourceOnly(vfsPath, sourcePath, false, flags)
	return vfs.VfsHandle(h), nil
}

// openFromSourceWithResolver opens a file from source using the resolver
func (fs *LatentFS) openFromSourceWithResolver(vfsPath string, flags int) (vfs.VfsHandle, error) {
	log.Debugf("[VFS] openFromSourceWithResolver: path=%q flags=%d", vfsPath, flags)

	// Check if file exists in source (with cascade-aware hiding)
	_, isDir, err := fs.resolver.StatWithHiding(fs.sourceFolder, vfsPath, fs.symlinkTarget)
	if err != nil {
		return 0, err
	}

	if isDir {
		return 0, EISDIR
	}

	// If writing, need to copy-on-write first
	if flags&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_TRUNC) != 0 {
		// Copy file from source to overlay before writing
		if err := fs.copyOnWrite(vfsPath); err != nil {
			return 0, err
		}
		// Now open from overlay
		ino, err := fs.dataFile.ResolvePath(vfsPath)
		if err != nil {
			return 0, EIO
		}
		h := fs.handles.Allocate(ino, vfsPath, false, flags, fs.dataFile.GetReadableEpoch())
		return vfs.VfsHandle(h), nil
	}

	// Read-only: create a resolver-based handle
	// We use AllocateSourceOnly but with a special marker that means "use resolver"
	// For now, we store the source folder path - the Read method will use resolver
	sourcePath := fs.sourceFolder + "/" + vfsPath
	h := fs.handles.AllocateSourceOnly(vfsPath, sourcePath, false, flags)
	return vfs.VfsHandle(h), nil
}

// openDirFromSource opens a directory from source folder
func (fs *LatentFS) openDirFromSource(vfsPath string) (vfs.VfsHandle, error) {
	// Use resolver only for cascaded forks
	if fs.isSourceCascaded() {
		// Check if directory exists in source (uses cascade-aware hiding)
		_, isDir, err := fs.resolver.StatWithHiding(fs.sourceFolder, vfsPath, fs.symlinkTarget)
		if err != nil {
			return 0, err
		}
		if !isDir {
			return 0, ENOTDIR
		}
		// Allocate source-only handle for directory
		sourcePath := fs.sourceFolder + "/" + vfsPath
		h := fs.handles.AllocateSourceOnly(vfsPath, sourcePath, true, os.O_RDONLY)
		return vfs.VfsHandle(h), nil
	}

	// Fallback: Check if this path should be hidden (single-level)
	if fs.isPathHidden(vfsPath) {
		log.Debugf("[VFS] openDirFromSource: hiding %q (target inside source)", vfsPath)
		return 0, ENOENT
	}

	sourcePath := fs.sourceFolder + "/" + vfsPath

	info, err := os.Stat(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ENOENT
		}
		return 0, EIO
	}

	if !info.IsDir() {
		return 0, ENOTDIR
	}

	// Allocate source-only handle for directory
	h := fs.handles.AllocateSourceOnly(vfsPath, sourcePath, true, os.O_RDONLY)
	return vfs.VfsHandle(h), nil
}

// openAnyFromSource opens a file or directory from source folder.
// Must be called with fs.mu held.
func (fs *LatentFS) openAnyFromSource(vfsPath string, flags int, mode int) (vfs.VfsHandle, error) {
	if fs.isSourceCascaded() {
		_, isDir, err := fs.resolver.StatWithHiding(fs.sourceFolder, vfsPath, fs.symlinkTarget)
		if err != nil {
			return 0, err
		}
		if isDir {
			sourcePath := fs.sourceFolder + "/" + vfsPath
			h := fs.handles.AllocateSourceOnly(vfsPath, sourcePath, true, os.O_RDONLY)
			return vfs.VfsHandle(h), nil
		}
		// File — delegate to existing resolver-based open
		return fs.openFromSourceWithResolver(vfsPath, flags)
	}

	// Direct source access
	if fs.isPathHidden(vfsPath) {
		return 0, ENOENT
	}

	sourcePath := fs.sourceFolder + "/" + vfsPath
	info, err := os.Lstat(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ENOENT
		}
		return 0, EIO
	}

	if info.IsDir() {
		h := fs.handles.AllocateSourceOnly(vfsPath, sourcePath, true, os.O_RDONLY)
		return vfs.VfsHandle(h), nil
	}

	// File — delegate to existing source open logic
	return fs.openFromSource(vfsPath, flags, mode)
}

// =============================================================================
// Source Folder Helpers - Copy-on-Write
// =============================================================================

// copyOnWrite copies a file from source folder to overlay (for first write).
// Uses resolver if available, otherwise direct filesystem access.
func (fs *LatentFS) copyOnWrite(vfsPath string) error {
	log.Debugf("[VFS] copyOnWrite: copying %s from source to overlay", vfsPath)

	// Get source file info and content based on resolver availability
	var srcMode os.FileMode
	var srcData []byte
	var err error

	if fs.isSourceCascaded() {
		srcInfo, err := fs.resolver.StatWithInfo(fs.sourceFolder, vfsPath)
		if err != nil {
			log.Debugf("[VFS] copyOnWrite: failed to stat source via resolver: %v", err)
			return EIO
		}
		srcMode = srcInfo.Mode()

		srcData, err = fs.resolver.ReadFileWithHiding(fs.sourceFolder, vfsPath, fs.symlinkTarget)
		if err != nil {
			return err
		}
	} else {
		sourcePath := fs.sourceFolder + "/" + vfsPath
		srcInfo, err := os.Lstat(sourcePath)
		if err != nil {
			return EIO
		}
		srcMode = srcInfo.Mode()

		srcData, err = os.ReadFile(sourcePath)
		if err != nil {
			return EIO
		}
	}

	// Ensure parent directory exists in overlay
	parentPath := common.ParentPath(vfsPath)
	if _, err = fs.ensureParentInOverlayLocked(parentPath); err != nil {
		return err
	}

	// Create file in overlay
	name := common.BaseName(vfsPath)
	parentIno, err := fs.dataFile.ResolvePath(parentPath)
	if err != nil {
		return EIO
	}

	// Create inode with source file's mode
	fileMode := uint32(storage.ModeFile | (int(srcMode) & 0777))
	ino, err := fs.dataFile.CreateInode(fileMode)
	if err != nil {
		return EIO
	}

	// Create dentry
	if err := fs.dataFile.CreateDentry(parentIno, name, ino); err != nil {
		fs.dataFile.DeleteInode(ino)
		return EIO
	}

	// Write content to overlay
	if len(srcData) > 0 {
		if err := fs.dataFile.WriteContent(ino, 0, srcData); err != nil {
			return EIO
		}
	}

	return nil
}

// createFile creates a new file (called from Open with O_CREATE)
func (fs *LatentFS) createFile(path string, mode int) (vfs.VfsHandle, error) {
	log.Debugf("[VFS] createFile: path=%q mode=%o", path, mode)
	parentPath := common.ParentPath(path)
	name := common.BaseName(path)

	if name == "" {
		return 0, EINVAL
	}

	// Resolve parent
	parentIno, err := fs.dataFile.ResolvePath(parentPath)
	if err != nil {
		return 0, ENOENT
	}

	// Create inode
	fileMode := uint32(storage.ModeFile | (mode & 0777))
	ino, err := fs.dataFile.CreateInode(fileMode)
	if err != nil {
		return 0, EIO
	}

	// Create dentry
	if err := fs.dataFile.CreateDentry(parentIno, name, ino); err != nil {
		fs.dataFile.DeleteInode(ino)
		return 0, EIO
	}

	// Invalidate caches for new file and parent directory
	fs.InvalidateAttrPathAndParent(path)

	// Allocate handle - pin epoch at open time for read consistency
	handle := fs.handles.Allocate(ino, path, false, os.O_CREATE|os.O_RDWR, fs.dataFile.GetReadableEpoch())
	return vfs.VfsHandle(handle), nil
}

// =============================================================================
// Source Folder Helpers - Read Operations
// =============================================================================

// readFromResolver reads data from source via the resolver
func (fs *LatentFS) readFromResolver(vfsPath string, buf []byte, offset uint64) (int, error) {
	// Read entire content from resolver (with cascade-aware hiding)
	content, err := fs.resolver.ReadFileWithHiding(fs.sourceFolder, vfsPath, fs.symlinkTarget)
	if err != nil {
		return 0, err
	}

	// Apply offset and copy to buffer
	if int(offset) >= len(content) {
		return 0, nil // EOF
	}

	remaining := content[offset:]
	n := copy(buf, remaining)
	return n, nil
}

// readFromSource reads data directly from a source file
// This always reads fresh from disk (no caching) to ensure changes are visible
func (fs *LatentFS) readFromSource(sourcePath string, buf []byte, offset uint64) (int, error) {
	f, err := os.Open(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ENOENT
		}
		return 0, EIO
	}
	defer f.Close()

	n, err := f.ReadAt(buf, int64(offset))
	if err != nil && err != io.EOF {
		return 0, EIO
	}
	return n, nil
}

// =============================================================================
// Source Folder Helpers - Directory Listing
// =============================================================================

// listParentDirEntries lists entries from source folder using resolver or parentFS
func (fs *LatentFS) listParentDirEntries(vfsPath string) []ParentDirEntry {
	// Use resolver only for cascaded forks
	if fs.isSourceCascaded() {
		sourceEntries, err := fs.resolver.ListDir(fs.sourceFolder, vfsPath)
		if err != nil {
			return nil
		}
		var entries []ParentDirEntry
		for _, se := range sourceEntries {
			entries = append(entries, ParentDirEntry{
				Name:  se.Name,
				IsDir: se.IsDir,
				Size:  se.Size,
			})
		}
		return entries
	}

	// For regular soft-forks without resolver, use filesystem
	if fs.sourceFolder != "" {
		sourcePath := fs.sourceFolder + "/" + vfsPath
		sourceEntries, err := os.ReadDir(sourcePath)
		if err != nil {
			return nil
		}
		var entries []ParentDirEntry
		for _, se := range sourceEntries {
			info, _ := se.Info()
			var size int64
			if info != nil {
				size = info.Size()
			}
			entries = append(entries, ParentDirEntry{
				Name:  se.Name(),
				IsDir: se.IsDir(),
				Size:  size,
			})
		}
		return entries
	}

	return nil
}

// readDirFromSource reads directory entries from source folder only
func (fs *LatentFS) readDirFromSource(handle vfs.VfsHandle, info *openHandle) ([]vfs.DirInfo, error) {
	sourcePath := info.sourcePath

	entries, err := os.ReadDir(sourcePath)
	if err != nil {
		return nil, EIO
	}

	// Get directory info for . and ..
	dirInfo, err := os.Stat(sourcePath)
	if err != nil {
		return nil, EIO
	}
	dirAttrs := osFileInfoToAttributes(dirInfo, info.path)

	var allEntries []vfs.DirInfo

	// Add . and .. entries
	allEntries = append(allEntries,
		vfs.DirInfo{Name: ".", Attributes: *dirAttrs},
		vfs.DirInfo{Name: "..", Attributes: *dirAttrs})

	// Add entries from source
	for _, e := range entries {
		// Skip hidden entries (prevents loop when target is inside source)
		if fs.isEntryHidden(info.path, e.Name()) {
			log.Debugf("[VFS] readDirFromSource: hiding entry %q in %q (target inside source)", e.Name(), info.path)
			continue
		}

		fi, err := e.Info()
		if err != nil {
			continue
		}

		entryPath := info.path + "/" + e.Name()
		modTimeNano := fi.ModTime().UnixNano()
		di := createDirInfoEntry(entryPath, DirEntryInfo{
			Name:    e.Name(),
			Size:    fi.Size(),
			Mode:    fi.Mode(),
			IsDir:   fi.IsDir(),
			ModTime: &modTimeNano,
		})
		allEntries = append(allEntries, di)
	}

	// Mark enumeration as done
	fs.handles.SetDirEnumDone(HandleID(handle), true)

	return allEntries, nil
}

// readDirFromResolver reads directory entries from source using the resolver
// This handles cascaded forks without triggering NFS recursion deadlock
func (fs *LatentFS) readDirFromResolver(handle vfs.VfsHandle, info *openHandle) ([]vfs.DirInfo, error) {
	log.Debugf("[VFS] readDirFromResolver: path=%q sourceFolder=%q", info.path, fs.sourceFolder)

	// Use resolver with cascade-aware hiding to list directory entries
	sourceEntries, err := fs.resolver.ListDirWithHiding(fs.sourceFolder, info.path, fs.symlinkTarget)
	if err != nil {
		return nil, err
	}

	var allEntries []vfs.DirInfo

	// Create dummy attributes for . and ..
	dotAttrs := &vfs.Attributes{}
	dotAttrs.SetFileType(vfs.FileTypeDirectory)
	dotAttrs.SetPermissions(vfs.NewPermissionsFromMode(0755))

	allEntries = append(allEntries,
		vfs.DirInfo{Name: ".", Attributes: *dotAttrs},
		vfs.DirInfo{Name: "..", Attributes: *dotAttrs})

	// Add entries from resolver (already filtered for hidden entries)
	for _, e := range sourceEntries {
		entryPath := info.path + "/" + e.Name
		di := createDirInfoEntry(entryPath, DirEntryInfo{
			Name:  e.Name,
			Size:  e.Size,
			Mode:  e.Mode,
			IsDir: e.IsDir,
		})
		allEntries = append(allEntries, di)
	}

	// Mark enumeration as done
	fs.handles.SetDirEnumDone(HandleID(handle), true)

	return allEntries, nil
}

// listSourceDir lists entries from source folder that aren't in overlay
func (fs *LatentFS) listSourceDir(vfsPath string, excludeNames map[string]bool) []vfs.DirInfo {
	// Use resolver only for cascaded forks
	if fs.isSourceCascaded() {
		return fs.listSourceDirFromResolver(vfsPath, excludeNames)
	}

	sourcePath := fs.sourceFolder + "/" + vfsPath

	entries, err := os.ReadDir(sourcePath)
	if err != nil {
		// Source directory doesn't exist or can't be read - not an error
		return nil
	}

	var result []vfs.DirInfo
	for _, e := range entries {
		// Skip entries that already exist in overlay
		if excludeNames[e.Name()] {
			continue
		}

		// Skip hidden entries (prevents loop when target is inside source)
		if fs.isEntryHidden(vfsPath, e.Name()) {
			log.Debugf("[VFS] listSourceDir: hiding entry %q in %q (target inside source)", e.Name(), vfsPath)
			continue
		}

		fi, err := e.Info()
		if err != nil {
			continue
		}

		entryPath := vfsPath + "/" + e.Name()
		modTimeNano := fi.ModTime().UnixNano()
		di := createDirInfoEntry(entryPath, DirEntryInfo{
			Name:    e.Name(),
			Size:    fi.Size(),
			Mode:    fi.Mode(),
			IsDir:   fi.IsDir(),
			ModTime: &modTimeNano,
		})
		result = append(result, di)
	}

	return result
}

// listSourceDirFromResolver lists entries from source folder using the resolver
// This handles cascaded forks without triggering NFS recursion deadlock
func (fs *LatentFS) listSourceDirFromResolver(vfsPath string, excludeNames map[string]bool) []vfs.DirInfo {
	// Use resolver with cascade-aware hiding
	sourceEntries, err := fs.resolver.ListDirWithHiding(fs.sourceFolder, vfsPath, fs.symlinkTarget)
	if err != nil {
		return nil
	}

	var result []vfs.DirInfo
	for _, e := range sourceEntries {
		// Skip entries that already exist in overlay
		if excludeNames[e.Name] {
			continue
		}

		// Hidden entries already filtered by ListDirWithHiding

		entryPath := vfsPath + "/" + e.Name
		di := createDirInfoEntry(entryPath, DirEntryInfo{
			Name:  e.Name,
			Size:  e.Size,
			Mode:  e.Mode,
			IsDir: e.IsDir,
		})
		result = append(result, di)
	}

	return result
}

// =============================================================================
// Source Folder Helpers - Attribute Operations
// =============================================================================

// getAttrFromSource gets attributes for a source-only file
// Uses Lstat to preserve symlink information (not follow the symlink)
func (fs *LatentFS) getAttrFromSource(sourcePath, vfsPath string) (*vfs.Attributes, error) {
	info, err := os.Lstat(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ENOENT
		}
		return nil, EIO
	}
	return osFileInfoToAttributes(info, vfsPath), nil
}

// getAttrFromResolver gets attributes for a source-only file using the resolver
// This avoids filesystem calls that would trigger NFS recursion
// Uses StatWithInfo to get full file info including mtime (critical for NFS cache invalidation)
func (fs *LatentFS) getAttrFromResolver(vfsPath string) (*vfs.Attributes, error) {
	info, err := fs.resolver.StatWithInfo(fs.sourceFolder, vfsPath)
	if err != nil {
		return nil, err
	}

	// Use osFileInfoToAttributes to properly set all attributes including mtime
	// This is critical for NFS cache invalidation - without mtime, clients can't detect changes
	return osFileInfoToAttributes(info, vfsPath), nil
}

// lookupFromSource looks up a file in the source folder and returns synthetic attributes
func (fs *LatentFS) lookupFromSource(vfsPath string) (*vfs.Attributes, error) {
	// Use resolver only for cascaded forks
	if fs.isSourceCascaded() {
		return fs.resolver.LookupWithHiding(fs.sourceFolder, vfsPath, fs.symlinkTarget)
	}

	// Fallback: Check if this path should be hidden (single-level)
	if fs.isPathHidden(vfsPath) {
		log.Debugf("[VFS] lookupFromSource: hiding %q (target inside source)", vfsPath)
		return nil, ENOENT
	}

	sourcePath := fs.sourceFolder + "/" + vfsPath

	info, err := os.Stat(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ENOENT
		}
		return nil, EIO
	}

	return osFileInfoToAttributes(info, vfsPath), nil
}

// =============================================================================
// Tombstone/Overlay Helpers
// =============================================================================

// unlinkSourceOnly handles deletion of a source-only path by creating a tombstone.
// Must be called with fs.mu held.
func (fs *LatentFS) unlinkSourceOnly(vfsPath string) error {
	// Verify the path exists in source before creating tombstone
	if fs.isSourceCascaded() {
		_, _, err := fs.resolver.StatWithHiding(fs.sourceFolder, vfsPath, fs.symlinkTarget)
		if err != nil {
			return err
		}
	} else {
		sourcePath := fs.sourceFolder + "/" + vfsPath
		_, err := os.Lstat(sourcePath)
		if err != nil {
			if os.IsNotExist(err) {
				return ENOENT
			}
			return EIO
		}
	}

	// Wrap all tombstone + parent creation in a single transaction
	ctx := context.Background()
	parentCache := make(map[string]int64)
	txErr := fs.dataFile.RunInTx(ctx, func(ctx context.Context, tx bun.Tx) error {
		return fs.createTombstoneTx(ctx, tx, vfsPath, parentCache)
	})
	if txErr != nil {
		log.Debugf("[VFS] unlinkSourceOnly: transaction failed: %v", txErr)
		return EIO
	}
	return nil
}

// createTombstoneLocked creates a tombstone entry in the overlay to mark a source file as deleted.
// Must be called with fs.mu held. Uses individual autocommit transactions (legacy path).
func (fs *LatentFS) createTombstoneLocked(vfsPath string) error {
	ctx := context.Background()
	parentCache := make(map[string]int64)
	return fs.dataFile.RunInTx(ctx, func(ctx context.Context, tx bun.Tx) error {
		return fs.createTombstoneTx(ctx, tx, vfsPath, parentCache)
	})
}

// createTombstoneTx creates a tombstone entry within a transaction.
// parentCache maps parent paths to their overlay inode numbers to avoid
// repeated ResolvePath calls during rm -rf (M3 optimization).
// Must be called with fs.mu held.
func (fs *LatentFS) createTombstoneTx(ctx context.Context, tx bun.Tx, vfsPath string, parentCache map[string]int64) error {
	parentPath := common.ParentPath(vfsPath)
	name := common.BaseName(vfsPath)
	log.Debugf("[VFS] createTombstoneTx: vfsPath=%q parentPath=%q name=%q", vfsPath, parentPath, name)

	// Ensure parent directory exists in overlay (with cache for repeated calls)
	parentIno, err := fs.ensureParentInOverlayTx(ctx, tx, parentPath, parentCache)
	if err != nil {
		log.Debugf("[VFS] createTombstoneTx: ensureParentInOverlayTx failed: %v", err)
		return err
	}
	log.Debugf("[VFS] createTombstoneTx: parentIno=%d", parentIno)

	// Create tombstone inode (mode=0 indicates deleted)
	tombstoneIno, err := fs.dataFile.CreateInodeTx(ctx, tx, storage.ModeDeleted)
	if err != nil {
		log.Debugf("[VFS] createTombstoneTx: CreateInodeTx failed: %v", err)
		return err
	}
	log.Debugf("[VFS] createTombstoneTx: tombstoneIno=%d", tombstoneIno)

	// Create dentry pointing to tombstone
	if err := fs.dataFile.CreateDentryTx(ctx, tx, parentIno, name, tombstoneIno); err != nil {
		log.Debugf("[VFS] createTombstoneTx: CreateDentryTx failed: %v", err)
		// Clean up orphaned tombstone inode
		fs.dataFile.DeleteInodeTx(ctx, tx, tombstoneIno)
		return err
	}

	log.Debugf("[VFS] createTombstoneTx: created tombstone for %q", vfsPath)
	return nil
}

// ensureParentInOverlayTx ensures all parent directories exist in overlay within a transaction.
// parentCache avoids repeated ResolvePath calls for the same parent during rm -rf (M3 optimization).
// Must be called with fs.mu held.
func (fs *LatentFS) ensureParentInOverlayTx(ctx context.Context, tx bun.Tx, parentPath string, parentCache map[string]int64) (int64, error) {
	if parentPath == "/" || parentPath == "" {
		return storage.RootIno, nil
	}

	// Check session cache first (M3: avoids repeated ResolvePath for same parent)
	if ino, ok := parentCache[parentPath]; ok {
		return ino, nil
	}

	// Try to resolve in overlay
	parentIno, err := fs.dataFile.ResolvePathTx(ctx, tx, parentPath)
	if err == nil {
		parentCache[parentPath] = parentIno
		return parentIno, nil
	}

	// Parent doesn't exist in overlay - need to create it
	grandparentPath := common.ParentPath(parentPath)
	grandparentIno, err := fs.ensureParentInOverlayTx(ctx, tx, grandparentPath, parentCache)
	if err != nil {
		return 0, err
	}

	// Get parent info from source to preserve mode
	var mode uint32 = storage.DefaultDirMode
	if fs.sourceFolder != "" {
		sourcePath := fs.sourceFolder + "/" + parentPath
		if info, err := os.Stat(sourcePath); err == nil && info.IsDir() {
			mode = storage.ModeDir | uint32(info.Mode()&0777)
		}
	}

	// Create directory inode in overlay
	dirIno, err := fs.dataFile.CreateInodeTx(ctx, tx, mode)
	if err != nil {
		return 0, EIO
	}

	// Create dentry for this directory
	dirName := common.BaseName(parentPath)
	if err := fs.dataFile.CreateDentryTx(ctx, tx, grandparentIno, dirName, dirIno); err != nil {
		fs.dataFile.DeleteInodeTx(ctx, tx, dirIno)
		return 0, EIO
	}

	log.Debugf("[VFS] ensureParentInOverlayTx: created directory %q in overlay", parentPath)
	parentCache[parentPath] = dirIno
	return dirIno, nil
}

// replaceTombstoneWithFile replaces a tombstone entry with a new file
// Must be called with fs.mu held
func (fs *LatentFS) replaceTombstoneWithFile(path string, tombstoneIno int64, mode int) (vfs.VfsHandle, error) {
	parentPath := common.ParentPath(path)
	name := common.BaseName(path)

	// Resolve parent
	parentIno, err := fs.dataFile.ResolvePath(parentPath)
	if err != nil {
		return 0, ENOENT
	}

	// Delete the tombstone dentry and inode
	if err := fs.dataFile.DeleteDentry(parentIno, name); err != nil {
		log.Errorf("[VFS] replaceTombstoneWithFile: failed to delete tombstone dentry: %v", err)
		return 0, EIO
	}
	if err := fs.dataFile.DeleteInode(tombstoneIno); err != nil {
		log.Errorf("[VFS] replaceTombstoneWithFile: failed to delete tombstone inode: %v", err)
		// Continue anyway - orphaned inode is not critical
	}

	// Create new file inode
	fileMode := uint32(storage.ModeFile | (mode & 0777))
	ino, err := fs.dataFile.CreateInode(fileMode)
	if err != nil {
		return 0, EIO
	}

	// Create new dentry
	if err := fs.dataFile.CreateDentry(parentIno, name, ino); err != nil {
		fs.dataFile.DeleteInode(ino)
		return 0, EIO
	}

	log.Debugf("[VFS] replaceTombstoneWithFile: replaced tombstone with new file at %q", path)

	// Allocate handle - pin epoch at open time for read consistency
	handle := fs.handles.Allocate(ino, path, false, os.O_CREATE|os.O_RDWR, fs.dataFile.GetReadableEpoch())
	return vfs.VfsHandle(handle), nil
}

// ensureParentInOverlayLocked ensures all parent directories exist in overlay
// Returns the inode of the immediate parent directory
// Must be called with fs.mu held
func (fs *LatentFS) ensureParentInOverlayLocked(parentPath string) (int64, error) {
	if parentPath == "/" || parentPath == "" {
		return storage.RootIno, nil
	}

	// Try to resolve in overlay first
	parentIno, err := fs.dataFile.ResolvePath(parentPath)
	if err == nil {
		// Parent exists in overlay
		return parentIno, nil
	}

	// Parent doesn't exist in overlay - need to create it
	// First ensure grandparent exists
	grandparentPath := common.ParentPath(parentPath)
	grandparentIno, err := fs.ensureParentInOverlayLocked(grandparentPath)
	if err != nil {
		return 0, err
	}

	// Get parent info from source to preserve mode
	var mode uint32 = storage.DefaultDirMode
	if fs.sourceFolder != "" {
		sourcePath := fs.sourceFolder + "/" + parentPath
		if info, err := os.Stat(sourcePath); err == nil && info.IsDir() {
			mode = storage.ModeDir | uint32(info.Mode()&0777)
		}
	}

	// Create directory inode in overlay
	dirIno, err := fs.dataFile.CreateInode(mode)
	if err != nil {
		return 0, EIO
	}

	// Create dentry for this directory
	dirName := common.BaseName(parentPath)
	if err := fs.dataFile.CreateDentry(grandparentIno, dirName, dirIno); err != nil {
		fs.dataFile.DeleteInode(dirIno)
		return 0, EIO
	}

	log.Debugf("[VFS] ensureParentInOverlay: created directory %q in overlay", parentPath)
	return dirIno, nil
}

// =============================================================================
// Hidden Path Helpers
// =============================================================================

// getHiddenPath returns the path that should be hidden (computed once and cached)
// This is used to prevent infinite loops when the symlink target is inside the source folder
func (fs *LatentFS) getHiddenPath() string {
	fs.hiddenPathOnce.Do(func() {
		if fs.sourceFolder != "" && fs.symlinkTarget != "" {
			fs.hiddenPath = computeHiddenPathForFS(fs.sourceFolder, fs.symlinkTarget)
		}
	})
	return fs.hiddenPath
}

// computeHiddenPathForFS checks if target is inside source folder and returns the relative path to hide
func computeHiddenPathForFS(source, target string) string {
	sourceAbs, err := filepath.Abs(source)
	if err != nil {
		return ""
	}
	targetAbs, err := filepath.Abs(target)
	if err != nil {
		return ""
	}

	// Ensure source ends with separator for proper prefix matching
	sourcePrefix := sourceAbs
	if !strings.HasSuffix(sourcePrefix, string(filepath.Separator)) {
		sourcePrefix += string(filepath.Separator)
	}

	// Check if target is inside source
	if !strings.HasPrefix(targetAbs, sourcePrefix) {
		return ""
	}

	// Get relative path from source to target
	relPath, err := filepath.Rel(sourceAbs, targetAbs)
	if err != nil {
		return ""
	}

	// Don't hide if it's the source itself or parent
	if relPath == "." || relPath == ".." || strings.HasPrefix(relPath, ".."+string(filepath.Separator)) {
		return ""
	}

	return relPath
}

// isPathHidden checks if a path should be hidden (to prevent infinite loop)
// path is the full VFS path (e.g., "/hidden_folder")
func (fs *LatentFS) isPathHidden(path string) bool {
	hidden := fs.getHiddenPath()
	if hidden == "" {
		return false
	}
	// Normalize path - remove leading slash
	normalizedPath := path
	if len(path) > 0 && path[0] == '/' {
		normalizedPath = path[1:]
	}
	return normalizedPath == hidden
}

// isEntryHidden checks if a directory entry name at a given parent path should be hidden
// parentPath is the VFS path of the parent directory (e.g., "/" or "/subdir")
// name is the entry name being checked
func (fs *LatentFS) isEntryHidden(parentPath, name string) bool {
	hidden := fs.getHiddenPath()
	if hidden == "" {
		return false
	}
	// Build the full path
	var fullPath string
	if parentPath == "/" || parentPath == "" {
		fullPath = name
	} else {
		// Remove leading slash from parentPath
		normalizedParent := parentPath
		if len(parentPath) > 0 && parentPath[0] == '/' {
			normalizedParent = parentPath[1:]
		}
		if normalizedParent == "" {
			fullPath = name
		} else {
			fullPath = normalizedParent + "/" + name
		}
	}
	return fullPath == hidden
}

// =============================================================================
// Directory Entry Helpers
// =============================================================================

// DirEntryInfo contains the information needed to create a vfs.DirInfo entry.
// This abstracts over os.FileInfo and resolver entries.
type DirEntryInfo struct {
	Name    string
	Size    int64
	Mode    os.FileMode
	IsDir   bool
	ModTime *int64 // Unix nano, nil if not available
}

// createDirInfoEntry creates a vfs.DirInfo from entry information and computes inode from path.
// This consolidates the repeated DirInfo creation pattern across readDirFromSource,
// readDirFromResolver, listSourceDir, and listSourceDirFromResolver.
func createDirInfoEntry(entryPath string, info DirEntryInfo) vfs.DirInfo {
	di := vfs.DirInfo{Name: info.Name}
	ino := hashPathToInode(entryPath)

	di.SetFileHandle(vfs.VfsNode(ino))
	di.SetInodeNumber(ino)
	di.SetSizeBytes(uint64(info.Size))

	if info.ModTime != nil {
		di.SetLastDataModificationTime(timeFromUnixNano(*info.ModTime))
	}

	if info.IsDir {
		di.SetFileType(vfs.FileTypeDirectory)
	} else if info.Mode&os.ModeSymlink != 0 {
		di.SetFileType(vfs.FileTypeSymlink)
	} else {
		di.SetFileType(vfs.FileTypeRegularFile)
	}

	di.SetPermissions(vfs.NewPermissionsFromMode(uint32(info.Mode)))
	di.SetUnixMode(uint32(info.Mode) & 0777)

	return di
}

// timeFromUnixNano converts Unix nanoseconds to time.Time
func timeFromUnixNano(ns int64) time.Time {
	return time.Unix(0, ns)
}

// =============================================================================
// Attribute Conversion Helpers
// =============================================================================

// osFileInfoToAttributes converts os.FileInfo to vfs.Attributes
// Used for source-only files (not in overlay)
func osFileInfoToAttributes(info os.FileInfo, path string) *vfs.Attributes {
	attrs := &vfs.Attributes{}

	// Use hash of path as inode number for source files (with high bit set)
	ino := hashPathToInode(path)
	attrs.SetFileHandle(vfs.VfsNode(ino))
	attrs.SetInodeNumber(ino)
	attrs.SetSizeBytes(uint64(info.Size()))
	attrs.SetLinkCount(1)
	attrs.SetUID(uint32(os.Getuid()))
	attrs.SetGID(uint32(os.Getgid()))
	attrs.SetPermissions(vfs.NewPermissionsFromMode(uint32(info.Mode())))
	attrs.SetUnixMode(uint32(info.Mode()) & 0777)
	attrs.SetLastDataModificationTime(info.ModTime())
	attrs.SetLastStatusChangeTime(info.ModTime())
	attrs.SetAccessTime(info.ModTime())
	attrs.SetBirthTime(info.ModTime())
	attrs.SetChangeID(uint64(info.ModTime().UnixNano()))

	if info.IsDir() {
		attrs.SetFileType(vfs.FileTypeDirectory)
	} else if info.Mode()&os.ModeSymlink != 0 {
		attrs.SetFileType(vfs.FileTypeSymlink)
	} else {
		attrs.SetFileType(vfs.FileTypeRegularFile)
	}

	return attrs
}

// hashPathToInode generates a deterministic inode number from a path
// High bit is set to distinguish from overlay inodes
func hashPathToInode(path string) uint64 {
	const fnvPrime = 1099511628211
	const fnvOffset = 14695981039346656037
	hash := uint64(fnvOffset)
	for i := 0; i < len(path); i++ {
		hash ^= uint64(path[i])
		hash *= fnvPrime
	}
	// Set high bit to distinguish from overlay inodes
	return hash | 0x8000000000000000
}

// inodeToAttributes converts a storage.Inode to vfs.Attributes
func inodeToAttributes(inode *storage.Inode) *vfs.Attributes {
	attrs := &vfs.Attributes{}

	attrs.SetFileHandle(vfs.VfsNode(inode.Ino))
	attrs.SetInodeNumber(uint64(inode.Ino))
	attrs.SetSizeBytes(uint64(inode.Size))
	attrs.SetLinkCount(uint32(inode.Nlink))
	attrs.SetUID(inode.Uid)
	attrs.SetGID(inode.Gid)
	attrs.SetPermissions(vfs.NewPermissionsFromMode(inode.Mode))
	attrs.SetUnixMode(inode.Mode & 0777)
	attrs.SetLastDataModificationTime(inode.Mtime)
	attrs.SetLastStatusChangeTime(inode.Ctime)
	attrs.SetAccessTime(inode.Atime)
	attrs.SetBirthTime(inode.Ctime)
	attrs.SetChangeID(uint64(inode.Mtime.UnixNano()))

	if inode.IsDir() {
		attrs.SetFileType(vfs.FileTypeDirectory)
	} else if inode.IsSymlink() {
		attrs.SetFileType(vfs.FileTypeSymlink)
	} else {
		attrs.SetFileType(vfs.FileTypeRegularFile)
	}

	return attrs
}

// =============================================================================
// Cascade Detection
// =============================================================================

// isSourceCascaded checks whether the source folder is itself a mounted VFS.
// Result is cached after first check. If false, we can skip the resolver
// and use direct filesystem access (much faster for non-cascaded forks).
// Must NOT be called with fs.mu held (uses sync.Once).
func (fs *LatentFS) isSourceCascaded() bool {
	if fs.resolver == nil || fs.sourceFolder == "" {
		return false
	}

	fs.sourceIsCascadedOnce.Do(func() {
		// Check if source folder is a mounted VFS by asking the resolver's MetaFS
		// This is O(N) where N is number of mounts, but only happens once
		vfs := fs.resolver.metaFS.GetVFSBySymlinkTarget(fs.sourceFolder)
		fs.sourceIsCascaded = (vfs != nil)
		if fs.sourceIsCascaded {
			log.Debugf("[VFS] Source folder %q is a cascaded VFS mount", fs.sourceFolder)
		} else {
			log.Debugf("[VFS] Source folder %q is a real filesystem (non-cascaded)", fs.sourceFolder)
		}
	})

	return fs.sourceIsCascaded
}
