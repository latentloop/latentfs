package vfs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uptrace/bun"

	"github.com/macos-fuse-t/go-smb2/vfs"

	"latentfs/internal/cache"
	"latentfs/internal/storage"
	"latentfs/internal/common"
)

// attrCacheTTL is the TTL for cached attributes.
// 30ms captures NFS GETATTR bursts while keeping source file freshness under 50ms.
const attrCacheTTL = 30 * time.Millisecond

// attrCacheMaxEntries caps memory usage. Typical large trees have <10K unique paths.
const attrCacheMaxEntries = 10000

// LatentFS implements vfs.VFSFileSystem backed by SQLite
// When sourceFolder is set, files not found in the overlay will fall through
// to the source folder (copy-on-write semantics)
type LatentFS struct {
	mu            sync.RWMutex
	dataFile      *storage.DataFile
	handles       *HandleManager
	sourceFolder  string // Empty string means no source (regular mount)
	symlinkTarget string // Symlink target path (used to compute hidden path for loop prevention)

	// SourceResolver for stack-based source folder access (handles cascaded forks)
	// When set, all source folder operations go through resolver instead of direct fs
	resolver *SourceResolver

	// Cached hidden path (computed once from sourceFolder and symlinkTarget)
	hiddenPath     string
	hiddenPathOnce sync.Once

	// attrCache caches GetAttrByPath results with TTL-based expiration.
	// Supports fine-grained invalidation by path.
	attrCache *cache.AttrCache

	// sourceIsCascaded caches whether the source folder is itself a mounted VFS.
	// Checked once on first access, then cached. If false, we skip the resolver
	// and use direct filesystem access (much faster for non-cascaded forks).
	sourceIsCascaded      bool
	sourceIsCascadedOnce  sync.Once
}

// NewLatentFS creates a new VFS backed by the given data file
func NewLatentFS(dataFile *storage.DataFile) *LatentFS {
	return &LatentFS{
		dataFile:  dataFile,
		handles:   NewHandleManager(),
		attrCache: cache.NewAttrCache(attrCacheTTL, attrCacheMaxEntries),
	}
}

// NewForkLatentFS creates a VFS with copy-on-write semantics over a source folder
// Reads fall through to sourceFolder when files don't exist in overlay
// Writes are always captured in the overlay (copy-on-write)
func NewForkLatentFS(dataFile *storage.DataFile, sourceFolder string) *LatentFS {
	return NewForkLatentFSWithTarget(dataFile, sourceFolder, "")
}

// NewForkLatentFSWithTarget creates a VFS with copy-on-write semantics and symlink target
// symlinkTarget is used to compute hidden path for loop prevention when target is inside source
func NewForkLatentFSWithTarget(dataFile *storage.DataFile, sourceFolder, symlinkTarget string) *LatentFS {
	return &LatentFS{
		dataFile:      dataFile,
		handles:       NewHandleManager(),
		sourceFolder:  sourceFolder,
		symlinkTarget: symlinkTarget,
		attrCache:     cache.NewAttrCache(attrCacheTTL, attrCacheMaxEntries),
	}
}

// NewForkLatentFSWithResolver creates a VFS with copy-on-write semantics using SourceResolver
// The resolver handles all source folder access, including cascaded fork detection
// This is the new preferred constructor for soft-forks
func NewForkLatentFSWithResolver(dataFile *storage.DataFile, sourceFolder, symlinkTarget string, resolver *SourceResolver) *LatentFS {
	return &LatentFS{
		dataFile:      dataFile,
		handles:       NewHandleManager(),
		sourceFolder:  sourceFolder,
		symlinkTarget: symlinkTarget,
		resolver:      resolver,
		attrCache:     cache.NewAttrCache(attrCacheTTL, attrCacheMaxEntries),
	}
}


// DataFile returns the underlying data file
func (fs *LatentFS) DataFile() *storage.DataFile {
	return fs.dataFile
}

// SourceFolder returns the source folder path for soft-forks
// Returns empty string for regular mounts
func (fs *LatentFS) SourceFolder() string {
	return fs.sourceFolder
}

// SymlinkTarget returns the symlink target path
// Used for computing hidden paths in soft-forks
func (fs *LatentFS) SymlinkTarget() string {
	return fs.symlinkTarget
}

// --- Cache Invalidation Methods ---
// These methods provide fine-grained cache invalidation for different operations.

// InvalidateAttrPath invalidates a specific path in the attribute cache.
// Used for write operations that only affect a single file.
func (fs *LatentFS) InvalidateAttrPath(path string) {
	if fs.attrCache != nil {
		fs.attrCache.InvalidatePath(path)
	}
}

// InvalidateAttrPathAndParent invalidates a path and its parent directory.
// Used for create, remove, mkdir, symlink operations.
func (fs *LatentFS) InvalidateAttrPathAndParent(path string) {
	if fs.attrCache != nil {
		fs.attrCache.InvalidatePathAndParent(path, filepath.Dir(path))
	}
}

// InvalidateAttrRename invalidates all paths affected by a rename operation.
// Affects: oldPath, newPath, oldParent, newParent
func (fs *LatentFS) InvalidateAttrRename(oldPath, newPath string) {
	if fs.attrCache != nil {
		fs.attrCache.InvalidateRename(oldPath, newPath, filepath.Dir(oldPath), filepath.Dir(newPath))
	}
}

// ReadFileContent implements ParentVFS interface for cascaded fork support
// Reads the entire content of a file at the given VFS path
func (fs *LatentFS) ReadFileContent(vfsPath string) ([]byte, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Try to resolve in local data file first
	ino, err := fs.dataFile.ResolvePath(vfsPath)
	if err == nil {
		// Check for tombstone
		inode, err := fs.dataFile.GetInode(ino)
		if err != nil {
			return nil, EIO
		}
		if inode.IsDeleted() {
			return nil, ENOENT
		}
		// Read from local data file - read entire content based on file size
		if inode.Size == 0 {
			return []byte{}, nil
		}
		return fs.dataFile.ReadContent(ino, 0, int(inode.Size))
	}

	// Check if path is tombstoned before falling through to source
	if fs.sourceFolder != "" && fs.isPathTombstoned(vfsPath) {
		return nil, ENOENT
	}

	// Use resolver only for cascaded forks
	if fs.isSourceCascaded() {
		return fs.resolver.ReadFileWithHiding(fs.sourceFolder, vfsPath, fs.symlinkTarget)
	}

	// Not in local data file, try source folder fallback (filesystem)
	if fs.sourceFolder != "" {
		sourcePath := fs.sourceFolder + "/" + vfsPath
		return os.ReadFile(sourcePath)
	}

	return nil, ENOENT
}

// StatPath implements ParentVFS interface for cascaded fork support
// Returns file size and whether it's a directory
func (fs *LatentFS) StatPath(vfsPath string) (size int64, isDir bool, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Try to resolve in local data file first
	ino, err := fs.dataFile.ResolvePath(vfsPath)
	if err == nil {
		inode, err := fs.dataFile.GetInode(ino)
		if err != nil {
			return 0, false, EIO
		}
		if inode.IsDeleted() {
			return 0, false, ENOENT
		}
		return inode.Size, inode.IsDir(), nil
	}

	// Check if path is tombstoned before falling through to source
	if fs.sourceFolder != "" && fs.isPathTombstoned(vfsPath) {
		return 0, false, ENOENT
	}

	// Use resolver only for cascaded forks
	if fs.isSourceCascaded() {
		return fs.resolver.Stat(fs.sourceFolder, vfsPath)
	}

	// Not in local data file, try source folder fallback (filesystem)
	if fs.sourceFolder != "" {
		sourcePath := fs.sourceFolder + "/" + vfsPath
		info, err := os.Stat(sourcePath)
		if err != nil {
			if os.IsNotExist(err) {
				return 0, false, ENOENT
			}
			return 0, false, EIO
		}
		return info.Size(), info.IsDir(), nil
	}

	return 0, false, ENOENT
}

// GetAttrByPath returns full attributes for a file or directory by path.
// This avoids the Open/GetAttr/Close round-trip used by BillyAdapter.Stat().
// A single RLock acquisition replaces 2-3 separate lock acquisitions.
func (fs *LatentFS) GetAttrByPath(vfsPath string) (attrs *vfs.Attributes, err error) {
	defer recoverLatentFSPanic("GetAttrByPath", &err)
	if log.IsLevelEnabled(log.TraceLevel) {
		start := time.Now()
		defer func() { log.Tracef("[VFS] GetAttrByPath %q → %v (%v)", vfsPath, err, time.Since(start)) }()
	}
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	vfsPath = common.NormalizePath(vfsPath)

	// Try attrCache first (fast path)
	if fs.attrCache != nil {
		if cached := fs.attrCache.Get(vfsPath); cached != nil {
			return cached, nil
		}
	}

	// Try to resolve in overlay first
	ino, err := fs.dataFile.ResolvePath(vfsPath)
	log.Debugf("[VFS] GetAttrByPath %q: ResolvePath → ino=%d err=%v", vfsPath, ino, err)
	if err == nil {
		inode, err := fs.dataFile.GetInode(ino)
		log.Debugf("[VFS] GetAttrByPath %q: GetInode(%d) → err=%v", vfsPath, ino, err)
		if err != nil {
			// GetInode returns ErrNotFound for tombstones (mode=0)
			if err == common.ErrNotFound {
				log.Debugf("[VFS] GetAttrByPath %q: tombstone detected (GetInode returned ErrNotFound)", vfsPath)
				return nil, ENOENT
			}
			return nil, EIO
		}
		if inode.IsDeleted() {
			log.Debugf("[VFS] GetAttrByPath %q: tombstone detected (inode.IsDeleted)", vfsPath)
			return nil, ENOENT
		}
		attrs = inodeToAttributes(inode)
		// Cache overlay result (isSource=false)
		if fs.attrCache != nil {
			fs.attrCache.Set(vfsPath, attrs, false)
		}
		return attrs, nil
	}

	// Source folder fallback (soft-fork)
	if fs.sourceFolder != "" {
		// Block source fallback if any path component has a dentry tombstone
		if fs.isPathTombstoned(vfsPath) {
			return nil, ENOENT
		}
		// Use resolver only for cascaded forks (source folder is itself a VFS mount)
		// For non-cascaded forks, direct filesystem access is much faster
		if fs.isSourceCascaded() {
			attrs, err = fs.getAttrFromResolver(vfsPath)
			if err == nil && fs.attrCache != nil {
				fs.attrCache.Set(vfsPath, attrs, true)
			}
			return attrs, err
		}
		// Direct source access (non-cascaded fork or no resolver)
		sourcePath := fs.sourceFolder + "/" + vfsPath
		info, err := os.Lstat(sourcePath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, ENOENT
			}
			return nil, EIO
		}
		attrs = osFileInfoToAttributes(info, vfsPath)
		// Cache source result (isSource=true, subject to TTL)
		if fs.attrCache != nil {
			fs.attrCache.Set(vfsPath, attrs, true)
		}
		return attrs, nil
	}

	return nil, ENOENT
}

// ListDir implements ParentVFS interface for cascaded fork support
// Lists directory contents at the given VFS path
func (fs *LatentFS) ListDir(vfsPath string) ([]ParentDirEntry, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var entries []ParentDirEntry

	// Try to list from local data file first
	ino, err := fs.dataFile.ResolvePath(vfsPath)
	if err == nil {
		// Check if it's a directory
		inode, err := fs.dataFile.GetInode(ino)
		if err != nil {
			return nil, EIO
		}
		if inode.IsDeleted() {
			return nil, ENOENT
		}
		if !inode.IsDir() {
			return nil, ENOTDIR
		}

		// Get entries from data file
		dbEntries, err := fs.dataFile.ListDir(ino)
		if err != nil {
			return nil, EIO
		}

		tombstones := make(map[string]bool)
		for _, e := range dbEntries {
			if e.Mode == storage.ModeDeleted {
				tombstones[e.Name] = true
				continue
			}
			entries = append(entries, ParentDirEntry{
				Name:  e.Name,
				IsDir: e.Mode == storage.ModeDir,
				Size:  e.Size,
			})
		}

		// Also get dentry tombstone names (ino=0) which are excluded by ListDirEntries
		dentryTombstones, _ := fs.dataFile.ListDentryTombstoneNames(ino)
		for _, name := range dentryTombstones {
			tombstones[name] = true
		}

		// Merge with parent entries
		parentEntries := fs.listParentDirEntries(vfsPath)
		for _, pe := range parentEntries {
			if tombstones[pe.Name] {
				continue // Blocked by tombstone
			}
			// Check if already in overlay
			found := false
			for _, e := range entries {
				if e.Name == pe.Name {
					found = true
					break
				}
			}
			if !found {
				entries = append(entries, pe)
			}
		}

		return entries, nil
	}

	// Check if path is tombstoned before falling through to parent/source
	if fs.sourceFolder != "" && fs.isPathTombstoned(vfsPath) {
		return nil, ENOENT
	}

	// Not in local data file, try parent fallback
	parentEntries := fs.listParentDirEntries(vfsPath)
	if len(parentEntries) > 0 {
		return parentEntries, nil
	}

	// If we have a cascaded source folder but got no entries, the directory doesn't exist
	if fs.isSourceCascaded() {
		return nil, ENOENT
	}

	return nil, ENOENT
}

// InvalidateCache refreshes all cached state in the LatentFS.
// This clears: attrCache, lookupCache, parentInodeCache, and refreshes epochs.
// This should be called after external modifications to the DataFile
// (e.g., restore command) to ensure subsequent reads reflect the new state.
func (fs *LatentFS) InvalidateCache() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.invalidateCacheInternal()
}

// --- File Operations ---
// All operations have panic recovery to prevent SMB server disconnections

// Open opens a file
func (fs *LatentFS) Open(path string, flags int, mode int) (handle vfs.VfsHandle, err error) {
	defer recoverLatentFSPanic("Open", &err)
	if log.IsLevelEnabled(log.TraceLevel) {
		start := time.Now()
		defer func() { log.Tracef("[VFS] Open %q flags=%d → %v (%v)", path, flags, err, time.Since(start)) }()
	}
	log.Debugf("[VFS] Open: path=%q flags=%d mode=%d", path, flags, mode)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path = common.NormalizePath(path)

	// DISABLED: All DataFile modifications now go through daemon (NFS or IPC).
	// IPC handlers call InvalidateCacheForDataFile for immediate cache invalidation.
	// fs.dataFile.CheckForExternalChanges()

	// Resolve path to inode in overlay
	ino, err := fs.dataFile.ResolvePath(path)
	if err != nil {
		if err == common.ErrNotFound {
			// Handle create
			if flags&os.O_CREATE != 0 {
				return fs.createFile(path, mode)
			}

			// Try source folder fallback (soft-fork)
			if fs.sourceFolder != "" {
				// Block source fallback if any path component has a dentry tombstone
				if fs.isPathTombstoned(path) {
					return 0, ENOENT
				}
				return fs.openFromSource(path, flags, mode)
			}

			return 0, ENOENT
		}
		return 0, EIO
	}

	// Get inode to check type
	inode, err := fs.dataFile.GetInode(ino)
	if err != nil {
		// If ResolvePath succeeded but GetInode returns ErrNotFound, it's a tombstone
		// (GetInode filters out tombstones by design)
		if err == common.ErrNotFound {
			if flags&os.O_CREATE != 0 {
				return fs.replaceTombstoneWithFile(path, ino, mode)
			}
			return 0, ENOENT
		}
		return 0, EIO
	}

	// Check for tombstone (deleted source file) - this check is kept for clarity
	// but shouldn't be reached since GetInode returns ErrNotFound for tombstones
	if inode.IsDeleted() {
		// If O_CREATE is set, replace tombstone with new file
		if flags&os.O_CREATE != 0 {
			return fs.replaceTombstoneWithFile(path, ino, mode)
		}
		return 0, ENOENT
	}

	if inode.IsDir() {
		return 0, EISDIR
	}

	// Allocate handle - pin epoch at open time for read consistency
	h := fs.handles.Allocate(ino, path, false, flags, fs.dataFile.GetReadableEpoch())
	return vfs.VfsHandle(h), nil
}

// Close closes a file handle
func (fs *LatentFS) Close(handle vfs.VfsHandle) (err error) {
	defer recoverLatentFSPanic("Close", &err)
	fs.handles.Release(HandleID(handle))
	return nil
}

// Read reads data from a file
func (fs *LatentFS) Read(handle vfs.VfsHandle, buf []byte, offset uint64, flags int) (n int, err error) {
	defer recoverLatentFSPanic("Read", &err)

	fs.mu.RLock()
	defer fs.mu.RUnlock()

	info, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return 0, EBADF
	}

	if info.isDir {
		return 0, EISDIR
	}

	// Handle source-only files (soft-fork read-through)
	if info.isSourceOnly {
		// Use resolver only for cascaded forks
		if fs.isSourceCascaded() {
			return fs.readFromResolver(info.path, buf, offset)
		}
		return fs.readFromSource(info.sourcePath, buf, offset)
	}

	// Length comes from buffer size, not flags
	// Use pinned epoch for read consistency - ensures all chunks come from
	// the same MVCC version even if readableEpoch changes during the read
	length := len(buf)
	data, err := fs.dataFile.ReadContentAtEpoch(info.ino, int64(offset), length, info.pinnedEpoch)
	if err != nil {
		return 0, EIO
	}

	n = copy(buf, data)
	return n, nil
}

// Write writes data to a file
func (fs *LatentFS) Write(handle vfs.VfsHandle, buf []byte, offset uint64, flags int) (n int, err error) {
	defer recoverLatentFSPanic("Write", &err)
	if log.IsLevelEnabled(log.TraceLevel) {
		start := time.Now()
		defer func() { log.Tracef("[VFS] Write handle=%d len=%d off=%d → %v (%v)", handle, len(buf), offset, err, time.Since(start)) }()
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()

	info, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return 0, EBADF
	}

	if info.isDir {
		return 0, EISDIR
	}

	// Write all data from buffer
	if err := fs.dataFile.WriteContent(info.ino, int64(offset), buf); err != nil {
		return 0, EIO
	}

	// Invalidate cache for the written file
	fs.InvalidateAttrPath(info.path)

	return len(buf), nil
}

// Truncate truncates a file
func (fs *LatentFS) Truncate(handle vfs.VfsHandle, size uint64) (err error) {
	defer recoverLatentFSPanic("Truncate", &err)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	info, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return EBADF
	}

	if info.isDir {
		return EISDIR
	}

	if err := fs.dataFile.TruncateContent(info.ino, int64(size)); err != nil {
		return err
	}

	// Invalidate cache for the truncated file
	fs.InvalidateAttrPath(info.path)

	return nil
}

// FSync flushes file data to disk
func (fs *LatentFS) FSync(handle vfs.VfsHandle) error {
	// SQLite handles durability
	return nil
}

// Flush flushes file data
func (fs *LatentFS) Flush(handle vfs.VfsHandle) error {
	return nil
}

// --- Directory Operations ---

// Mkdir creates a directory
func (fs *LatentFS) Mkdir(path string, mode int) (attrs *vfs.Attributes, err error) {
	defer recoverLatentFSPanic("Mkdir", &err)
	if log.IsLevelEnabled(log.TraceLevel) {
		start := time.Now()
		defer func() { log.Tracef("[VFS] Mkdir %q → %v (%v)", path, err, time.Since(start)) }()
	}
	log.Debugf("[VFS] Mkdir: path=%q mode=%o", path, mode)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path = common.NormalizePath(path)

	// DISABLED: Single-writer architecture - daemon handles all DB modifications.
	// IPC handlers call InvalidateCacheForDataFile for immediate cache invalidation.
	// fs.dataFile.CheckForExternalChanges()

	parentPath := common.ParentPath(path)
	name := common.BaseName(path)

	if name == "" {
		return nil, EINVAL
	}

	// Resolve parent
	parentIno, err := fs.dataFile.ResolvePath(parentPath)
	if err != nil {
		return nil, ENOENT
	}

	// Check if exists
	if _, err := fs.dataFile.Lookup(parentIno, name); err == nil {
		return nil, EEXIST
	}

	// Create inode - ensure ModeDir is always set
	dirMode := uint32(storage.ModeDir | (mode & 0777))
	ino, err := fs.dataFile.CreateInode(dirMode)
	if err != nil {
		log.Errorf("[VFS] Mkdir: CreateInode failed: %v", err)
		return nil, EIO
	}

	// Create dentry
	if err := fs.dataFile.CreateDentry(parentIno, name, ino); err != nil {
		fs.dataFile.DeleteInode(ino)
		return nil, EIO
	}

	// Invalidate caches for new directory and parent
	fs.InvalidateAttrPathAndParent(path)

	// Return attributes
	inode, err := fs.dataFile.GetInode(ino)
	if err != nil {
		return nil, EIO
	}

	return inodeToAttributes(inode), nil
}

// OpenDir opens a directory
func (fs *LatentFS) OpenDir(path string) (handle vfs.VfsHandle, err error) {
	defer recoverLatentFSPanic("OpenDir", &err)
	log.Debugf("[VFS] OpenDir: path=%q", path)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path = common.NormalizePath(path)

	// DISABLED: Single-writer architecture - daemon handles all DB modifications.
	// fs.dataFile.CheckForExternalChanges()

	// Resolve path in overlay
	ino, err := fs.dataFile.ResolvePath(path)
	if err != nil {
		if err == common.ErrNotFound {
			// Try source folder fallback (soft-fork)
			if fs.sourceFolder != "" {
				// Block source fallback if any path component has a dentry tombstone
				if fs.isPathTombstoned(path) {
					return 0, ENOENT
				}
				return fs.openDirFromSource(path)
			}
			return 0, ENOENT
		}
		return 0, EIO
	}

	// Check it's a directory
	inode, err := fs.dataFile.GetInode(ino)
	if err != nil {
		return 0, EIO
	}

	if !inode.IsDir() {
		return 0, ENOTDIR
	}

	// Allocate handle - pin epoch for consistency
	h := fs.handles.Allocate(ino, path, true, os.O_RDONLY, fs.dataFile.GetReadableEpoch())
	return vfs.VfsHandle(h), nil
}

// OpenAny opens a file or directory by path in a single lock acquisition.
// This eliminates the try-Open-then-OpenDir double-attempt pattern used by
// BillyAdapter methods like Rename() and Chmod().
func (fs *LatentFS) OpenAny(path string, flags int, mode int) (handle vfs.VfsHandle, err error) {
	defer recoverLatentFSPanic("OpenAny", &err)
	log.Debugf("[VFS] OpenAny: path=%q flags=%d mode=%d", path, flags, mode)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	path = common.NormalizePath(path)
	// DISABLED: Single-writer architecture - daemon handles all DB modifications.
	// fs.dataFile.CheckForExternalChanges()

	// Resolve path in overlay
	ino, resolveErr := fs.dataFile.ResolvePath(path)
	if resolveErr != nil {
		if resolveErr == common.ErrNotFound {
			// Try source folder fallback
			if fs.sourceFolder != "" {
				// Block source fallback if any path component has a dentry tombstone
				if fs.isPathTombstoned(path) {
					return 0, ENOENT
				}
				return fs.openAnyFromSource(path, flags, mode)
			}
			return 0, ENOENT
		}
		return 0, EIO
	}

	// Get inode to determine type
	inode, err := fs.dataFile.GetInode(ino)
	if err != nil {
		if err == common.ErrNotFound {
			// Tombstone
			return 0, ENOENT
		}
		return 0, EIO
	}
	if inode.IsDeleted() {
		return 0, ENOENT
	}

	if inode.IsDir() {
		h := fs.handles.Allocate(ino, path, true, os.O_RDONLY, fs.dataFile.GetReadableEpoch())
		return vfs.VfsHandle(h), nil
	}

	h := fs.handles.Allocate(ino, path, false, flags, fs.dataFile.GetReadableEpoch())
	return vfs.VfsHandle(h), nil
}

// ReadDir reads directory entries
func (fs *LatentFS) ReadDir(handle vfs.VfsHandle, offset int, count int) (entries []vfs.DirInfo, err error) {
	defer recoverLatentFSPanic("ReadDir", &err)
	if log.IsLevelEnabled(log.TraceLevel) {
		start := time.Now()
		defer func() {
			log.Tracef("[VFS] ReadDir handle=%d off=%d → %d entries, %v (%v)", handle, offset, len(entries), err, time.Since(start))
		}()
	}
	log.Debugf("[VFS] ReadDir: handle=%d offset=%d count=%d", handle, offset, count)
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	info, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		log.Debugf("[VFS] ReadDir: EBADF handle not found")
		return nil, EBADF
	}

	if !info.isDir {
		return nil, ENOTDIR
	}

	// SMB2 protocol: offset > 0 (RESTART_SCANS) means restart enumeration
	// offset == 0 means continue from where we left off
	if offset > 0 {
		// RESTART_SCANS - reset enumeration state
		fs.handles.SetDirEnumDone(HandleID(handle), false)
	}

	// If we already returned all entries, return EOF
	if fs.handles.IsDirEnumDone(HandleID(handle)) {
		log.Debugf("[VFS] ReadDir: returning EOF (enumeration done)")
		return nil, io.EOF
	}

	// Handle source-only directory (soft-fork)
	log.Debugf("[VFS] ReadDir: path=%q isSourceOnly=%v ino=%d", info.path, info.isSourceOnly, info.ino)
	if info.isSourceOnly {
		// Use resolver only for cascaded forks
		if fs.isSourceCascaded() {
			return fs.readDirFromResolver(handle, info)
		}
		return fs.readDirFromSource(handle, info)
	}

	// Get current directory's attributes for . and ..
	dirInode, err := fs.dataFile.GetInode(info.ino)
	if err != nil {
		log.Debugf("[VFS] ReadDir: GetInode(%d) failed: %v", info.ino, err)
		return nil, EIO
	}
	dirAttrs := inodeToAttributes(dirInode)

	// Build all entries including . and ..
	var allEntries []vfs.DirInfo

	// Add . and .. entries
	allEntries = append(allEntries,
		vfs.DirInfo{
			Name:       ".",
			Attributes: *dirAttrs,
		},
		vfs.DirInfo{
			Name:       "..",
			Attributes: *dirAttrs, // Simplified: use same attrs for parent
		})

	// Track names from overlay to avoid duplicates when merging
	overlayNames := make(map[string]bool)

	// Get directory contents from database (overlay)
	dbEntries, err := fs.dataFile.ListDir(info.ino)
	if err != nil {
		log.Debugf("[VFS] ReadDir: ListDir(%d) failed: %v", info.ino, err)
		return nil, EIO
	}
	log.Debugf("[VFS] ReadDir: got %d entries from overlay", len(dbEntries))
	for _, e := range dbEntries {
		log.Debugf("[VFS] ReadDir: overlay entry: name=%q ino=%d mode=%o", e.Name, e.Ino, e.Mode)
	}

	for _, e := range dbEntries {
		// Always add to overlayNames so tombstones block source entries
		overlayNames[e.Name] = true

		// Skip tombstones from listing (but they still block source)
		if e.Mode == storage.ModeDeleted {
			log.Debugf("[VFS] ReadDir: skipping tombstone entry: %q", e.Name)
			continue
		}

		di := vfs.DirInfo{
			Name: e.Name,
		}
		// Set attributes
		di.SetFileHandle(vfs.VfsNode(e.Ino))
		di.SetInodeNumber(uint64(e.Ino))
		di.SetSizeBytes(uint64(e.Size))
		di.SetLastDataModificationTime(e.Mtime)

		if e.Mode&storage.ModeMask == storage.ModeDir {
			di.SetFileType(vfs.FileTypeDirectory)
		} else if e.Mode&storage.ModeMask == storage.ModeSymlink {
			di.SetFileType(vfs.FileTypeSymlink)
		} else {
			di.SetFileType(vfs.FileTypeRegularFile)
		}
		di.SetPermissions(vfs.NewPermissionsFromMode(e.Mode))
		di.SetUnixMode(e.Mode & 0777)

		allEntries = append(allEntries, di)
	}

	// Merge entries from source folder (soft-fork)
	if fs.sourceFolder != "" {
		// Add dentry tombstone names to overlayNames so they block source entries.
		// ListDirEntries uses WHERE ino != 0, so dentry tombstones (ino=0) are not
		// included in dbEntries above. We must query them separately.
		tombstoneNames, err := fs.dataFile.ListDentryTombstoneNames(info.ino)
		if err != nil {
			log.Debugf("[VFS] ReadDir: ListDentryTombstoneNames(%d) failed: %v", info.ino, err)
		}
		for _, name := range tombstoneNames {
			overlayNames[name] = true
		}
		sourceEntries := fs.listSourceDir(info.path, overlayNames)
		allEntries = append(allEntries, sourceEntries...)
	}

	// Mark enumeration as done - next call will return EOF
	fs.handles.SetDirEnumDone(HandleID(handle), true)

	// Apply count limit if needed
	if count > 0 && count < len(allEntries) {
		return allEntries[:count], nil
	}

	return allEntries, nil
}

// --- Metadata Operations ---

// GetAttr gets file attributes
func (fs *LatentFS) GetAttr(handle vfs.VfsHandle) (attrs *vfs.Attributes, err error) {
	defer recoverLatentFSPanic("GetAttr", &err)
	log.Debugf("[VFS] GetAttr: handle=%d", handle)
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Handle 0 means root directory
	var ino int64 = storage.RootIno
	if handle != 0 {
		info, ok := fs.handles.Get(HandleID(handle))
		if !ok {
			return nil, EBADF
		}

		// Handle source-only files (soft-fork)
		if info.isSourceOnly {
			// Use resolver only for cascaded forks
			if fs.isSourceCascaded() {
				return fs.getAttrFromResolver(info.path)
			}
			return fs.getAttrFromSource(info.sourcePath, info.path)
		}

		ino = info.ino
	}

	inode, err := fs.dataFile.GetInode(ino)
	if err != nil {
		return nil, EIO
	}

	return inodeToAttributes(inode), nil
}

// SetAttr sets file attributes
func (fs *LatentFS) SetAttr(handle vfs.VfsHandle, inAttrs *vfs.Attributes) (attrs *vfs.Attributes, err error) {
	defer recoverLatentFSPanic("SetAttr", &err)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	info, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return nil, EBADF
	}

	updates := &storage.InodeUpdate{}

	if mode, ok := inAttrs.GetUnixMode(); ok {
		// Preserve the file type bits (ModeDir, ModeFile, etc.) and only update permission bits
		currentInode, err := fs.dataFile.GetInode(info.ino)
		if err != nil {
			return nil, EIO
		}
		// Keep type bits from current mode, apply new permission bits
		newMode := (currentInode.Mode & storage.ModeMask) | (uint32(mode) & 0777)
		updates.Mode = &newMode
	}

	if size, ok := inAttrs.GetSizeBytes(); ok {
		s := int64(size)
		updates.Size = &s
		// Also truncate content
		if err := fs.dataFile.TruncateContent(info.ino, int64(size)); err != nil {
			return nil, EIO
		}
	}

	if mtime, ok := inAttrs.GetLastDataModificationTime(); ok {
		updates.Mtime = &mtime
	}

	if atime, ok := inAttrs.GetAccessTime(); ok {
		updates.Atime = &atime
	}

	if err := fs.dataFile.UpdateInode(info.ino, updates); err != nil {
		return nil, EIO
	}

	// Invalidate cache for the modified file
	fs.InvalidateAttrPath(info.path)

	// Return updated attributes
	inode, err := fs.dataFile.GetInode(info.ino)
	if err != nil {
		return nil, EIO
	}

	return inodeToAttributes(inode), nil
}

// Lookup finds a file in a directory
func (fs *LatentFS) Lookup(dirHandle vfs.VfsHandle, name string) (attrs *vfs.Attributes, err error) {
	defer recoverLatentFSPanic("Lookup", &err)
	log.Debugf("[VFS] Lookup: dirHandle=%d name=%q", dirHandle, name)
	defer func() {
		log.Debugf("[VFS] Lookup done: dirHandle=%d name=%q", dirHandle, name)
	}()
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Handle 0 means root directory
	var parentIno int64 = storage.RootIno
	if dirHandle != 0 {
		info, ok := fs.handles.Get(HandleID(dirHandle))
		if !ok {
			return nil, EBADF
		}
		if !info.isDir {
			return nil, ENOTDIR
		}
		parentIno = info.ino
	}

	// Looking up "/" or "" or "." in root means return root/parent attributes
	if name == "/" || name == "" || name == "." {
		inode, err := fs.dataFile.GetInode(parentIno)
		if err != nil {
			return nil, EIO
		}
		return inodeToAttributes(inode), nil
	}

	// Clean the name (handle "/" prefix)
	if len(name) > 0 && name[0] == '/' {
		name = name[1:]
	}
	if name == "" {
		// Empty name after stripping "/" - return parent attributes
		inode, err := fs.dataFile.GetInode(parentIno)
		if err != nil {
			return nil, EIO
		}
		return inodeToAttributes(inode), nil
	}

	// Handle full paths (e.g., "subdir/nested.txt") by walking each component
	parts := common.SplitPath(name)
	currentIno := parentIno
	var lookupPath string // Track the path being looked up

	for i, part := range parts {
		dentry, err := fs.dataFile.Lookup(currentIno, part)
		if err != nil {
			if err == common.ErrNotFound {
				// Check if this is a dentry tombstone (deleted file) — don't fall through to source
				if fs.sourceFolder != "" && fs.dataFile.IsDentryTombstoned(currentIno, part) {
					return nil, ENOENT
				}
				// Try source folder fallback (soft-fork)
				if fs.sourceFolder != "" {
					// Build full path
					if lookupPath == "" {
						lookupPath = "/" + part
					} else {
						lookupPath = lookupPath + "/" + part
					}
					// Add remaining parts
					for j := i + 1; j < len(parts); j++ {
						lookupPath = lookupPath + "/" + parts[j]
					}
					return fs.lookupFromSource(lookupPath)
				}
				return nil, ENOENT
			}
			return nil, EIO
		}

		// Check if this component is a tombstone
		componentInode, err := fs.dataFile.GetInode(dentry.Ino)
		if err != nil {
			return nil, EIO
		}
		if componentInode.IsDeleted() {
			// Tombstone blocks lookup - file/dir was deleted
			return nil, ENOENT
		}

		currentIno = dentry.Ino
		if lookupPath == "" {
			lookupPath = "/" + part
		} else {
			lookupPath = lookupPath + "/" + part
		}
	}

	inode, err := fs.dataFile.GetInode(currentIno)
	if err != nil {
		return nil, EIO
	}

	// Check for tombstone (deleted source file)
	if inode.IsDeleted() {
		return nil, ENOENT
	}

	attrs = inodeToAttributes(inode)
	log.Debugf("[VFS] Lookup result: name=%q ino=%d mode=%o isDir=%v fileType=%v",
		name, inode.Ino, inode.Mode, inode.IsDir(), attrs.GetFileType())
	return attrs, nil
}

// StatFS returns filesystem statistics
func (fs *LatentFS) StatFS(handle vfs.VfsHandle) (*vfs.FSAttributes, error) {
	attrs := &vfs.FSAttributes{}
	attrs.SetBlockSize(4096)
	attrs.SetIOSize(4096)
	attrs.SetBlocks(1000000)
	attrs.SetFreeBlocks(500000)
	attrs.SetAvailableBlocks(500000)
	attrs.SetFiles(100000)
	attrs.SetFreeFiles(50000)
	return attrs, nil
}

// --- File Management ---

// Unlink removes a file or empty directory
func (fs *LatentFS) Unlink(handle vfs.VfsHandle) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	log.Debugf("[VFS] Unlink: handle=%d", handle)
	info, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		log.Debugf("[VFS] Unlink: handle not found, returning EBADF")
		return EBADF
	}
	log.Debugf("[VFS] Unlink: path=%q ino=%d isDir=%v isSourceOnly=%v", info.path, info.ino, info.isDir, info.isSourceOnly)

	// Handle source-only files (soft-fork): create tombstone to block source fallback
	if info.isSourceOnly {
		log.Debugf("[VFS] Unlink: source-only file, creating tombstone")
		err := fs.createTombstoneLocked(info.path)
		if err != nil {
			log.Debugf("[VFS] Unlink: createTombstoneLocked failed: %v", err)
		} else {
			// Invalidate cache after creating tombstone
			log.Debugf("[VFS] Unlink: tombstone created, invalidating cache for %q", info.path)
			fs.InvalidateAttrPathAndParent(info.path)
		}
		return err
	}

	// If it's a directory, check that it's logically empty (ignoring tombstones).
	// O6: Use HasLiveChildren (EXISTS + LIMIT 1) instead of ListDir.
	if info.isDir {
		hasChildren, err := fs.dataFile.HasLiveChildren(info.ino)
		if err != nil {
			log.Debugf("[VFS] Unlink: HasLiveChildren failed: %v", err)
			return EIO
		}
		if hasChildren {
			log.Debugf("[VFS] Unlink: directory not empty")
			return ENOTEMPTY
		}
	}

	// Get parent and name from path
	parentPath := common.ParentPath(info.path)
	name := common.BaseName(info.path)

	parentIno, err := fs.dataFile.ResolvePath(parentPath)
	if err != nil {
		return EIO
	}

	// Delete dentry + inode in a single transaction (M1 + M2 optimization)
	ctx := context.Background()
	txErr := fs.dataFile.RunInTx(ctx, func(ctx context.Context, tx bun.Tx) error {
		return fs.dataFile.DeleteDentryAndInodeTx(ctx, tx, parentIno, name, info.ino, info.isDir)
	})
	if txErr != nil {
		return EIO
	}

	// Invalidate caches for removed path and parent
	fs.InvalidateAttrPathAndParent(info.path)

	return nil
}

// UnlinkByPath removes a file or empty directory by path without requiring a handle.
// This avoids the Open/Unlink/Close round-trip and the try-file-then-dir double-attempt
// used by BillyAdapter.Remove(). A single Lock acquisition replaces 2-3 separate ones.
// All DB mutations are wrapped in a single SQLite transaction (M1 optimization).
func (fs *LatentFS) UnlinkByPath(vfsPath string) (err error) {
	defer recoverLatentFSPanic("UnlinkByPath", &err)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	vfsPath = common.NormalizePath(vfsPath)
	log.Debugf("[VFS] UnlinkByPath: path=%q", vfsPath)

	// DISABLED: Single-writer architecture - daemon handles all DB modifications.
	// fs.dataFile.CheckForExternalChanges()

	// Try to resolve in overlay
	ino, resolveErr := fs.dataFile.ResolvePath(vfsPath)
	log.Debugf("[VFS] UnlinkByPath %q: ResolvePath → ino=%d err=%v", vfsPath, ino, resolveErr)
	if resolveErr != nil {
		if resolveErr == common.ErrNotFound {
			// Not in overlay — check source folder for tombstone creation
			if fs.sourceFolder != "" {
				log.Debugf("[VFS] UnlinkByPath %q: creating tombstone for source-only file", vfsPath)
				err := fs.unlinkSourceOnly(vfsPath)
				if err == nil {
					// Invalidate cache after creating tombstone
					log.Debugf("[VFS] UnlinkByPath %q: tombstone created, invalidating cache", vfsPath)
					fs.InvalidateAttrPathAndParent(vfsPath)
				}
				return err
			}
			return ENOENT
		}
		return EIO
	}

	// Get inode to check type and tombstone status
	inode, err := fs.dataFile.GetInode(ino)
	if err != nil {
		if err == common.ErrNotFound {
			// Tombstone — path exists but inode is deleted; source-only tombstone path
			if fs.sourceFolder != "" {
				err := fs.unlinkSourceOnly(vfsPath)
				if err == nil {
					// Invalidate cache after creating tombstone
					fs.InvalidateAttrPathAndParent(vfsPath)
				}
				return err
			}
			return ENOENT
		}
		return EIO
	}

	if inode.IsDeleted() {
		// Already deleted tombstone
		return ENOENT
	}

	// If it's a directory, check that it's logically empty (ignoring tombstones).
	// O6: Use HasLiveChildren (EXISTS + LIMIT 1) instead of ListDir.
	isDir := inode.IsDir()
	if isDir {
		hasChildren, err := fs.dataFile.HasLiveChildren(ino)
		if err != nil {
			return EIO
		}
		if hasChildren {
			return ENOTEMPTY
		}
	}

	// Delete dentry + inode in a single transaction (M1 + M2 optimization).
	// The caller already knows childIno and isDir, so DeleteDentryAndInodeTx
	// skips the redundant Lookup + GetInode reads that DeleteDentry does.
	parentPath := common.ParentPath(vfsPath)
	name := common.BaseName(vfsPath)

	parentIno, err := fs.dataFile.ResolvePath(parentPath)
	if err != nil {
		return EIO
	}

	ctx := context.Background()
	txErr := fs.dataFile.RunInTx(ctx, func(ctx context.Context, tx bun.Tx) error {
		return fs.dataFile.DeleteDentryAndInodeTx(ctx, tx, parentIno, name, ino, isDir)
	})
	if txErr != nil {
		return EIO
	}

	// Invalidate caches for removed path and parent
	fs.InvalidateAttrPathAndParent(vfsPath)

	return nil
}

// Rename renames/moves a file
func (fs *LatentFS) Rename(handle vfs.VfsHandle, newName string, flags int) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	info, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return EBADF
	}

	// Get source parent and name
	srcParentPath := common.ParentPath(info.path)
	srcName := common.BaseName(info.path)

	srcParentIno, err := fs.dataFile.ResolvePath(srcParentPath)
	if err != nil {
		return EIO
	}

	// Parse destination - newName is just the new name in same directory
	dstParentIno := srcParentIno
	dstName := newName

	// Perform rename
	if err := fs.dataFile.RenameDentry(srcParentIno, srcName, dstParentIno, dstName); err != nil {
		return EIO
	}

	// Invalidate caches for old path, new path, and parent(s)
	newPath := srcParentPath + "/" + dstName
	if srcParentPath == "/" {
		newPath = "/" + dstName
	}
	fs.InvalidateAttrRename(info.path, newPath)

	return nil
}

// --- Symbolic Link Operations ---

// Readlink reads a symbolic link target
func (fs *LatentFS) Readlink(handle vfs.VfsHandle) (string, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	info, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return "", EBADF
	}

	// Handle source-only symlinks (soft-fork)
	if info.isSourceOnly {
		return os.Readlink(info.sourcePath)
	}

	// Get inode to verify it's a symlink
	inode, err := fs.dataFile.GetInode(info.ino)
	if err != nil {
		return "", EIO
	}

	if !inode.IsSymlink() {
		return "", EINVAL
	}

	// Read symlink target from database
	target, err := fs.dataFile.ReadSymlink(info.ino)
	if err != nil {
		if err == common.ErrNotFound {
			return "", EINVAL
		}
		return "", EIO
	}

	return target, nil
}

// Symlink converts an existing file to a symbolic link
// The handle points to a file that will be converted to a symlink pointing to target
func (fs *LatentFS) Symlink(handle vfs.VfsHandle, target string, mode int) (*vfs.Attributes, error) {
	log.Debugf("[VFS] Symlink: handle=%d target=%q mode=%d", handle, target, mode)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	info, ok := fs.handles.Get(HandleID(handle))
	if !ok {
		return nil, EBADF
	}

	// Delete any existing content for this inode
	if err := fs.dataFile.TruncateContent(info.ino, 0); err != nil {
		return nil, EIO
	}

	// Update inode mode to symlink type
	symlinkMode := uint32(storage.ModeSymlink | 0777)
	updates := &storage.InodeUpdate{
		Mode: &symlinkMode,
	}
	if err := fs.dataFile.UpdateInode(info.ino, updates); err != nil {
		return nil, EIO
	}

	// Store the symlink target
	if err := fs.dataFile.CreateSymlink(info.ino, target); err != nil {
		return nil, EIO
	}

	// Invalidate cache for the converted symlink
	fs.InvalidateAttrPath(info.path)

	// Return updated attributes
	inode, err := fs.dataFile.GetInode(info.ino)
	if err != nil {
		return nil, EIO
	}

	return inodeToAttributes(inode), nil
}

// Link creates a hard link
func (fs *LatentFS) Link(srcNode vfs.VfsNode, dstNode vfs.VfsNode, name string) (*vfs.Attributes, error) {
	return nil, ENOTSUP
}

// --- Extended Attributes (stub implementation) ---

func (fs *LatentFS) Listxattr(handle vfs.VfsHandle) ([]string, error) {
	// Return empty list - no xattrs supported
	return []string{}, nil
}

func (fs *LatentFS) Getxattr(handle vfs.VfsHandle, name string, buf []byte) (int, error) {
	// No xattrs supported
	return 0, ENOATTR
}

func (fs *LatentFS) Setxattr(handle vfs.VfsHandle, name string, value []byte) error {
	// Silently succeed (some SMB clients expect this to work)
	return nil
}

func (fs *LatentFS) Removexattr(handle vfs.VfsHandle, name string) error {
	// Silently succeed
	return nil
}

