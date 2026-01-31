package vfs

import "sync"

// HandleID is the type for VFS handles
type HandleID uint64

// SourceOnlyIno is a sentinel value indicating the handle is for a source-only file
const SourceOnlyIno int64 = -1

// openHandle represents an open file or directory
type openHandle struct {
	ino         int64
	pinnedEpoch int64  // Epoch at time of Open - ensures read consistency across chunks
	path        string // path within the VFS (relative)
	isDir       bool
	flags       int
	dirPos      int  // For ReadDir pagination
	dirEnumDone bool // True if directory enumeration completed (for SMB)

	// Source folder fallback support (for soft-fork/copy-on-write)
	isSourceOnly bool   // true if file exists only in source folder (not in overlay)
	sourcePath   string // full path to file in source folder (set when isSourceOnly=true)
}

// ParentVFS interface for delegated reads from parent VFS
// This avoids import cycles by defining the interface here
type ParentVFS interface {
	ReadFileContent(path string) ([]byte, error)
	StatPath(path string) (size int64, isDir bool, err error)
	ListDir(path string) ([]ParentDirEntry, error)
}

// ParentDirEntry represents a directory entry from parent VFS
type ParentDirEntry struct {
	Name  string
	IsDir bool
	Size  int64
}

// HandleManager manages VFS handles
type HandleManager struct {
	mu         sync.RWMutex
	handles    map[HandleID]*openHandle
	nextHandle HandleID
}

// NewHandleManager creates a new handle manager
func NewHandleManager() *HandleManager {
	return &HandleManager{
		handles:    make(map[HandleID]*openHandle),
		nextHandle: 1,
	}
}

// Allocate creates a new handle for the given inode
// epoch is pinned at open time to ensure read consistency across multiple read operations
func (hm *HandleManager) Allocate(ino int64, path string, isDir bool, flags int, epoch int64) HandleID {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	handle := hm.nextHandle
	hm.nextHandle++

	hm.handles[handle] = &openHandle{
		ino:         ino,
		pinnedEpoch: epoch,
		path:        path,
		isDir:       isDir,
		flags:       flags,
	}

	return handle
}

// AllocateSourceOnly creates a handle for a source-only file (not in overlay)
func (hm *HandleManager) AllocateSourceOnly(vfsPath, sourcePath string, isDir bool, flags int) HandleID {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	handle := hm.nextHandle
	hm.nextHandle++

	hm.handles[handle] = &openHandle{
		ino:          SourceOnlyIno,
		path:         vfsPath,
		isDir:        isDir,
		flags:        flags,
		isSourceOnly: true,
		sourcePath:   sourcePath,
	}

	return handle
}


// Get retrieves a handle's info
func (hm *HandleManager) Get(h HandleID) (*openHandle, bool) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	info, ok := hm.handles[h]
	return info, ok
}

// Release frees a handle
func (hm *HandleManager) Release(h HandleID) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	delete(hm.handles, h)
}

// UpdateDirPos updates the directory position for ReadDir
func (hm *HandleManager) UpdateDirPos(h HandleID, pos int) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	if info, ok := hm.handles[h]; ok {
		info.dirPos = pos
	}
}

// GetDirPos gets the current directory position
func (hm *HandleManager) GetDirPos(h HandleID) int {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	if info, ok := hm.handles[h]; ok {
		return info.dirPos
	}
	return 0
}

// SetDirEnumDone marks directory enumeration as complete
func (hm *HandleManager) SetDirEnumDone(h HandleID, done bool) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	if info, ok := hm.handles[h]; ok {
		info.dirEnumDone = done
	}
}

// IsDirEnumDone checks if directory enumeration is complete
func (hm *HandleManager) IsDirEnumDone(h HandleID) bool {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	if info, ok := hm.handles[h]; ok {
		return info.dirEnumDone
	}
	return false
}

// Clear removes all handles, returning the count of handles cleared
// This is used for cache invalidation when external changes occur
func (hm *HandleManager) Clear() int {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	count := len(hm.handles)
	hm.handles = make(map[HandleID]*openHandle)
	// Don't reset nextHandle to avoid handle ID reuse issues
	return count
}
