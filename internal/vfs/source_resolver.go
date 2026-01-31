package vfs

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/macos-fuse-t/go-smb2/vfs"
	log "github.com/sirupsen/logrus"

	"latentfs/internal/storage"
)

// mountInChain represents a mount point in the cascade chain
type mountInChain struct {
	symlinkTarget string // The actual mount path (e.g., /foo/a/b)
	sourceFolder  string // That fork's source folder
}

// SourceResolver handles all source folder access for soft-forks.
// It uses iterative stack-based traversal to avoid stack overflow with deep cascaded chains.
// Shared by all VFSes, has access to MetaFS mount info for dynamic VFS resolution.
type SourceResolver struct {
	metaFS *MetaFS
}

// NewSourceResolver creates a new SourceResolver
func NewSourceResolver(metaFS *MetaFS) *SourceResolver {
	return &SourceResolver{metaFS: metaFS}
}

// sourceRequest represents a pending source lookup in the stack
type sourceRequest struct {
	sourcePath   string
	relativePath string
}

// collectMountsInChain walks the cascade chain and collects all mount targets
// Returns list of (symlinkTarget, sourceFolder) pairs in chain order
func (r *SourceResolver) collectMountsInChain(startSourcePath string) []mountInChain {
	var mounts []mountInChain
	current := startSourcePath

	for {
		vfs := r.metaFS.GetVFSBySymlinkTarget(current)
		if vfs == nil {
			break // Reached real filesystem
		}
		mounts = append(mounts, mountInChain{
			symlinkTarget: vfs.symlinkTarget,
			sourceFolder:  vfs.sourceFolder,
		})
		if vfs.sourceFolder == "" {
			break
		}
		current = vfs.sourceFolder
	}

	return mounts
}

// computeHiddenPathsForSource computes all paths that should be hidden
// relative to a given source folder, based on all mounts in the chain
func (r *SourceResolver) computeHiddenPathsForSource(sourceFolder string, mounts []mountInChain) []string {
	var hidden []string

	sourceAbs, err := filepath.Abs(sourceFolder)
	if err != nil {
		return nil
	}
	sourcePrefix := sourceAbs
	if !strings.HasSuffix(sourcePrefix, string(filepath.Separator)) {
		sourcePrefix += string(filepath.Separator)
	}

	for _, m := range mounts {
		targetAbs, err := filepath.Abs(m.symlinkTarget)
		if err != nil {
			continue
		}

		// Check if this mount is inside sourceFolder
		if !strings.HasPrefix(targetAbs, sourcePrefix) && targetAbs != sourceAbs {
			continue // Not inside this source
		}

		// Compute relative path
		relPath, err := filepath.Rel(sourceAbs, targetAbs)
		if err != nil || relPath == "." || strings.HasPrefix(relPath, "..") {
			continue
		}

		hidden = append(hidden, relPath)
	}

	return hidden
}

// isEntryHiddenAtLevel checks if entry should be hidden given the hidden set for this level
func (r *SourceResolver) isEntryHiddenAtLevel(parentPath, name string, hiddenSet map[string]bool) bool {
	// Build the full path relative to source
	var fullPath string
	if parentPath == "/" || parentPath == "" {
		fullPath = name
	} else {
		normalizedParent := strings.TrimPrefix(parentPath, "/")
		if normalizedParent == "" {
			fullPath = name
		} else {
			fullPath = normalizedParent + "/" + name
		}
	}

	// Check if this path matches any hidden path
	return hiddenSet[fullPath]
}

// isPathHiddenInChain checks if a path should be hidden based on cascade chain mounts
func (r *SourceResolver) isPathHiddenInChain(normalizedPath string, hiddenPaths []string) bool {
	for _, h := range hiddenPaths {
		if normalizedPath == h || strings.HasPrefix(normalizedPath, h+"/") {
			return true
		}
	}
	return false
}

// ListDirWithHiding iteratively lists directory with cascaded hidden path awareness
func (r *SourceResolver) ListDirWithHiding(sourcePath, relativePath, initiatorMount string) ([]SourceDirEntry, error) {
	// Collect all mounts in the chain (including initiator)
	mounts := r.collectMountsInChain(sourcePath)
	// Add initiator mount if not already in chain
	if initiatorMount != "" {
		mounts = append([]mountInChain{{
			symlinkTarget: initiatorMount,
			sourceFolder:  sourcePath,
		}}, mounts...)
	}

	seen := make(map[string]bool) // Track seen entries (child entries shadow parent)
	var result []SourceDirEntry

	stack := []sourceRequest{{sourcePath, relativePath}}

	for len(stack) > 0 {
		req := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Compute hidden paths for THIS source level
		hiddenAtLevel := r.computeHiddenPathsForSource(req.sourcePath, mounts)
		hiddenSet := make(map[string]bool)
		for _, h := range hiddenAtLevel {
			hiddenSet[h] = true
		}

		log.Debugf("[SourceResolver] ListDirWithHiding: source=%q path=%q hidden=%v", req.sourcePath, req.relativePath, hiddenAtLevel)

		if vfs := r.metaFS.GetVFSBySymlinkTarget(req.sourcePath); vfs != nil {
			// Get local entries from VFS
			entries := vfs.getLocalDirEntries(req.relativePath)
			for _, e := range entries {
				if !seen[e.Name] && !r.isEntryHiddenAtLevel(req.relativePath, e.Name, hiddenSet) {
					seen[e.Name] = true
					result = append(result, e)
				}
			}
			// Also need to check VFS's source for more entries
			if vfs.sourceFolder != "" {
				stack = append(stack, sourceRequest{vfs.sourceFolder, req.relativePath})
				continue
			}
		} else {
			// Real filesystem
			fullPath := filepath.Join(req.sourcePath, req.relativePath)
			entries, err := os.ReadDir(fullPath)
			if err == nil {
				for _, e := range entries {
					if !seen[e.Name()] && !r.isEntryHiddenAtLevel(req.relativePath, e.Name(), hiddenSet) {
						seen[e.Name()] = true
						info, _ := e.Info()
						var size int64
						var mode os.FileMode = 0644
						if info != nil {
							size = info.Size()
							mode = info.Mode()
						}
						result = append(result, SourceDirEntry{
							Name:  e.Name(),
							IsDir: e.IsDir(),
							Size:  size,
							Mode:  mode,
						})
					}
				}
			}
		}
	}

	return result, nil
}

// LookupWithHiding checks if a path exists, respecting cascaded hidden paths
func (r *SourceResolver) LookupWithHiding(sourcePath, relativePath, initiatorMount string) (*vfs.Attributes, error) {
	mounts := r.collectMountsInChain(sourcePath)
	if initiatorMount != "" {
		mounts = append([]mountInChain{{
			symlinkTarget: initiatorMount,
			sourceFolder:  sourcePath,
		}}, mounts...)
	}

	stack := []sourceRequest{{sourcePath, relativePath}}

	for len(stack) > 0 {
		req := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Check if this path is hidden at this level
		hiddenAtLevel := r.computeHiddenPathsForSource(req.sourcePath, mounts)
		normalizedPath := strings.TrimPrefix(req.relativePath, "/")
		if r.isPathHiddenInChain(normalizedPath, hiddenAtLevel) {
			log.Debugf("[SourceResolver] LookupWithHiding: path %q hidden at source %q", normalizedPath, req.sourcePath)
			return nil, ENOENT // Path is hidden
		}

		if vfsFS := r.metaFS.GetVFSBySymlinkTarget(req.sourcePath); vfsFS != nil {
			if attrs, found := vfsFS.tryLocalLookup(req.relativePath); found {
				return attrs, nil
			}
			if vfsFS.sourceFolder != "" {
				stack = append(stack, sourceRequest{vfsFS.sourceFolder, req.relativePath})
				continue
			}
			return nil, ENOENT
		}

		// Real filesystem
		fullPath := filepath.Join(req.sourcePath, req.relativePath)
		info, err := os.Stat(fullPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, ENOENT
			}
			return nil, EIO
		}
		return osFileInfoToAttributes(info, req.relativePath), nil
	}

	return nil, ENOENT
}

// StatWithHiding iteratively traverses the chain with cascade-aware hidden path checking
func (r *SourceResolver) StatWithHiding(sourcePath, relativePath, initiatorMount string) (size int64, isDir bool, err error) {
	mounts := r.collectMountsInChain(sourcePath)
	if initiatorMount != "" {
		mounts = append([]mountInChain{{
			symlinkTarget: initiatorMount,
			sourceFolder:  sourcePath,
		}}, mounts...)
	}

	stack := []sourceRequest{{sourcePath, relativePath}}

	for len(stack) > 0 {
		req := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Check if this path is hidden at this level
		hiddenAtLevel := r.computeHiddenPathsForSource(req.sourcePath, mounts)
		normalizedPath := strings.TrimPrefix(req.relativePath, "/")
		if r.isPathHiddenInChain(normalizedPath, hiddenAtLevel) {
			return 0, false, ENOENT
		}

		if vfs := r.metaFS.GetVFSBySymlinkTarget(req.sourcePath); vfs != nil {
			if size, isDir, found := vfs.tryLocalStat(req.relativePath); found {
				return size, isDir, nil
			}
			if vfs.sourceFolder != "" {
				stack = append(stack, sourceRequest{vfs.sourceFolder, req.relativePath})
				continue
			}
			return 0, false, ENOENT
		}

		// Real filesystem
		fullPath := filepath.Join(req.sourcePath, req.relativePath)
		info, err := os.Stat(fullPath)
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

// ReadFileWithHiding iteratively traverses the chain with cascade-aware hidden path checking
func (r *SourceResolver) ReadFileWithHiding(sourcePath, relativePath, initiatorMount string) ([]byte, error) {
	mounts := r.collectMountsInChain(sourcePath)
	if initiatorMount != "" {
		mounts = append([]mountInChain{{
			symlinkTarget: initiatorMount,
			sourceFolder:  sourcePath,
		}}, mounts...)
	}

	stack := []sourceRequest{{sourcePath, relativePath}}

	for len(stack) > 0 {
		req := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Check if this path is hidden at this level
		hiddenAtLevel := r.computeHiddenPathsForSource(req.sourcePath, mounts)
		normalizedPath := strings.TrimPrefix(req.relativePath, "/")
		if r.isPathHiddenInChain(normalizedPath, hiddenAtLevel) {
			return nil, ENOENT
		}

		if vfs := r.metaFS.GetVFSBySymlinkTarget(req.sourcePath); vfs != nil {
			if content, found := vfs.tryLocalRead(req.relativePath); found {
				return content, nil
			}
			if vfs.sourceFolder != "" {
				stack = append(stack, sourceRequest{vfs.sourceFolder, req.relativePath})
				continue
			}
			return nil, ENOENT
		}

		// Real filesystem
		fullPath := filepath.Join(req.sourcePath, req.relativePath)
		return os.ReadFile(fullPath)
	}

	return nil, ENOENT
}

// ReadFile iteratively traverses the chain using a stack to read file content
func (r *SourceResolver) ReadFile(sourcePath, relativePath string) ([]byte, error) {
	// Stack for iterative traversal (avoids recursive function calls)
	stack := []sourceRequest{{sourcePath, relativePath}}

	for len(stack) > 0 {
		// Pop from stack
		req := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		log.Debugf("[SourceResolver] ReadFile: checking source=%q path=%q", req.sourcePath, req.relativePath)

		// Check if this source path is a mounted VFS
		if vfs := r.metaFS.GetVFSBySymlinkTarget(req.sourcePath); vfs != nil {
			// Try to read from VFS's local data
			if content, found := vfs.tryLocalRead(req.relativePath); found {
				log.Debugf("[SourceResolver] ReadFile: found in VFS local data")
				return content, nil
			}
			// File not in VFS's local data - need to check VFS's source
			if vfs.sourceFolder != "" {
				// Push VFS's source onto stack (continue iteration)
				stack = append(stack, sourceRequest{vfs.sourceFolder, req.relativePath})
				continue
			}
			// VFS has no source folder - file not found
			return nil, ENOENT
		}

		// Not a VFS mount - read from real filesystem
		fullPath := filepath.Join(req.sourcePath, req.relativePath)
		log.Debugf("[SourceResolver] ReadFile: reading from filesystem %q", fullPath)
		return os.ReadFile(fullPath)
	}

	return nil, ENOENT
}

// Stat iteratively traverses the chain using a stack to get file info
func (r *SourceResolver) Stat(sourcePath, relativePath string) (size int64, isDir bool, err error) {
	stack := []sourceRequest{{sourcePath, relativePath}}

	for len(stack) > 0 {
		req := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		log.Debugf("[SourceResolver] Stat: checking source=%q path=%q", req.sourcePath, req.relativePath)

		if vfs := r.metaFS.GetVFSBySymlinkTarget(req.sourcePath); vfs != nil {
			// Check VFS's local data
			if size, isDir, found := vfs.tryLocalStat(req.relativePath); found {
				log.Debugf("[SourceResolver] Stat: found in local data, size=%d isDir=%v", size, isDir)
				return size, isDir, nil
			}
			// Not in local data - check VFS's source
			if vfs.sourceFolder != "" {
				stack = append(stack, sourceRequest{vfs.sourceFolder, req.relativePath})
				continue
			}
			return 0, false, ENOENT
		}

		// Real filesystem
		fullPath := filepath.Join(req.sourcePath, req.relativePath)
		info, err := os.Stat(fullPath)
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

// StatWithInfo iteratively traverses the chain to get full file info (for COW operations)
// Returns os.FileInfo with proper mtime - critical for NFS cache invalidation
func (r *SourceResolver) StatWithInfo(sourcePath, relativePath string) (os.FileInfo, error) {
	stack := []sourceRequest{{sourcePath, relativePath}}

	for len(stack) > 0 {
		req := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if vfs := r.metaFS.GetVFSBySymlinkTarget(req.sourcePath); vfs != nil {
			// Check VFS's local data - if found, create synthetic FileInfo with mtime
			if info, found := vfs.tryLocalFileInfo(req.relativePath); found {
				return info, nil
			}
			if vfs.sourceFolder != "" {
				stack = append(stack, sourceRequest{vfs.sourceFolder, req.relativePath})
				continue
			}
			return nil, ENOENT
		}

		// Real filesystem
		fullPath := filepath.Join(req.sourcePath, req.relativePath)
		return os.Lstat(fullPath)
	}

	return nil, ENOENT
}

// syntheticFileInfo implements os.FileInfo for VFS files
type syntheticFileInfo struct {
	name    string
	size    int64
	isDir   bool
	mode    os.FileMode
	modTime time.Time
}

func (f *syntheticFileInfo) Name() string       { return f.name }
func (f *syntheticFileInfo) Size() int64        { return f.size }
func (f *syntheticFileInfo) Mode() os.FileMode  { return f.mode }
func (f *syntheticFileInfo) ModTime() time.Time { return f.modTime }
func (f *syntheticFileInfo) IsDir() bool        { return f.isDir }
func (f *syntheticFileInfo) Sys() any           { return nil }

// SourceDirEntry represents a directory entry from source resolution
type SourceDirEntry struct {
	Name  string
	IsDir bool
	Size  int64
	Mode  os.FileMode
}

// ListDir iteratively traverses the chain to list directory entries
// Merges entries from all levels (child entries shadow parent)
func (r *SourceResolver) ListDir(sourcePath, relativePath string) ([]SourceDirEntry, error) {
	seen := make(map[string]bool) // Track seen entries (child entries shadow parent)
	var result []SourceDirEntry

	stack := []sourceRequest{{sourcePath, relativePath}}

	for len(stack) > 0 {
		req := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		log.Debugf("[SourceResolver] ListDir: checking source=%q path=%q", req.sourcePath, req.relativePath)

		if vfs := r.metaFS.GetVFSBySymlinkTarget(req.sourcePath); vfs != nil {
			// Get local entries from VFS
			entries := vfs.getLocalDirEntries(req.relativePath)
			for _, e := range entries {
				if !seen[e.Name] {
					seen[e.Name] = true
					result = append(result, e)
				}
			}
			// Also need to check VFS's source for more entries
			if vfs.sourceFolder != "" {
				stack = append(stack, sourceRequest{vfs.sourceFolder, req.relativePath})
				continue
			}
		} else {
			// Real filesystem
			fullPath := filepath.Join(req.sourcePath, req.relativePath)
			entries, err := os.ReadDir(fullPath)
			if err == nil {
				for _, e := range entries {
					if !seen[e.Name()] {
						seen[e.Name()] = true
						info, _ := e.Info()
						var size int64
						var mode os.FileMode = 0644
						if info != nil {
							size = info.Size()
							mode = info.Mode()
						}
						result = append(result, SourceDirEntry{
							Name:  e.Name(),
							IsDir: e.IsDir(),
							Size:  size,
							Mode:  mode,
						})
					}
				}
			}
		}
	}

	return result, nil
}

// Lookup iteratively traverses the chain to lookup file attributes
func (r *SourceResolver) Lookup(sourcePath, relativePath string) (*vfs.Attributes, error) {
	stack := []sourceRequest{{sourcePath, relativePath}}

	for len(stack) > 0 {
		req := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		log.Debugf("[SourceResolver] Lookup: checking source=%q path=%q", req.sourcePath, req.relativePath)

		if vfsFS := r.metaFS.GetVFSBySymlinkTarget(req.sourcePath); vfsFS != nil {
			if attrs, found := vfsFS.tryLocalLookup(req.relativePath); found {
				return attrs, nil
			}
			if vfsFS.sourceFolder != "" {
				stack = append(stack, sourceRequest{vfsFS.sourceFolder, req.relativePath})
				continue
			}
			return nil, ENOENT
		}

		// Real filesystem
		fullPath := filepath.Join(req.sourcePath, req.relativePath)
		info, err := os.Stat(fullPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, ENOENT
			}
			return nil, EIO
		}
		return osFileInfoToAttributes(info, req.relativePath), nil
	}

	return nil, ENOENT
}

// Exists checks if a file/directory exists in the source chain
func (r *SourceResolver) Exists(sourcePath, relativePath string) bool {
	_, _, err := r.Stat(sourcePath, relativePath)
	return err == nil
}

// IsDir checks if a path is a directory in the source chain
func (r *SourceResolver) IsDir(sourcePath, relativePath string) bool {
	_, isDir, err := r.Stat(sourcePath, relativePath)
	return err == nil && isDir
}

// --- Helper methods for LatentFS (local-only, no recursion) ---

// tryLocalRead checks ONLY local data file, returns (content, found)
// Called by SourceResolver during stack-based iteration
func (fs *LatentFS) tryLocalRead(path string) ([]byte, bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	ino, err := fs.dataFile.ResolvePath(path)
	if err != nil {
		return nil, false // Not in local data
	}

	// Check for tombstone
	inode, err := fs.dataFile.GetInode(ino)
	if err != nil {
		return nil, false
	}
	if inode.IsDeleted() {
		return nil, false // Tombstone - treat as not found locally
	}

	// Read content
	if inode.Size == 0 {
		return []byte{}, true
	}
	content, err := fs.dataFile.ReadContent(ino, 0, int(inode.Size))
	if err != nil {
		return nil, false
	}
	return content, true
}

// tryLocalStat checks ONLY local data file, returns (size, isDir, found)
func (fs *LatentFS) tryLocalStat(path string) (int64, bool, bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	ino, err := fs.dataFile.ResolvePath(path)
	if err != nil {
		return 0, false, false
	}
	inode, err := fs.dataFile.GetInode(ino)
	if err != nil {
		return 0, false, false
	}
	if inode.IsDeleted() {
		return 0, false, false // Tombstone
	}
	isDir := (inode.Mode & storage.ModeMask) == storage.ModeDir
	return inode.Size, isDir, true
}

// tryLocalFileInfo checks ONLY local data file, returns (os.FileInfo, found)
// Includes mtime which is critical for NFS cache invalidation
func (fs *LatentFS) tryLocalFileInfo(path string) (os.FileInfo, bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	ino, err := fs.dataFile.ResolvePath(path)
	if err != nil {
		return nil, false
	}
	inode, err := fs.dataFile.GetInode(ino)
	if err != nil {
		return nil, false
	}
	if inode.IsDeleted() {
		return nil, false // Tombstone
	}
	isDir := (inode.Mode & storage.ModeMask) == storage.ModeDir
	mode := os.FileMode(inode.Mode & 0777)
	if isDir {
		mode |= os.ModeDir
	}
	return &syntheticFileInfo{
		name:    filepath.Base(path),
		size:    inode.Size,
		isDir:   isDir,
		mode:    mode,
		modTime: inode.Mtime,
	}, true
}

// tryLocalLookup checks ONLY local data file, returns (attrs, found)
func (fs *LatentFS) tryLocalLookup(path string) (*vfs.Attributes, bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	ino, err := fs.dataFile.ResolvePath(path)
	if err != nil {
		return nil, false
	}
	inode, err := fs.dataFile.GetInode(ino)
	if err != nil {
		return nil, false
	}
	if inode.IsDeleted() {
		return nil, false // Tombstone
	}
	return inodeToAttributes(inode), true
}

// getLocalDirEntries returns entries ONLY from local data file
// Also returns deleted entries in seen map for tombstone tracking
func (fs *LatentFS) getLocalDirEntries(path string) []SourceDirEntry {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	ino, err := fs.dataFile.ResolvePath(path)
	if err != nil {
		return nil
	}

	// Check if it's a directory
	inode, err := fs.dataFile.GetInode(ino)
	if err != nil || inode.IsDeleted() || !inode.IsDir() {
		return nil
	}

	dentries, err := fs.dataFile.ListDir(ino)
	if err != nil {
		return nil
	}

	var result []SourceDirEntry
	for _, d := range dentries {
		// Skip tombstones from listing (but they're tracked by caller via seen map)
		if d.Mode == storage.ModeDeleted {
			continue
		}
		result = append(result, SourceDirEntry{
			Name:  d.Name,
			IsDir: (d.Mode & storage.ModeMask) == storage.ModeDir,
			Size:  d.Size,
			Mode:  os.FileMode(d.Mode & 0777),
		})
	}
	return result
}
