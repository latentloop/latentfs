package cache

import (
	"strings"
	"sync"
	"time"

	"github.com/macos-fuse-t/go-smb2/vfs"
)

// AttrCache caches file/directory attributes with TTL-based expiration.
// Supports fine-grained invalidation by path.
//
// Thread-safe: Uses RWMutex for concurrent access.
type AttrCache struct {
	mu      sync.RWMutex
	entries map[string]*attrEntry
	ttl     time.Duration
	maxSize int
}

type attrEntry struct {
	attrs    *vfs.Attributes
	expires  time.Time
	isSource bool // Source files use TTL; overlay entries can have longer TTL
}

// NewAttrCache creates a new attribute cache.
// ttl: Time-to-live for cached entries (use 0 for no expiration)
// maxSize: Maximum number of entries (use 0 for unlimited)
func NewAttrCache(ttl time.Duration, maxSize int) *AttrCache {
	return &AttrCache{
		entries: make(map[string]*attrEntry, 256),
		ttl:     ttl,
		maxSize: maxSize,
	}
}

// Get retrieves cached attributes for a path.
// Returns nil if not found, expired, or caching is disabled (LATENTFS_CACHE=0).
func (c *AttrCache) Get(path string) *vfs.Attributes {
	if Disabled {
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.entries[path]
	if !ok {
		return nil
	}

	// Check TTL expiration
	if c.ttl > 0 && time.Now().After(entry.expires) {
		return nil
	}

	return entry.attrs
}

// Set stores attributes for a path.
// isSource: true if the attributes are from source folder (subject to TTL)
// No-op if caching is disabled (LATENTFS_CACHE=0).
func (c *AttrCache) Set(path string, attrs *vfs.Attributes, isSource bool) {
	if Disabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check size limit
	if c.maxSize > 0 && len(c.entries) >= c.maxSize {
		// Don't add new entries when at capacity
		// A more sophisticated implementation could use LRU eviction
		if _, exists := c.entries[path]; !exists {
			return
		}
	}

	expires := time.Time{} // No expiration by default
	if c.ttl > 0 {
		expires = time.Now().Add(c.ttl)
	}

	c.entries[path] = &attrEntry{
		attrs:    attrs,
		expires:  expires,
		isSource: isSource,
	}
}

// Invalidate clears all entries from the cache.
func (c *AttrCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) > 0 {
		c.entries = make(map[string]*attrEntry, 256)
	}
}

// InvalidatePath removes a specific path from the cache.
func (c *AttrCache) InvalidatePath(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.entries, path)
}

// InvalidatePrefix removes all paths with the given prefix.
// Useful for invalidating all entries under a directory.
func (c *AttrCache) InvalidatePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Ensure prefix ends with / for directory matching
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	for path := range c.entries {
		if strings.HasPrefix(path, prefix) {
			delete(c.entries, path)
		}
	}
}

// InvalidatePathAndParent invalidates a path and its parent directory.
// Used for create, remove, mkdir, symlink operations.
func (c *AttrCache) InvalidatePathAndParent(path, parentPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.entries, path)
	delete(c.entries, parentPath)
}

// InvalidateRename invalidates paths affected by a rename operation.
// Affects: oldPath, newPath, oldParent, newParent
func (c *AttrCache) InvalidateRename(oldPath, newPath, oldParent, newParent string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.entries, oldPath)
	delete(c.entries, newPath)
	delete(c.entries, oldParent)
	delete(c.entries, newParent)
}

// Size returns the current number of entries in the cache.
func (c *AttrCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// Stats returns cache statistics.
type AttrCacheStats struct {
	Size    int
	MaxSize int
	TTL     time.Duration
}

// Stats returns current cache statistics.
func (c *AttrCache) Stats() AttrCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return AttrCacheStats{
		Size:    len(c.entries),
		MaxSize: c.maxSize,
		TTL:     c.ttl,
	}
}
