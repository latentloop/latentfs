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

// Package cache provides cache implementations for LatentFS VFS layer.
//
// Design Principles:
// 1. Fine-grained cache management - Invalidate only affected paths, not entire cache
// 2. Single layer ownership - Each cache lives in one layer (no cross-layer signaling)
//
// Currently provides:
// - AttrCache: TTL-based attribute cache with fine-grained invalidation (used by LatentFS)
//
// Note: DataFile uses inline cache implementations (lookupCache, cachedParentInode)
// rather than generic types for simplicity and performance.
package cache

import "os"

// Disabled controls whether all caching mechanisms are disabled.
// Set via LATENTFS_CACHE=0 environment variable.
// When true:
// - AttrCache.Get() always returns nil (cache miss)
// - AttrCache.Set() is a no-op
// - DataFile.Lookup() bypasses lookupCache
// - DataFile.GetInodeCachedForParent() bypasses parent inode cache
//
// This is useful for testing and debugging to verify logic works correctly
// without caching, and to isolate cache-related bugs.
var Disabled = os.Getenv("LATENTFS_CACHE") == "0"

// Invalidator is implemented by all caches that support full invalidation.
type Invalidator interface {
	// Invalidate clears all entries from the cache.
	Invalidate()
}
