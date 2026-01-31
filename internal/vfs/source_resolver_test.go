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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"latentfs/internal/storage"
)

// --- Cascade Hidden Path Tests ---

// TestCollectMountsInChain tests that mount collection traverses the cascade chain correctly
func TestCollectMountsInChain(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "collect_mounts_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create source directory
	sourceDir := filepath.Join(tmpDir, "source")
	require.NoError(t, os.MkdirAll(sourceDir, 0755))

	// Create meta file and MetaFS
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	require.NoError(t, err)
	defer metaFile.Close()
	metaFS := NewMetaFS(metaFile)

	// Create fork0: source = real directory
	fork0DataPath := filepath.Join(tmpDir, "fork0.latentfs")
	fork0DataFile, err := storage.Create(fork0DataPath)
	require.NoError(t, err)
	defer fork0DataFile.Close()
	fork0Target := filepath.Join(tmpDir, "fork0")
	require.NoError(t, metaFS.AddForkMount("fork0_uuid", fork0DataFile, sourceDir, storage.ForkTypeSoft, fork0Target))

	// Create fork1: source = fork0 (cascaded)
	fork1DataPath := filepath.Join(tmpDir, "fork1.latentfs")
	fork1DataFile, err := storage.Create(fork1DataPath)
	require.NoError(t, err)
	defer fork1DataFile.Close()
	fork1Target := filepath.Join(tmpDir, "fork1")
	require.NoError(t, metaFS.AddForkMount("fork1_uuid", fork1DataFile, fork0Target, storage.ForkTypeSoft, fork1Target))

	// Create fork2: source = fork1 (cascaded)
	fork2DataPath := filepath.Join(tmpDir, "fork2.latentfs")
	fork2DataFile, err := storage.Create(fork2DataPath)
	require.NoError(t, err)
	defer fork2DataFile.Close()
	fork2Target := filepath.Join(tmpDir, "fork2")
	require.NoError(t, metaFS.AddForkMount("fork2_uuid", fork2DataFile, fork1Target, storage.ForkTypeSoft, fork2Target))

	t.Run("single fork chain", func(t *testing.T) {
		mounts := metaFS.resolver.collectMountsInChain(fork0Target)
		require.Len(t, mounts, 1)
		assert.Equal(t, fork0Target, mounts[0].symlinkTarget)
		assert.Equal(t, sourceDir, mounts[0].sourceFolder)
	})

	t.Run("two level cascade", func(t *testing.T) {
		mounts := metaFS.resolver.collectMountsInChain(fork1Target)
		require.Len(t, mounts, 2)
		assert.Equal(t, fork1Target, mounts[0].symlinkTarget)
		assert.Equal(t, fork0Target, mounts[1].symlinkTarget)
	})

	t.Run("three level cascade", func(t *testing.T) {
		mounts := metaFS.resolver.collectMountsInChain(fork2Target)
		require.Len(t, mounts, 3)
		assert.Equal(t, fork2Target, mounts[0].symlinkTarget)
		assert.Equal(t, fork1Target, mounts[1].symlinkTarget)
		assert.Equal(t, fork0Target, mounts[2].symlinkTarget)
	})

	t.Run("real filesystem returns empty", func(t *testing.T) {
		mounts := metaFS.resolver.collectMountsInChain(sourceDir)
		assert.Empty(t, mounts)
	})
}

// TestComputeHiddenPathsForSource tests hidden path computation for various mount/source combinations
func TestComputeHiddenPathsForSource(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compute_hidden_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create meta file and MetaFS
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	require.NoError(t, err)
	defer metaFile.Close()
	metaFS := NewMetaFS(metaFile)

	sourceDir := filepath.Join(tmpDir, "source")

	t.Run("mount inside source", func(t *testing.T) {
		mounts := []mountInChain{
			{symlinkTarget: filepath.Join(sourceDir, "a", "b"), sourceFolder: sourceDir},
		}
		hidden := metaFS.resolver.computeHiddenPathsForSource(sourceDir, mounts)
		require.Len(t, hidden, 1)
		assert.Equal(t, "a/b", hidden[0])
	})

	t.Run("mount outside source", func(t *testing.T) {
		otherDir := filepath.Join(tmpDir, "other")
		mounts := []mountInChain{
			{symlinkTarget: filepath.Join(otherDir, "x"), sourceFolder: sourceDir},
		}
		hidden := metaFS.resolver.computeHiddenPathsForSource(sourceDir, mounts)
		assert.Empty(t, hidden)
	})

	t.Run("multiple mounts inside source", func(t *testing.T) {
		mounts := []mountInChain{
			{symlinkTarget: filepath.Join(sourceDir, "c"), sourceFolder: filepath.Join(sourceDir, "a", "b")},
			{symlinkTarget: filepath.Join(sourceDir, "a", "b"), sourceFolder: sourceDir},
		}
		hidden := metaFS.resolver.computeHiddenPathsForSource(sourceDir, mounts)
		require.Len(t, hidden, 2)
		// Should contain both "c" and "a/b"
		hiddenSet := make(map[string]bool)
		for _, h := range hidden {
			hiddenSet[h] = true
		}
		assert.True(t, hiddenSet["c"], "Should hide 'c'")
		assert.True(t, hiddenSet["a/b"], "Should hide 'a/b'")
	})

	t.Run("nested path mount", func(t *testing.T) {
		mounts := []mountInChain{
			{symlinkTarget: filepath.Join(sourceDir, "a", "b", "c", "d"), sourceFolder: sourceDir},
		}
		hidden := metaFS.resolver.computeHiddenPathsForSource(sourceDir, mounts)
		require.Len(t, hidden, 1)
		assert.Equal(t, "a/b/c/d", hidden[0])
	})
}

// TestIsEntryHiddenAtLevel tests entry filtering logic at various directory levels
func TestIsEntryHiddenAtLevel(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "entry_hidden_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create meta file and MetaFS
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	require.NoError(t, err)
	defer metaFile.Close()
	metaFS := NewMetaFS(metaFile)

	t.Run("exact match at root", func(t *testing.T) {
		hiddenSet := map[string]bool{"c": true}
		assert.True(t, metaFS.resolver.isEntryHiddenAtLevel("/", "c", hiddenSet))
		assert.True(t, metaFS.resolver.isEntryHiddenAtLevel("", "c", hiddenSet))
	})

	t.Run("nested path exact match", func(t *testing.T) {
		hiddenSet := map[string]bool{"a/b": true}
		assert.True(t, metaFS.resolver.isEntryHiddenAtLevel("/a", "b", hiddenSet))
		assert.True(t, metaFS.resolver.isEntryHiddenAtLevel("a", "b", hiddenSet))
	})

	t.Run("parent of hidden path not hidden", func(t *testing.T) {
		hiddenSet := map[string]bool{"a/b": true}
		// "a" itself is not hidden, only "a/b"
		assert.False(t, metaFS.resolver.isEntryHiddenAtLevel("/", "a", hiddenSet))
	})

	t.Run("unrelated entry not hidden", func(t *testing.T) {
		hiddenSet := map[string]bool{"c": true, "a/b": true}
		assert.False(t, metaFS.resolver.isEntryHiddenAtLevel("/", "file.txt", hiddenSet))
		assert.False(t, metaFS.resolver.isEntryHiddenAtLevel("/", "other", hiddenSet))
	})
}

// TestListDirWithHidingCascade tests directory listing with cascade-aware hiding
// This is the key test for the cascade symlink hiding scenario
func TestListDirWithHidingCascade(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "listdir_hiding_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create source directory structure:
	// source/
	// ├── a/
	// │   └── (will be fork1's mount at a/b)
	// ├── (will be fork2's mount at c)
	// └── file.txt
	sourceDir := filepath.Join(tmpDir, "source")
	require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "a"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "file.txt"), []byte("content"), 0644))

	// Create meta file and MetaFS
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	require.NoError(t, err)
	defer metaFile.Close()
	metaFS := NewMetaFS(metaFile)

	// Fork1: source=/source, mount=/source/a/b
	fork1DataPath := filepath.Join(tmpDir, "fork1.latentfs")
	fork1DataFile, err := storage.Create(fork1DataPath)
	require.NoError(t, err)
	defer fork1DataFile.Close()
	fork1Target := filepath.Join(sourceDir, "a", "b")
	require.NoError(t, metaFS.AddForkMount("fork1_uuid", fork1DataFile, sourceDir, storage.ForkTypeSoft, fork1Target))

	// Fork2: source=/source/a/b (fork1), mount=/source/c
	fork2DataPath := filepath.Join(tmpDir, "fork2.latentfs")
	fork2DataFile, err := storage.Create(fork2DataPath)
	require.NoError(t, err)
	defer fork2DataFile.Close()
	fork2Target := filepath.Join(sourceDir, "c")
	require.NoError(t, metaFS.AddForkMount("fork2_uuid", fork2DataFile, fork1Target, storage.ForkTypeSoft, fork2Target))

	// Get fork2's VFS
	fork2VFS, err := metaFS.GetSubFS("fork2_uuid")
	require.NoError(t, err)

	t.Run("cascade hides mount at root source", func(t *testing.T) {
		// List from Fork2's source (which is Fork1) with Fork2 as initiator
		entries, err := metaFS.resolver.ListDirWithHiding(fork2VFS.sourceFolder, "/", fork2Target)
		require.NoError(t, err)

		entryNames := make(map[string]bool)
		for _, e := range entries {
			entryNames[e.Name] = true
		}

		// Should see "a" (parent of hidden path)
		assert.True(t, entryNames["a"], "Should see 'a' directory")
		// Should see "file.txt"
		assert.True(t, entryNames["file.txt"], "Should see 'file.txt'")
		// Should NOT see "c" (Fork2's own mount hidden at root source level)
		assert.False(t, entryNames["c"], "Should NOT see 'c' (Fork2's mount)")
	})

	t.Run("nested path hidden in subdirectory", func(t *testing.T) {
		// List "a" directory - should not see "b" (Fork1's mount)
		entries, err := metaFS.resolver.ListDirWithHiding(fork2VFS.sourceFolder, "/a", fork2Target)
		require.NoError(t, err)

		entryNames := make(map[string]bool)
		for _, e := range entries {
			entryNames[e.Name] = true
		}

		// Should NOT see "b" (Fork1's mount is at a/b)
		assert.False(t, entryNames["b"], "Should NOT see 'b' (Fork1's mount at a/b)")
	})
}

// TestLookupWithHidingCascade tests lookup with cascade-aware hidden path checking
func TestLookupWithHidingCascade(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lookup_hiding_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create source directory
	sourceDir := filepath.Join(tmpDir, "source")
	require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "a"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "file.txt"), []byte("content"), 0644))

	// Create directories that will become mount points
	require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "a", "b"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "c"), 0755))

	// Create meta file and MetaFS
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	require.NoError(t, err)
	defer metaFile.Close()
	metaFS := NewMetaFS(metaFile)

	// Fork1: source=/source, mount=/source/a/b
	fork1DataPath := filepath.Join(tmpDir, "fork1.latentfs")
	fork1DataFile, err := storage.Create(fork1DataPath)
	require.NoError(t, err)
	defer fork1DataFile.Close()
	fork1Target := filepath.Join(sourceDir, "a", "b")
	require.NoError(t, metaFS.AddForkMount("fork1_uuid", fork1DataFile, sourceDir, storage.ForkTypeSoft, fork1Target))

	// Fork2: source=/source/a/b (fork1), mount=/source/c
	fork2DataPath := filepath.Join(tmpDir, "fork2.latentfs")
	fork2DataFile, err := storage.Create(fork2DataPath)
	require.NoError(t, err)
	defer fork2DataFile.Close()
	fork2Target := filepath.Join(sourceDir, "c")
	require.NoError(t, metaFS.AddForkMount("fork2_uuid", fork2DataFile, fork1Target, storage.ForkTypeSoft, fork2Target))

	// Get fork2's VFS
	fork2VFS, err := metaFS.GetSubFS("fork2_uuid")
	require.NoError(t, err)

	t.Run("lookup non-hidden file succeeds", func(t *testing.T) {
		attrs, err := metaFS.resolver.LookupWithHiding(fork2VFS.sourceFolder, "/file.txt", fork2Target)
		require.NoError(t, err)
		assert.NotNil(t, attrs)
	})

	t.Run("lookup hidden path returns ENOENT", func(t *testing.T) {
		// Looking up "c" should fail (Fork2's mount is hidden)
		_, err := metaFS.resolver.LookupWithHiding(fork2VFS.sourceFolder, "/c", fork2Target)
		assert.Equal(t, ENOENT, err)
	})

	t.Run("lookup parent of hidden path succeeds", func(t *testing.T) {
		// Looking up "a" should succeed (only "a/b" is hidden)
		attrs, err := metaFS.resolver.LookupWithHiding(fork2VFS.sourceFolder, "/a", fork2Target)
		require.NoError(t, err)
		assert.NotNil(t, attrs)
	})

	t.Run("lookup nested hidden path returns ENOENT", func(t *testing.T) {
		// Looking up "a/b" should fail (Fork1's mount)
		_, err := metaFS.resolver.LookupWithHiding(fork2VFS.sourceFolder, "/a/b", fork2Target)
		assert.Equal(t, ENOENT, err)
	})
}

// TestSiblingForksIsolated tests that sibling forks don't affect each other's hiding
func TestSiblingForksIsolated(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sibling_forks_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create source directory with directories that will become mount points
	// In real usage, these would be symlinks created by the fork command
	sourceDir := filepath.Join(tmpDir, "source")
	require.NoError(t, os.MkdirAll(sourceDir, 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "a"), 0755)) // Will be Fork1's mount
	require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "b"), 0755)) // Will be Fork2's mount
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "file.txt"), []byte("content"), 0644))

	// Create meta file and MetaFS
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	require.NoError(t, err)
	defer metaFile.Close()
	metaFS := NewMetaFS(metaFile)

	// Fork1: source=/source, mount=/source/a
	fork1DataPath := filepath.Join(tmpDir, "fork1.latentfs")
	fork1DataFile, err := storage.Create(fork1DataPath)
	require.NoError(t, err)
	defer fork1DataFile.Close()
	fork1Target := filepath.Join(sourceDir, "a")
	require.NoError(t, metaFS.AddForkMount("fork1_uuid", fork1DataFile, sourceDir, storage.ForkTypeSoft, fork1Target))

	// Fork2: source=/source (same source), mount=/source/b (sibling, not cascaded)
	fork2DataPath := filepath.Join(tmpDir, "fork2.latentfs")
	fork2DataFile, err := storage.Create(fork2DataPath)
	require.NoError(t, err)
	defer fork2DataFile.Close()
	fork2Target := filepath.Join(sourceDir, "b")
	require.NoError(t, metaFS.AddForkMount("fork2_uuid", fork2DataFile, sourceDir, storage.ForkTypeSoft, fork2Target))

	t.Run("fork1 hides a not b", func(t *testing.T) {
		// From Fork1, should hide "a", see "b"
		entries, err := metaFS.resolver.ListDirWithHiding(sourceDir, "/", fork1Target)
		require.NoError(t, err)

		entryNames := make(map[string]bool)
		for _, e := range entries {
			entryNames[e.Name] = true
		}

		assert.False(t, entryNames["a"], "Fork1 should hide 'a'")
		assert.True(t, entryNames["b"], "Fork1 should see 'b'")
		assert.True(t, entryNames["file.txt"], "Fork1 should see 'file.txt'")
	})

	t.Run("fork2 hides b not a", func(t *testing.T) {
		// From Fork2, should hide "b", see "a"
		entries, err := metaFS.resolver.ListDirWithHiding(sourceDir, "/", fork2Target)
		require.NoError(t, err)

		entryNames := make(map[string]bool)
		for _, e := range entries {
			entryNames[e.Name] = true
		}

		assert.True(t, entryNames["a"], "Fork2 should see 'a'")
		assert.False(t, entryNames["b"], "Fork2 should hide 'b'")
		assert.True(t, entryNames["file.txt"], "Fork2 should see 'file.txt'")
	})
}

// --- Original Tests ---

// TestSourceResolverPathMatching verifies that GetVFSBySymlinkTarget matches paths correctly
func TestSourceResolverPathMatching(t *testing.T) {
	// Create temp directory for test
	tmpDir, err := os.MkdirTemp("", "resolver_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a source directory with a file
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("Failed to create source dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "test.txt"), []byte("hello"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create meta file
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	if err != nil {
		t.Fatalf("Failed to create meta file: %v", err)
	}
	defer metaFile.Close()

	// Create MetaFS with resolver
	metaFS := NewMetaFS(metaFile)

	// Verify resolver exists
	if metaFS.resolver == nil {
		t.Fatal("MetaFS resolver is nil")
	}

	// Create data file for fork0
	fork0DataPath := filepath.Join(tmpDir, "fork0.latentfs")
	fork0DataFile, err := storage.Create(fork0DataPath)
	if err != nil {
		t.Fatalf("Failed to create fork0 data file: %v", err)
	}
	defer fork0DataFile.Close()

	fork0Target := filepath.Join(tmpDir, "fork0")

	// Add fork0 mount (source = real directory, symlinkTarget = fork0 path)
	if err := metaFS.AddForkMount("fork0_uuid", fork0DataFile, sourceDir, storage.ForkTypeSoft, fork0Target); err != nil {
		t.Fatalf("Failed to add fork0 mount: %v", err)
	}

	// Verify fork0 is in subFS
	fork0VFS, err := metaFS.GetSubFS("fork0_uuid")
	if err != nil {
		t.Fatalf("Failed to get fork0 subFS: %v", err)
	}
	if fork0VFS.symlinkTarget != fork0Target {
		t.Errorf("fork0 symlinkTarget = %q, want %q", fork0VFS.symlinkTarget, fork0Target)
	}
	if fork0VFS.sourceFolder != sourceDir {
		t.Errorf("fork0 sourceFolder = %q, want %q", fork0VFS.sourceFolder, sourceDir)
	}

	// Test GetVFSBySymlinkTarget
	foundVFS := metaFS.GetVFSBySymlinkTarget(fork0Target)
	if foundVFS == nil {
		t.Errorf("GetVFSBySymlinkTarget(%q) returned nil, expected fork0 VFS", fork0Target)
	}
	if foundVFS != fork0VFS {
		t.Errorf("GetVFSBySymlinkTarget returned different VFS instance")
	}

	// Test with non-existent path
	notFoundVFS := metaFS.GetVFSBySymlinkTarget("/nonexistent/path")
	if notFoundVFS != nil {
		t.Errorf("GetVFSBySymlinkTarget for non-existent path should return nil")
	}

	t.Log("Path matching test passed!")
}

// TestSourceResolverCascadedLookup tests that the resolver correctly traverses cascaded forks
func TestSourceResolverCascadedLookup(t *testing.T) {
	// Create temp directory for test
	tmpDir, err := os.MkdirTemp("", "resolver_cascade_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a source directory with a file
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("Failed to create source dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "source.txt"), []byte("source content"), 0644); err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}

	// Create meta file
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	if err != nil {
		t.Fatalf("Failed to create meta file: %v", err)
	}
	defer metaFile.Close()

	// Create MetaFS with resolver
	metaFS := NewMetaFS(metaFile)

	fork0Target := filepath.Join(tmpDir, "fork0")
	fork1Target := filepath.Join(tmpDir, "fork1")

	// Create fork0: source = real source directory
	fork0DataPath := filepath.Join(tmpDir, "fork0.latentfs")
	fork0DataFile, err := storage.Create(fork0DataPath)
	if err != nil {
		t.Fatalf("Failed to create fork0 data file: %v", err)
	}
	defer fork0DataFile.Close()

	if err := metaFS.AddForkMount("fork0_uuid", fork0DataFile, sourceDir, storage.ForkTypeSoft, fork0Target); err != nil {
		t.Fatalf("Failed to add fork0 mount: %v", err)
	}

	// Create fork1: source = fork0 (cascaded)
	fork1DataPath := filepath.Join(tmpDir, "fork1.latentfs")
	fork1DataFile, err := storage.Create(fork1DataPath)
	if err != nil {
		t.Fatalf("Failed to create fork1 data file: %v", err)
	}
	defer fork1DataFile.Close()

	// CRITICAL: fork1's source is fork0's symlink target path
	// This simulates how cascaded forks are created: fork1's source folder is set to fork0's target
	if err := metaFS.AddForkMount("fork1_uuid", fork1DataFile, fork0Target, storage.ForkTypeSoft, fork1Target); err != nil {
		t.Fatalf("Failed to add fork1 mount: %v", err)
	}

	// Get fork1's VFS
	fork1VFS, err := metaFS.GetSubFS("fork1_uuid")
	if err != nil {
		t.Fatalf("Failed to get fork1 subFS: %v", err)
	}

	// Verify fork1's configuration
	t.Logf("fork1.sourceFolder = %q", fork1VFS.sourceFolder)
	t.Logf("fork1.symlinkTarget = %q", fork1VFS.symlinkTarget)

	// The key test: can fork1's resolver find fork0 by its symlink target?
	t.Logf("Looking up VFS for path %q (fork0's target)", fork0Target)
	foundVFS := metaFS.GetVFSBySymlinkTarget(fork0Target)
	if foundVFS == nil {
		// This is the bug! The resolver can't find fork0
		t.Fatalf("CRITICAL: GetVFSBySymlinkTarget(%q) returned nil - this causes NFS deadlock!", fork0Target)
	}
	t.Logf("Found VFS: symlinkTarget=%q sourceFolder=%q", foundVFS.symlinkTarget, foundVFS.sourceFolder)

	// Now test the resolver's Stat method
	// fork1.sourceFolder = fork0Target
	// Stat should find source.txt by:
	// 1. GetVFSBySymlinkTarget(fork0Target) -> returns fork0 VFS
	// 2. fork0.tryLocalStat("source.txt") -> not found (fork0 has no local data)
	// 3. fork0.sourceFolder = sourceDir (real directory)
	// 4. Push sourceDir onto stack
	// 5. GetVFSBySymlinkTarget(sourceDir) -> nil (not a VFS)
	// 6. os.Stat(sourceDir/source.txt) -> success!
	size, isDir, err := metaFS.resolver.Stat(fork1VFS.sourceFolder, "/source.txt")
	if err != nil {
		t.Fatalf("Resolver.Stat failed: %v", err)
	}
	if isDir {
		t.Error("source.txt should not be a directory")
	}
	if size != 14 { // "source content" is 14 bytes
		t.Errorf("size = %d, want 14", size)
	}

	t.Log("Cascaded lookup test passed!")
}

// TestSourceResolverReadFile tests reading files through cascaded forks
func TestSourceResolverReadFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "resolver_read_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create source directory with files
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("Failed to create source dir: %v", err)
	}
	testContent := "hello from source"
	if err := os.WriteFile(filepath.Join(sourceDir, "test.txt"), []byte(testContent), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create meta file and MetaFS
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	if err != nil {
		t.Fatalf("Failed to create meta file: %v", err)
	}
	defer metaFile.Close()
	metaFS := NewMetaFS(metaFile)

	// Create fork0: source = real directory
	fork0DataPath := filepath.Join(tmpDir, "fork0.latentfs")
	fork0DataFile, err := storage.Create(fork0DataPath)
	if err != nil {
		t.Fatalf("Failed to create fork0 data file: %v", err)
	}
	defer fork0DataFile.Close()
	fork0Target := filepath.Join(tmpDir, "fork0")
	if err := metaFS.AddForkMount("fork0_uuid", fork0DataFile, sourceDir, storage.ForkTypeSoft, fork0Target); err != nil {
		t.Fatalf("Failed to add fork0 mount: %v", err)
	}

	// Create fork1: source = fork0 (cascaded)
	fork1DataPath := filepath.Join(tmpDir, "fork1.latentfs")
	fork1DataFile, err := storage.Create(fork1DataPath)
	if err != nil {
		t.Fatalf("Failed to create fork1 data file: %v", err)
	}
	defer fork1DataFile.Close()
	fork1Target := filepath.Join(tmpDir, "fork1")
	if err := metaFS.AddForkMount("fork1_uuid", fork1DataFile, fork0Target, storage.ForkTypeSoft, fork1Target); err != nil {
		t.Fatalf("Failed to add fork1 mount: %v", err)
	}

	// Get fork1's VFS
	fork1VFS, err := metaFS.GetSubFS("fork1_uuid")
	if err != nil {
		t.Fatalf("Failed to get fork1 subFS: %v", err)
	}

	// Test ReadFile through cascade: fork1 -> fork0 -> source
	content, err := metaFS.resolver.ReadFile(fork1VFS.sourceFolder, "/test.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(content) != testContent {
		t.Errorf("content = %q, want %q", string(content), testContent)
	}

	// Test reading non-existent file
	_, err = metaFS.resolver.ReadFile(fork1VFS.sourceFolder, "/nonexistent.txt")
	if err == nil {
		t.Error("ReadFile for non-existent file should return error")
	}

	t.Log("ReadFile test passed!")
}

// TestSourceResolverListDir tests directory listing through cascaded forks
func TestSourceResolverListDir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "resolver_listdir_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create source directory with files and subdirectories
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(filepath.Join(sourceDir, "subdir"), 0755); err != nil {
		t.Fatalf("Failed to create source dirs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to create file1: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatalf("Failed to create file2: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "subdir", "nested.txt"), []byte("nested"), 0644); err != nil {
		t.Fatalf("Failed to create nested file: %v", err)
	}

	// Create meta file and MetaFS
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	if err != nil {
		t.Fatalf("Failed to create meta file: %v", err)
	}
	defer metaFile.Close()
	metaFS := NewMetaFS(metaFile)

	// Create fork0: source = real directory
	fork0DataPath := filepath.Join(tmpDir, "fork0.latentfs")
	fork0DataFile, err := storage.Create(fork0DataPath)
	if err != nil {
		t.Fatalf("Failed to create fork0 data file: %v", err)
	}
	defer fork0DataFile.Close()
	fork0Target := filepath.Join(tmpDir, "fork0")
	if err := metaFS.AddForkMount("fork0_uuid", fork0DataFile, sourceDir, storage.ForkTypeSoft, fork0Target); err != nil {
		t.Fatalf("Failed to add fork0 mount: %v", err)
	}

	// Create fork1: source = fork0 (cascaded)
	fork1DataPath := filepath.Join(tmpDir, "fork1.latentfs")
	fork1DataFile, err := storage.Create(fork1DataPath)
	if err != nil {
		t.Fatalf("Failed to create fork1 data file: %v", err)
	}
	defer fork1DataFile.Close()
	fork1Target := filepath.Join(tmpDir, "fork1")
	if err := metaFS.AddForkMount("fork1_uuid", fork1DataFile, fork0Target, storage.ForkTypeSoft, fork1Target); err != nil {
		t.Fatalf("Failed to add fork1 mount: %v", err)
	}

	// Get fork1's VFS
	fork1VFS, err := metaFS.GetSubFS("fork1_uuid")
	if err != nil {
		t.Fatalf("Failed to get fork1 subFS: %v", err)
	}

	// Test ListDir at root
	entries, err := metaFS.resolver.ListDir(fork1VFS.sourceFolder, "/")
	if err != nil {
		t.Fatalf("ListDir failed: %v", err)
	}

	// Verify entries
	entryMap := make(map[string]bool)
	for _, e := range entries {
		entryMap[e.Name] = e.IsDir
	}

	if !entryMap["file1.txt"] && entryMap["file1.txt"] {
		t.Error("file1.txt should be a file")
	}
	if _, ok := entryMap["file1.txt"]; !ok {
		t.Error("file1.txt not found in listing")
	}
	if _, ok := entryMap["file2.txt"]; !ok {
		t.Error("file2.txt not found in listing")
	}
	if isDir, ok := entryMap["subdir"]; !ok || !isDir {
		t.Error("subdir not found or not marked as directory")
	}

	// Test ListDir on subdirectory
	subEntries, err := metaFS.resolver.ListDir(fork1VFS.sourceFolder, "/subdir")
	if err != nil {
		t.Fatalf("ListDir on subdir failed: %v", err)
	}
	found := false
	for _, e := range subEntries {
		if e.Name == "nested.txt" {
			found = true
			break
		}
	}
	if !found {
		t.Error("nested.txt not found in subdir listing")
	}

	t.Log("ListDir test passed!")
}

// TestSourceResolverLocalShadowing tests that local data shadows source data
func TestSourceResolverLocalShadowing(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "resolver_shadow_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create source directory with a file
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("Failed to create source dir: %v", err)
	}
	sourceContent := "source version"
	if err := os.WriteFile(filepath.Join(sourceDir, "test.txt"), []byte(sourceContent), 0644); err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}

	// Create meta file and MetaFS
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	if err != nil {
		t.Fatalf("Failed to create meta file: %v", err)
	}
	defer metaFile.Close()
	metaFS := NewMetaFS(metaFile)

	// Create fork0: source = real directory
	fork0DataPath := filepath.Join(tmpDir, "fork0.latentfs")
	fork0DataFile, err := storage.Create(fork0DataPath)
	if err != nil {
		t.Fatalf("Failed to create fork0 data file: %v", err)
	}
	defer fork0DataFile.Close()
	fork0Target := filepath.Join(tmpDir, "fork0")
	if err := metaFS.AddForkMount("fork0_uuid", fork0DataFile, sourceDir, storage.ForkTypeSoft, fork0Target); err != nil {
		t.Fatalf("Failed to add fork0 mount: %v", err)
	}

	// Verify fork0 was mounted (we don't need to use the VFS directly)
	if _, err := metaFS.GetSubFS("fork0_uuid"); err != nil {
		t.Fatalf("Failed to get fork0 subFS: %v", err)
	}

	// Write a local version of the file in fork0
	localContent := "local version"
	ino, err := fork0DataFile.CreateInode(uint32(storage.ModeFile | 0644))
	if err != nil {
		t.Fatalf("Failed to create inode: %v", err)
	}
	if err := fork0DataFile.CreateDentry(storage.RootIno, "test.txt", ino); err != nil {
		t.Fatalf("Failed to create dentry: %v", err)
	}
	if err := fork0DataFile.WriteContent(ino, 0, []byte(localContent)); err != nil {
		t.Fatalf("Failed to write content: %v", err)
	}

	// Create fork1: source = fork0
	fork1DataPath := filepath.Join(tmpDir, "fork1.latentfs")
	fork1DataFile, err := storage.Create(fork1DataPath)
	if err != nil {
		t.Fatalf("Failed to create fork1 data file: %v", err)
	}
	defer fork1DataFile.Close()
	fork1Target := filepath.Join(tmpDir, "fork1")
	if err := metaFS.AddForkMount("fork1_uuid", fork1DataFile, fork0Target, storage.ForkTypeSoft, fork1Target); err != nil {
		t.Fatalf("Failed to add fork1 mount: %v", err)
	}

	// Get fork1's VFS
	fork1VFS, err := metaFS.GetSubFS("fork1_uuid")
	if err != nil {
		t.Fatalf("Failed to get fork1 subFS: %v", err)
	}

	// Read through fork1 - should get fork0's local version, not source
	content, err := metaFS.resolver.ReadFile(fork1VFS.sourceFolder, "/test.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(content) != localContent {
		t.Errorf("content = %q, want %q (local should shadow source)", string(content), localContent)
	}

	t.Log("Local shadowing test passed!")
}

// TestSourceResolverLookup tests the Lookup method
func TestSourceResolverLookup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "resolver_lookup_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create source directory with files
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(filepath.Join(sourceDir, "subdir"), 0755); err != nil {
		t.Fatalf("Failed to create source dirs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "file.txt"), []byte("content"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Create meta file and MetaFS
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	if err != nil {
		t.Fatalf("Failed to create meta file: %v", err)
	}
	defer metaFile.Close()
	metaFS := NewMetaFS(metaFile)

	// Create fork0
	fork0DataPath := filepath.Join(tmpDir, "fork0.latentfs")
	fork0DataFile, err := storage.Create(fork0DataPath)
	if err != nil {
		t.Fatalf("Failed to create fork0 data file: %v", err)
	}
	defer fork0DataFile.Close()
	fork0Target := filepath.Join(tmpDir, "fork0")
	if err := metaFS.AddForkMount("fork0_uuid", fork0DataFile, sourceDir, storage.ForkTypeSoft, fork0Target); err != nil {
		t.Fatalf("Failed to add fork0 mount: %v", err)
	}

	// Test Lookup for file
	attrs, err := metaFS.resolver.Lookup(sourceDir, "/file.txt")
	if err != nil {
		t.Fatalf("Lookup for file failed: %v", err)
	}
	if attrs.GetFileType() != 0 { // FileTypeRegularFile = 0
		t.Errorf("file.txt should be regular file, got type %d", attrs.GetFileType())
	}
	size, _ := attrs.GetSizeBytes()
	if size != 7 { // "content" is 7 bytes
		t.Errorf("size = %d, want 7", size)
	}

	// Test Lookup for directory
	attrs, err = metaFS.resolver.Lookup(sourceDir, "/subdir")
	if err != nil {
		t.Fatalf("Lookup for directory failed: %v", err)
	}
	if attrs.GetFileType() != 1 { // FileTypeDirectory = 1
		t.Errorf("subdir should be directory, got type %d", attrs.GetFileType())
	}

	// Test Lookup for non-existent path
	_, err = metaFS.resolver.Lookup(sourceDir, "/nonexistent")
	if err == nil {
		t.Error("Lookup for non-existent path should return error")
	}

	t.Log("Lookup test passed!")
}

// TestSourceResolverStatWithInfo tests the StatWithInfo method
func TestSourceResolverStatWithInfo(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "resolver_statwithinfo_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create source directory with a file
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("Failed to create source dir: %v", err)
	}
	content := "test content"
	if err := os.WriteFile(filepath.Join(sourceDir, "test.txt"), []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create meta file and MetaFS
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	if err != nil {
		t.Fatalf("Failed to create meta file: %v", err)
	}
	defer metaFile.Close()
	metaFS := NewMetaFS(metaFile)

	// Test StatWithInfo on real filesystem
	info, err := metaFS.resolver.StatWithInfo(sourceDir, "/test.txt")
	if err != nil {
		t.Fatalf("StatWithInfo failed: %v", err)
	}
	if info.Size() != int64(len(content)) {
		t.Errorf("size = %d, want %d", info.Size(), len(content))
	}
	if info.IsDir() {
		t.Error("test.txt should not be a directory")
	}
	if info.Name() != "test.txt" {
		t.Errorf("name = %q, want %q", info.Name(), "test.txt")
	}

	t.Log("StatWithInfo test passed!")
}

// TestSourceResolverExists tests the Exists and IsDir helper methods
func TestSourceResolverExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "resolver_exists_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create source directory with file and subdirectory
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(filepath.Join(sourceDir, "subdir"), 0755); err != nil {
		t.Fatalf("Failed to create source dirs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "file.txt"), []byte("content"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Create meta file and MetaFS
	metaPath := filepath.Join(tmpDir, "meta.latentfs")
	metaFile, err := storage.CreateMeta(metaPath)
	if err != nil {
		t.Fatalf("Failed to create meta file: %v", err)
	}
	defer metaFile.Close()
	metaFS := NewMetaFS(metaFile)

	// Test Exists
	if !metaFS.resolver.Exists(sourceDir, "/file.txt") {
		t.Error("file.txt should exist")
	}
	if !metaFS.resolver.Exists(sourceDir, "/subdir") {
		t.Error("subdir should exist")
	}
	if metaFS.resolver.Exists(sourceDir, "/nonexistent") {
		t.Error("nonexistent should not exist")
	}

	// Test IsDir
	if metaFS.resolver.IsDir(sourceDir, "/file.txt") {
		t.Error("file.txt should not be a directory")
	}
	if !metaFS.resolver.IsDir(sourceDir, "/subdir") {
		t.Error("subdir should be a directory")
	}
	if metaFS.resolver.IsDir(sourceDir, "/nonexistent") {
		t.Error("nonexistent should not be a directory")
	}

	t.Log("Exists/IsDir test passed!")
}
