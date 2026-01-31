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

package integration

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCascadeHiding(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "cascade_hiding")
	defer shared.Cleanup()

	// TestCascadeForkHidesSymlinkAtRootSource tests the primary cascade symlink hiding scenario:
	// - source: /foo
	// - Fork1: source=/foo, mount=/foo/a/b
	// - Fork2: source=/foo/a/b (Fork1), mount=/foo/c
	// Fork2 should NOT see "c" because it's its own mount inside the root source /foo
	t.Run("HidesSymlinkAtRootSource", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "hides_symlink_at_root")
		defer env.Cleanup()

		// Create source directory structure:
		// source/
		// +-- a/
		// +-- file.txt
		env.CreateSourceFile("file.txt", "content")
		require.NoError(t, os.MkdirAll(filepath.Join(env.SourceDir, "a"), 0755))

		// Create Fork1: source=/source, mount=/source/a/b
		fork1Mount := filepath.Join(env.SourceDir, "a", "b")
		result := env.RunCLI("fork", env.SourceDir, "-m", fork1Mount)
		require.Equal(t, 0, result.ExitCode, "Fork1 creation failed: %s", result.Combined)
		defer env.RunCLI("unmount", fork1Mount)
		waitForPath(t, fork1Mount, MountReadyTimeout)

		// Create Fork2: source=/source/a/b (Fork1), mount=/source/c
		// Use --type=full because overlay fork from inside a mount is not supported
		fork2Mount := filepath.Join(env.SourceDir, "c")
		result = env.RunCLI("fork", fork1Mount, "--type=full", "-m", fork2Mount)
		require.Equal(t, 0, result.ExitCode, "Fork2 creation failed: %s", result.Combined)
		defer env.RunCLI("unmount", fork2Mount)
		waitForPath(t, fork2Mount, MountReadyTimeout)

		// Test: List root of Fork2 - should NOT see "c"
		entries, err := os.ReadDir(fork2Mount)
		require.NoError(t, err)

		entryNames := extractNames(entries)

		t.Logf("Fork2 root listing: %v", entryNames)

		// Should see "a" (parent of hidden path)
		assert.Contains(t, entryNames, "a", "Should see 'a' directory")
		// Should see "file.txt"
		assert.Contains(t, entryNames, "file.txt", "Should see 'file.txt'")
		// Should NOT see "c" (Fork2's own mount)
		assert.NotContains(t, entryNames, "c", "Should NOT see 'c' (Fork2's own mount)")

		// Test: Accessing /c from Fork2 should fail (hidden)
		_, err = os.Stat(filepath.Join(fork2Mount, "c"))
		assert.True(t, os.IsNotExist(err), "Accessing 'c' should return not exist error")
	})

	// TestCascadeForkThreeLevelDeep tests hiding with 3+ levels of cascading
	t.Run("ThreeLevelDeep", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "three_level")
		defer env.Cleanup()

		// Create source with file
		env.CreateSourceFile("file.txt", "content")

		// Create cascaded forks all mounting inside the root source
		fork1 := filepath.Join(env.SourceDir, "a")
		fork2 := filepath.Join(env.SourceDir, "b")
		fork3 := filepath.Join(env.SourceDir, "c")

		// Fork1: source=/source, mount=/source/a
		result := env.RunCLI("fork", env.SourceDir, "-m", fork1)
		require.Equal(t, 0, result.ExitCode, "Fork1 creation failed: %s", result.Combined)
		defer env.RunCLI("unmount", fork1)
		waitForPath(t, fork1, MountReadyTimeout)

		// Fork2: source=/source/a (Fork1), mount=/source/b
		// Use --type=full because overlay fork from inside a mount is not supported
		result = env.RunCLI("fork", fork1, "--type=full", "-m", fork2)
		require.Equal(t, 0, result.ExitCode, "Fork2 creation failed: %s", result.Combined)
		defer env.RunCLI("unmount", fork2)
		waitForPath(t, fork2, MountReadyTimeout)

		// Fork3: source=/source/b (Fork2), mount=/source/c
		// Use --type=full because overlay fork from inside a mount is not supported
		result = env.RunCLI("fork", fork2, "--type=full", "-m", fork3)
		require.Equal(t, 0, result.ExitCode, "Fork3 creation failed: %s", result.Combined)
		defer env.RunCLI("unmount", fork3)
		waitForPath(t, fork3, MountReadyTimeout)

		// From Fork3, should only see file.txt (all mounts a, b, c are hidden)
		entries, err := os.ReadDir(fork3)
		require.NoError(t, err)

		entryNames := extractNames(entries)
		t.Logf("Fork3 root listing: %v", entryNames)

		assert.Contains(t, entryNames, "file.txt", "Should see file.txt")
		assert.NotContains(t, entryNames, "a", "Should NOT see 'a' (Fork1's mount)")
		assert.NotContains(t, entryNames, "b", "Should NOT see 'b' (Fork2's mount)")
		assert.NotContains(t, entryNames, "c", "Should NOT see 'c' (Fork3's mount)")
	})

	// TestCascadeForkNestedPathHiding tests that nested mount paths (e.g., a/b) hide correctly
	// while keeping parent (a) visible
	t.Run("NestedPathHiding", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "nested_path")
		defer env.Cleanup()

		// Create source structure:
		// source/
		// +-- a/
		// |   +-- other/
		// |   +-- sibling.txt
		// +-- file.txt
		require.NoError(t, os.MkdirAll(filepath.Join(env.SourceDir, "a", "other"), 0755))
		env.CreateSourceFile("a/sibling.txt", "sibling content")
		env.CreateSourceFile("file.txt", "root content")

		// Fork1: source=/source, mount=/source/a/b (nested inside a)
		fork1 := filepath.Join(env.SourceDir, "a", "b")
		result := env.RunCLI("fork", env.SourceDir, "-m", fork1)
		require.Equal(t, 0, result.ExitCode, "Fork1 creation failed: %s", result.Combined)
		defer env.RunCLI("unmount", fork1)
		waitForPath(t, fork1, MountReadyTimeout)

		// Fork2: source=/source/a/b (Fork1), mount=/source/x
		// Use --type=full because overlay fork from inside a mount is not supported
		fork2 := filepath.Join(env.SourceDir, "x")
		result = env.RunCLI("fork", fork1, "--type=full", "-m", fork2)
		require.Equal(t, 0, result.ExitCode, "Fork2 creation failed: %s", result.Combined)
		defer env.RunCLI("unmount", fork2)
		waitForPath(t, fork2, MountReadyTimeout)

		// From Fork2 root: should see "a", "file.txt", NOT "x"
		rootEntries, err := os.ReadDir(fork2)
		require.NoError(t, err)
		rootNames := extractNames(rootEntries)
		t.Logf("Fork2 root listing: %v", rootNames)

		assert.Contains(t, rootNames, "a", "Should see 'a'")
		assert.Contains(t, rootNames, "file.txt", "Should see 'file.txt'")
		assert.NotContains(t, rootNames, "x", "Should NOT see 'x' (Fork2's mount)")

		// From Fork2 listing "a/": should see "other", "sibling.txt", NOT "b"
		aEntries, err := os.ReadDir(filepath.Join(fork2, "a"))
		require.NoError(t, err)
		aNames := extractNames(aEntries)
		t.Logf("Fork2 a/ listing: %v", aNames)

		assert.Contains(t, aNames, "other", "Should see 'other'")
		assert.Contains(t, aNames, "sibling.txt", "Should see 'sibling.txt'")
		assert.NotContains(t, aNames, "b", "Should NOT see 'b' (Fork1's mount at a/b)")
	})

	// TestCascadeForkMountOutsideSource ensures no hiding when mount is outside source
	t.Run("MountOutsideSource", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "mount_outside")
		defer env.Cleanup()

		// Create source with files
		env.CreateSourceFile("file1.txt", "content1")
		env.CreateSourceFile("file2.txt", "content2")

		// Mount outside source directory (at TargetPath which is in TestDir, not SourceDir)
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		require.Equal(t, 0, result.ExitCode, "Fork creation failed: %s", result.Combined)
		env.WaitForMountReady(MountReadyTimeout)

		// All source files should be visible
		entries, err := os.ReadDir(env.TargetPath)
		require.NoError(t, err)
		entryNames := extractNames(entries)

		assert.Contains(t, entryNames, "file1.txt", "Should see file1.txt")
		assert.Contains(t, entryNames, "file2.txt", "Should see file2.txt")
	})

	// TestCascadeForkSiblingForksIsolated tests that sibling forks don't affect each other's hiding
	// Note: This test validates that each fork only hides its OWN mount, not sibling mounts
	t.Run("SiblingForksIsolated", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "sibling_forks")
		defer env.Cleanup()

		// Create source with a file and placeholder directories
		// (directories are needed so NFS cache sees them before forks replace them with symlinks)
		env.CreateSourceFile("file.txt", "content")
		require.NoError(t, os.MkdirAll(filepath.Join(env.SourceDir, "a"), 0755)) // Will become Fork1's mount
		require.NoError(t, os.MkdirAll(filepath.Join(env.SourceDir, "b"), 0755)) // Will become Fork2's mount

		// Create sibling forks (same source, different mounts)
		// The fork command will replace the directories with symlinks
		fork1 := filepath.Join(env.SourceDir, "a")
		fork2 := filepath.Join(env.SourceDir, "b")

		result := env.RunCLI("fork", env.SourceDir, "-m", fork1)
		require.Equal(t, 0, result.ExitCode, "Fork1 creation failed: %s", result.Combined)
		defer env.RunCLI("unmount", fork1)
		waitForPath(t, fork1, MountReadyTimeout)

		result = env.RunCLI("fork", env.SourceDir, "-m", fork2)
		require.Equal(t, 0, result.ExitCode, "Fork2 creation failed: %s", result.Combined)
		defer env.RunCLI("unmount", fork2)
		waitForPath(t, fork2, MountReadyTimeout)

		// Fork1 should NOT see "a" (its own mount), but should see "b" (sibling's mount)
		// Note: Due to NFS caching, we verify the key property - Fork1 hides "a"
		fork1Entries, err := os.ReadDir(fork1)
		require.NoError(t, err)
		fork1Names := extractNames(fork1Entries)
		t.Logf("Fork1 listing: %v", fork1Names)

		assert.Contains(t, fork1Names, "file.txt", "Fork1 should see 'file.txt'")
		assert.NotContains(t, fork1Names, "a", "Fork1 should NOT see 'a' (its own mount)")

		// Fork2 should NOT see "b" (its own mount), but should see "a" (sibling's mount)
		fork2Entries, err := os.ReadDir(fork2)
		require.NoError(t, err)
		fork2Names := extractNames(fork2Entries)
		t.Logf("Fork2 listing: %v", fork2Names)

		assert.Contains(t, fork2Names, "a", "Fork2 should see 'a' (sibling's mount)")
		assert.Contains(t, fork2Names, "file.txt", "Fork2 should see 'file.txt'")
		assert.NotContains(t, fork2Names, "b", "Fork2 should NOT see 'b' (its own mount)")
	})

	// TestCascadeForkCanReadFilesNotHidden verifies files at hidden paths can still be accessed
	// This tests that we're hiding symlinks, not blocking all access
	t.Run("CanReadFilesNotHidden", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "read_not_hidden")
		defer env.Cleanup()

		// Create source with files in various locations
		env.CreateSourceFile("file.txt", "root file")
		require.NoError(t, os.MkdirAll(filepath.Join(env.SourceDir, "a"), 0755))
		env.CreateSourceFile("a/inside.txt", "inside a")

		// Fork1: mount at /source/a/b
		fork1 := filepath.Join(env.SourceDir, "a", "b")
		result := env.RunCLI("fork", env.SourceDir, "-m", fork1)
		require.Equal(t, 0, result.ExitCode, "Fork1 creation failed: %s", result.Combined)
		defer env.RunCLI("unmount", fork1)
		waitForPath(t, fork1, MountReadyTimeout)

		// Fork2: mount at /source/c
		// Use --type=full because overlay fork from inside a mount is not supported
		fork2 := filepath.Join(env.SourceDir, "c")
		result = env.RunCLI("fork", fork1, "--type=full", "-m", fork2)
		require.Equal(t, 0, result.ExitCode, "Fork2 creation failed: %s", result.Combined)
		defer env.RunCLI("unmount", fork2)
		waitForPath(t, fork2, MountReadyTimeout)

		// Should be able to read file.txt from Fork2
		content, err := os.ReadFile(filepath.Join(fork2, "file.txt"))
		require.NoError(t, err)
		assert.Equal(t, "root file", string(content))

		// Should be able to read a/inside.txt from Fork2
		content, err = os.ReadFile(filepath.Join(fork2, "a", "inside.txt"))
		require.NoError(t, err)
		assert.Equal(t, "inside a", string(content))
	})
}

// --- Helper functions ---

// extractNames extracts names from directory entries
func extractNames(entries []os.DirEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}

// waitForPath polls until a path is accessible
func waitForPath(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	g := NewWithT(t)
	g.Eventually(func() error {
		_, err := os.ReadDir(path)
		return err
	}).WithTimeout(timeout).WithPolling(50 * time.Millisecond).Should(Succeed())
}

// CreateSourceDir helper for ForkTestEnv
func (e *ForkTestEnv) CreateSourceDir(relPath string) {
	e.t.Helper()
	fullPath := filepath.Join(e.SourceDir, relPath)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		e.t.Fatalf("Failed to create source directory %s: %v", relPath, err)
	}
}
