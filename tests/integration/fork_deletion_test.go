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
)

// TestForkDeletion groups all fork deletion tests under a single shared daemon
func TestForkDeletion(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "fork_deletion")
	defer shared.Cleanup()

	t.Run("DeleteSourceOnlyFile", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "delete_source_only")
		defer env.Cleanup()

		// Create files in source
		env.CreateSourceFile("keep.txt", "keep this")
		env.CreateSourceFile("delete.txt", "delete this")

		// Create soft-fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("soft-fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Verify both files are visible through the fork
		if !env.TargetFileExists("keep.txt") {
			t.Fatal("keep.txt should exist in fork")
		}
		if !env.TargetFileExists("delete.txt") {
			t.Fatal("delete.txt should exist in fork")
		}

		// Delete the source-only file through the fork
		deleteFile := filepath.Join(env.TargetPath, "delete.txt")
		if err := os.Remove(deleteFile); err != nil {
			t.Fatalf("Failed to delete delete.txt: %v", err)
		}
		// No sleep needed: noac mount option disables NFS attribute caching,
		// and tombstone mechanism provides immediate deletion visibility

		// Verify delete.txt is no longer visible
		if env.TargetFileExists("delete.txt") {
			t.Fatal("delete.txt should not exist after deletion")
		}

		// Verify keep.txt is still visible
		if !env.TargetFileExists("keep.txt") {
			t.Fatal("keep.txt should still exist")
		}

		// Verify source file is unchanged (still exists)
		sourceDeleteFile := filepath.Join(env.SourceDir, "delete.txt")
		if _, err := os.Stat(sourceDeleteFile); err != nil {
			t.Fatal("source delete.txt should still exist (not modified)")
		}

		// Verify file doesn't appear in directory listing
		entries, err := os.ReadDir(env.TargetPath)
		if err != nil {
			t.Fatalf("Failed to read fork directory: %v", err)
		}
		for _, e := range entries {
			if e.Name() == "delete.txt" {
				t.Fatal("delete.txt should not appear in directory listing")
			}
		}

		t.Log("Delete source-only file test successful")
	})

	t.Run("DeletedFileNotInSnapshot", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "delete_snapshot")
		defer env.Cleanup()

		// Create files in source
		env.CreateSourceFile("file1.txt", "file one")
		env.CreateSourceFile("file2.txt", "file two")

		// Create soft-fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("soft-fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Delete file2.txt through the fork
		if err := os.Remove(filepath.Join(env.TargetPath, "file2.txt")); err != nil {
			t.Fatalf("Failed to delete file2.txt: %v", err)
		}
		// No sleep needed: noac mount option disables NFS attribute caching

		// Create snapshot after deletion
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "after delete", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("snapshot save failed: %s", result.Combined)
		}

		// Restore file2.txt by writing it again
		env.WriteTargetFile("file2.txt", "restored content")
		// No sleep needed: noac mount option disables NFS attribute caching

		// Verify file2.txt is now visible
		if !env.TargetFileExists("file2.txt") {
			t.Fatal("file2.txt should exist after restoration")
		}

		// Restore from snapshot (should delete file2.txt again)
		result = env.RunCLI("data", "restore", "v1", "--data-file", env.DataFile, "-y")
		if result.ExitCode != 0 {
			t.Fatalf("snapshot restore failed: %s", result.Combined)
		}

		// Verify file2.txt is deleted again after restore
		// Note: macOS NFS client caches positive lookups separately from directory listings.
		// Even though the server reports the file as deleted (tombstone), the client may
		// still return success from its lookup cache. We must poll to force cache invalidation.
		g := NewWithT(t)
		file2Path := filepath.Join(env.TargetPath, "file2.txt")
		g.Eventually(func() bool {
			_, err := os.Stat(file2Path)
			return os.IsNotExist(err)
		}).WithTimeout(2 * time.Second).Should(BeTrue(), "file2.txt should not exist after restoring snapshot with deletion")

		// Verify file1.txt still exists
		if !env.TargetFileExists("file1.txt") {
			t.Fatal("file1.txt should still exist")
		}

		t.Log("Deleted file snapshot test successful")
	})

	t.Run("DeleteThenRecreate", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "delete_recreate")
		defer env.Cleanup()

		// Create file in source
		env.CreateSourceFile("test.txt", "original content")

		// Create soft-fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("soft-fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Verify file exists
		if content := env.ReadTargetFile("test.txt"); content != "original content" {
			t.Fatalf("Expected 'original content', got %q", content)
		}

		// Delete the file
		if err := os.Remove(filepath.Join(env.TargetPath, "test.txt")); err != nil {
			t.Fatalf("Failed to delete test.txt: %v", err)
		}
		// No sleep needed: noac mount option disables NFS attribute caching

		// Verify file doesn't exist
		if env.TargetFileExists("test.txt") {
			t.Fatal("test.txt should not exist after deletion")
		}

		// Recreate the file with new content
		env.WriteTargetFile("test.txt", "new content")
		// No sleep needed: noac mount option disables NFS attribute caching

		// Verify file exists with new content
		if content := env.ReadTargetFile("test.txt"); content != "new content" {
			t.Fatalf("Expected 'new content', got %q", content)
		}

		// Verify source file is unchanged
		sourceContent, err := os.ReadFile(filepath.Join(env.SourceDir, "test.txt"))
		if err != nil {
			t.Fatalf("Failed to read source file: %v", err)
		}
		if string(sourceContent) != "original content" {
			t.Fatalf("Source file should still have original content, got %q", string(sourceContent))
		}

		t.Log("Delete and recreate test successful")
	})

	t.Run("TombstoneBlocksNewSourceFile", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "tombstone_blocks_new")
		defer env.Cleanup()

		// Create initial file in source
		env.CreateSourceFile("test.txt", "original content")

		// Create soft-fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("soft-fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Verify file is visible
		if content := env.ReadTargetFile("test.txt"); content != "original content" {
			t.Fatalf("Expected 'original content', got %q", content)
		}

		// Delete the file through the fork (creates tombstone)
		if err := os.Remove(filepath.Join(env.TargetPath, "test.txt")); err != nil {
			t.Fatalf("Failed to delete test.txt: %v", err)
		}
		// No sleep needed: noac mount option disables NFS attribute caching

		// Verify file is deleted
		if env.TargetFileExists("test.txt") {
			t.Fatal("test.txt should not exist after deletion")
		}

		// Now create a NEW file with the same name directly in source folder
		// (simulating someone adding a new file to the source outside of the fork)
		env.CreateSourceFile("test.txt", "brand new content from source")
		// No sleep needed: tombstone in overlay blocks source fallback immediately,
		// regardless of whether daemon notices the new source file

		// The file should still NOT be visible through the fork!
		// The tombstone should block the new source file
		if env.TargetFileExists("test.txt") {
			t.Fatal("test.txt should NOT be visible - tombstone should block new source file")
		}

		// Verify the source file does exist directly
		sourceContent, err := os.ReadFile(filepath.Join(env.SourceDir, "test.txt"))
		if err != nil {
			t.Fatalf("Source file should exist: %v", err)
		}
		if string(sourceContent) != "brand new content from source" {
			t.Fatalf("Source file has wrong content: %q", string(sourceContent))
		}

		// Verify file doesn't appear in directory listing
		entries, err := os.ReadDir(env.TargetPath)
		if err != nil {
			t.Fatalf("Failed to read fork directory: %v", err)
		}
		for _, e := range entries {
			if e.Name() == "test.txt" {
				t.Fatal("test.txt should not appear in directory listing - tombstone should block it")
			}
		}

		t.Log("Tombstone blocks new source file test successful")
	})

	t.Run("SourceDeletedDuringMount", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "source_deleted_during_mount")
		defer env.Cleanup()

		// Create initial content in source directory
		env.CreateSourceFile("source_only.txt", "content from source")
		env.CreateSourceFile("will_be_copied.txt", "will copy this")
		env.CreateSourceFile("subdir/nested.txt", "nested content")

		// Fork with source folder
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		env.WaitForMountReady(MountReadyTimeout)

		// Create a file in overlay (not in source)
		env.WriteTargetFile("overlay_only.txt", "overlay content")

		// Copy a file from source to overlay via write (triggers copy-on-write)
		env.WriteTargetFile("will_be_copied.txt", "modified in overlay")

		// Verify both source-only and overlay files work before deletion
		content := env.ReadTargetFile("source_only.txt")
		if content != "content from source" {
			t.Errorf("Expected source content, got %q", content)
		}

		content = env.ReadTargetFile("overlay_only.txt")
		if content != "overlay content" {
			t.Errorf("Expected overlay content, got %q", content)
		}

		// Now delete the source directory
		t.Log("Deleting source directory...")
		if err := os.RemoveAll(env.SourceDir); err != nil {
			t.Fatalf("Failed to delete source directory: %v", err)
		}

		// Give NFS time to notice (if any caching)
		time.Sleep(50 * time.Millisecond)

		// Overlay files should still work
		content = env.ReadTargetFile("overlay_only.txt")
		if content != "overlay content" {
			t.Errorf("Overlay file should still work, got %q", content)
		}

		content = env.ReadTargetFile("will_be_copied.txt")
		if content != "modified in overlay" {
			t.Errorf("Copied file should still work, got %q", content)
		}

		// Source-only files should now return errors (not crash)
		_, err := os.ReadFile(filepath.Join(env.TargetPath, "source_only.txt"))
		if err == nil {
			t.Error("Reading source-only file should fail after source deleted")
		} else {
			t.Logf("Expected error for source-only file: %v", err)
		}

		// Directory listing should not crash (may return partial results)
		entries, err := os.ReadDir(env.TargetPath)
		if err != nil {
			t.Logf("ReadDir error (may be expected): %v", err)
		} else {
			t.Logf("Directory entries after source deletion: %d", len(entries))
			for _, e := range entries {
				t.Logf("  - %s", e.Name())
			}
		}

		// Creating new files should still work
		env.WriteTargetFile("new_after_delete.txt", "new content")
		content = env.ReadTargetFile("new_after_delete.txt")
		if content != "new content" {
			t.Errorf("New file creation should work, got %q", content)
		}

		t.Log("Source deleted during mount test successful - no crash")
	})

	t.Run("RemountAfterSourceDeleted", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "remount_source_deleted")
		defer env.Cleanup()

		// Create initial content in source directory
		env.CreateSourceFile("source_file.txt", "source content")

		// Fork with source folder
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		env.WaitForMountReady(MountReadyTimeout)

		// Create a file in overlay
		env.WriteTargetFile("overlay_file.txt", "overlay content")

		// Copy source file to overlay
		env.WriteTargetFile("source_file.txt", "copied and modified")

		// Verify files work
		content := env.ReadTargetFile("overlay_file.txt")
		if content != "overlay content" {
			t.Fatalf("Expected overlay content, got %q", content)
		}

		// Unmount
		result = env.RunCLI("unmount", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("unmount failed: %s", result.Combined)
		}
		env.WaitForUnmount(2*time.Second, env.TargetPath)

		// Delete source directory while unmounted
		t.Log("Deleting source directory while unmounted...")
		if err := os.RemoveAll(env.SourceDir); err != nil {
			t.Fatalf("Failed to delete source directory: %v", err)
		}

		// Remount - should not crash even though source is gone
		t.Log("Remounting after source deleted...")
		result = env.RunCLI("mount", env.TargetPath, "-d", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("remount failed (should succeed even without source): %s", result.Combined)
		}

		env.WaitForMountReady(MountReadyTimeout)

		// Overlay files should still work
		content = env.ReadTargetFile("overlay_file.txt")
		if content != "overlay content" {
			t.Errorf("Overlay file should work after remount, got %q", content)
		}

		content = env.ReadTargetFile("source_file.txt")
		if content != "copied and modified" {
			t.Errorf("Copied file should work after remount, got %q", content)
		}

		// Creating new files should work
		env.WriteTargetFile("new_file.txt", "new content")
		content = env.ReadTargetFile("new_file.txt")
		if content != "new content" {
			t.Errorf("New file creation should work, got %q", content)
		}

		t.Log("Remount after source deleted test successful - no crash")
	})

	t.Run("SourceMovedDuringMount", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "source_moved")
		defer env.Cleanup()

		// Create initial content in source directory
		env.CreateSourceFile("test.txt", "source content")

		// Fork with source folder
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		env.WaitForMountReady(MountReadyTimeout)

		// Create overlay file
		env.WriteTargetFile("overlay.txt", "overlay content")

		// Verify source read works
		content := env.ReadTargetFile("test.txt")
		if content != "source content" {
			t.Errorf("Expected source content, got %q", content)
		}

		// Move source directory to a new location
		movedPath := env.SourceDir + "_moved"
		t.Logf("Moving source from %s to %s", env.SourceDir, movedPath)
		if err := os.Rename(env.SourceDir, movedPath); err != nil {
			t.Fatalf("Failed to move source directory: %v", err)
		}
		defer os.RemoveAll(movedPath) // Cleanup moved dir

		time.Sleep(50 * time.Millisecond)

		// Overlay files should still work
		content = env.ReadTargetFile("overlay.txt")
		if content != "overlay content" {
			t.Errorf("Overlay file should still work, got %q", content)
		}

		// Source-only files should return errors (source path no longer valid)
		_, err := os.ReadFile(filepath.Join(env.TargetPath, "test.txt"))
		if err == nil {
			t.Error("Reading source-only file should fail after source moved")
		} else {
			t.Logf("Expected error for source-only file: %v", err)
		}

		// Creating new files should still work
		env.WriteTargetFile("new.txt", "new content")
		content = env.ReadTargetFile("new.txt")
		if content != "new content" {
			t.Errorf("New file creation should work, got %q", content)
		}

		t.Log("Source moved during mount test successful - no crash")
	})
}
