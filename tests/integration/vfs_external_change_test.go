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

	"latentfs/internal/storage"
)

// These tests verify that CheckForExternalChanges is called on appropriate VFS operations.
// Tests are designed to FAIL if CheckForExternalChanges is missing on specific operations.
//
// Current state:
// - Open: has CheckForExternalChanges
// - OpenDir: has CheckForExternalChanges
// - Lookup: missing (these tests expose the gap)
// - Mkdir: missing
// - Unlink: missing
// - Rmdir: missing
// - Rename: missing
// - Symlink: missing
// - Readlink: missing

func TestVFSExternalChange(t *testing.T) {
	t.Skip("TODO: CheckForExternalChanges not yet implemented on Lookup/Mkdir/Unlink/Rmdir/Rename/Symlink/Readlink")
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "vfs_external")
	defer shared.Cleanup()

	// TestLookupAfterExternalRestore tests that Lookup sees restored files
	// without requiring an Open call first.
	//
	// Scenario: os.Stat (triggers Lookup) on a file that was restored externally.
	// Without CheckForExternalChanges on Lookup, daemon uses stale epoch and may return ENOENT
	// or stale file attributes.
	//
	// NOTE: This test deliberately does NOT call ReadDir after restore to avoid
	// triggering OpenDir's CheckForExternalChanges. The test should only pass
	// if Lookup itself has CheckForExternalChanges.
	t.Run("LookupAfterExternalRestore", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "lookup_restore")
		defer env.Cleanup()

		// Create empty fork
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Create a file in a subdirectory (we'll later access the subdir file
		// without first doing ReadDir on the subdir)
		subDir := filepath.Join(env.TargetPath, "subdir")
		if err := os.Mkdir(subDir, 0755); err != nil {
			t.Fatalf("failed to create subdir: %v", err)
		}
		env.WriteTargetFile("subdir/lookup_test.txt", "original content")
		filePath := filepath.Join(env.TargetPath, "subdir", "lookup_test.txt")

		// Save snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "v1", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// Delete the file through NFS
		if err := os.Remove(filePath); err != nil {
			t.Fatalf("failed to delete file: %v", err)
		}

		// Verify file is gone
		g := NewWithT(t)
		g.Eventually(func() bool {
			_, err := os.Stat(filePath)
			return os.IsNotExist(err)
		}).WithTimeout(1 * time.Second).Should(BeTrue())

		// --- External restore WITHOUT InvalidateCache IPC ---
		// This simulates external process modifying the database
		df, err := storage.OpenWithContext(env.DataFile, storage.DBContextCLI)
		if err != nil {
			t.Fatalf("failed to open DataFile: %v", err)
		}
		_, err = df.RestoreFromSnapshot("v1", nil)
		if err != nil {
			df.Close()
			t.Fatalf("restore failed: %v", err)
		}
		df.Close()

		// DELIBERATELY DO NOT CALL ReadDir here!
		// The test should pass only if Lookup has CheckForExternalChanges.
		// If we called ReadDir, OpenDir's CheckForExternalChanges would refresh the epoch,
		// masking the need for Lookup to also check.

		// Small delay to ensure external write is committed
		time.Sleep(50 * time.Millisecond)

		// Now use os.Stat (Lookup) - this is the KEY test
		// Without CheckForExternalChanges on Lookup, this may fail with ENOENT
		// because daemon still uses old epoch where file is deleted
		g.Eventually(func() error {
			_, err := os.Stat(filePath)
			return err
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed(),
			"Lookup should see restored file without requiring OpenDir first")

		t.Log("LookupAfterExternalRestore passed")
	})

	// TestStatAfterExternalContentChange tests that GetAttr via Stat sees updated
	// file size after external restore.
	//
	// Scenario: File content changes due to restore, os.Stat should return correct size.
	//
	// NOTE: This test deliberately does NOT call ReadDir after restore.
	t.Run("StatAfterExternalContentChange", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "stat_change")
		defer env.Cleanup()

		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Create file in subdirectory
		subDir := filepath.Join(env.TargetPath, "sub")
		if err := os.Mkdir(subDir, 0755); err != nil {
			t.Fatalf("failed to create subdir: %v", err)
		}
		env.WriteTargetFile("sub/size_test.txt", "short")
		fullPath := filepath.Join(env.TargetPath, "sub", "size_test.txt")

		// Save snapshot with short content
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "short", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// Write longer content
		env.WriteTargetFile("sub/size_test.txt", "much longer content here")

		// Verify longer content
		g := NewWithT(t)
		g.Eventually(func() int64 {
			info, _ := os.Stat(fullPath)
			if info == nil {
				return 0
			}
			return info.Size()
		}).WithTimeout(1 * time.Second).Should(BeNumerically(">", 10))

		// External restore to short content
		df, err := storage.OpenWithContext(env.DataFile, storage.DBContextCLI)
		if err != nil {
			t.Fatalf("failed to open DataFile: %v", err)
		}
		_, err = df.RestoreFromSnapshot("v1", nil)
		if err != nil {
			df.Close()
			t.Fatalf("restore failed: %v", err)
		}
		df.Close()

		// DELIBERATELY DO NOT CALL ReadDir here!
		time.Sleep(50 * time.Millisecond)

		// Stat should show correct (short) size
		// Without CheckForExternalChanges on GetAttr/Lookup, may show old size
		g.Eventually(func() int64 {
			info, err := os.Stat(fullPath)
			if err != nil || info == nil {
				return -1
			}
			return info.Size()
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(Equal(int64(5)),
			"Stat should show restored file size (5 bytes for 'short')")

		t.Log("StatAfterExternalContentChange passed")
	})

	// TestMkdirAfterExternalRestore tests that Mkdir works correctly after
	// parent directory structure changes due to external restore.
	//
	// Scenario: Parent directory was deleted, then restored. Mkdir in that
	// directory should work after restore.
	t.Run("MkdirAfterExternalRestore", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "mkdir_restore")
		defer env.Cleanup()

		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Create parent directory
		parentDir := filepath.Join(env.TargetPath, "parent")
		if err := os.Mkdir(parentDir, 0755); err != nil {
			t.Fatalf("failed to create parent: %v", err)
		}

		// Save snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "with parent", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// Remove parent directory
		if err := os.Remove(parentDir); err != nil {
			t.Fatalf("failed to remove parent: %v", err)
		}

		g := NewWithT(t)
		g.Eventually(func() bool {
			_, err := os.Stat(parentDir)
			return os.IsNotExist(err)
		}).WithTimeout(1 * time.Second).Should(BeTrue())

		// External restore
		df, err := storage.OpenWithContext(env.DataFile, storage.DBContextCLI)
		if err != nil {
			t.Fatalf("failed to open DataFile: %v", err)
		}
		_, err = df.RestoreFromSnapshot("v1", nil)
		if err != nil {
			df.Close()
			t.Fatalf("restore failed: %v", err)
		}
		df.Close()

		// DELIBERATELY DO NOT CALL ReadDir here!
		time.Sleep(50 * time.Millisecond)

		// Now try to create a subdirectory in the restored parent
		// This triggers Lookup (to resolve parent) then Mkdir
		// Without CheckForExternalChanges on Lookup/Mkdir, this may fail
		childDir := filepath.Join(parentDir, "child")
		g.Eventually(func() error {
			return os.Mkdir(childDir, 0755)
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed(),
			"Mkdir should work in restored parent directory")

		t.Log("MkdirAfterExternalRestore passed")
	})

	// TestUnlinkAfterExternalRestore tests that Unlink (file deletion) works
	// correctly after file is restored externally.
	//
	// Scenario: File is deleted, restored externally, then deleted again through NFS.
	t.Run("UnlinkAfterExternalRestore", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "unlink_restore")
		defer env.Cleanup()

		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Create a file
		env.WriteTargetFile("to_delete.txt", "delete me")
		fullPath := filepath.Join(env.TargetPath, "to_delete.txt")

		// Save snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "with file", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// Delete file
		if err := os.Remove(fullPath); err != nil {
			t.Fatalf("failed to delete: %v", err)
		}

		g := NewWithT(t)
		g.Eventually(func() bool {
			_, err := os.Stat(fullPath)
			return os.IsNotExist(err)
		}).WithTimeout(1 * time.Second).Should(BeTrue())

		// External restore (file comes back)
		df, err := storage.OpenWithContext(env.DataFile, storage.DBContextCLI)
		if err != nil {
			t.Fatalf("failed to open DataFile: %v", err)
		}
		_, err = df.RestoreFromSnapshot("v1", nil)
		if err != nil {
			df.Close()
			t.Fatalf("restore failed: %v", err)
		}
		df.Close()

		// DELIBERATELY DO NOT CALL ReadDir here!
		time.Sleep(50 * time.Millisecond)

		// Now try to delete the restored file
		// This triggers Lookup then Unlink
		// Without CheckForExternalChanges, Unlink may try to delete at old epoch where file doesn't exist
		g.Eventually(func() error {
			return os.Remove(fullPath)
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed(),
			"Unlink should work on restored file")

		t.Log("UnlinkAfterExternalRestore passed")
	})

	// TestRenameAfterExternalRestore tests that Rename works correctly
	// after source or destination changes due to external restore.
	t.Run("RenameAfterExternalRestore", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "rename_restore")
		defer env.Cleanup()

		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Create source file
		srcPath := filepath.Join(env.TargetPath, "source.txt")
		dstPath := filepath.Join(env.TargetPath, "dest.txt")
		env.WriteTargetFile("source.txt", "rename me")

		// Save snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "with source", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// Delete source file
		if err := os.Remove(srcPath); err != nil {
			t.Fatalf("failed to delete source: %v", err)
		}

		g := NewWithT(t)
		g.Eventually(func() bool {
			_, err := os.Stat(srcPath)
			return os.IsNotExist(err)
		}).WithTimeout(1 * time.Second).Should(BeTrue())

		// External restore (source file comes back)
		df, err := storage.OpenWithContext(env.DataFile, storage.DBContextCLI)
		if err != nil {
			t.Fatalf("failed to open DataFile: %v", err)
		}
		_, err = df.RestoreFromSnapshot("v1", nil)
		if err != nil {
			df.Close()
			t.Fatalf("restore failed: %v", err)
		}
		df.Close()

		// DELIBERATELY DO NOT CALL ReadDir here!
		time.Sleep(50 * time.Millisecond)

		// Now try to rename the restored file
		// This triggers Lookup on both source and destination, then Rename
		g.Eventually(func() error {
			return os.Rename(srcPath, dstPath)
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed(),
			"Rename should work on restored file")

		// Verify rename succeeded
		if _, err := os.Stat(dstPath); err != nil {
			t.Errorf("destination file should exist after rename: %v", err)
		}
		if _, err := os.Stat(srcPath); !os.IsNotExist(err) {
			t.Errorf("source file should not exist after rename")
		}

		t.Log("RenameAfterExternalRestore passed")
	})

	// TestSymlinkReadlinkAfterExternalRestore tests that symlink operations
	// work correctly after external restore.
	t.Run("SymlinkReadlinkAfterExternalRestore", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "symlink_restore")
		defer env.Cleanup()

		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Create a symlink
		linkPath := filepath.Join(env.TargetPath, "mylink")
		if err := os.Symlink("target_path", linkPath); err != nil {
			t.Fatalf("failed to create symlink: %v", err)
		}

		// Save snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "with symlink", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// Delete symlink
		if err := os.Remove(linkPath); err != nil {
			t.Fatalf("failed to delete symlink: %v", err)
		}

		g := NewWithT(t)
		g.Eventually(func() bool {
			_, err := os.Lstat(linkPath)
			return os.IsNotExist(err)
		}).WithTimeout(1 * time.Second).Should(BeTrue())

		// External restore (symlink comes back)
		df, err := storage.OpenWithContext(env.DataFile, storage.DBContextCLI)
		if err != nil {
			t.Fatalf("failed to open DataFile: %v", err)
		}
		_, err = df.RestoreFromSnapshot("v1", nil)
		if err != nil {
			df.Close()
			t.Fatalf("restore failed: %v", err)
		}
		df.Close()

		// DELIBERATELY DO NOT CALL ReadDir here!
		time.Sleep(50 * time.Millisecond)

		// Now try to read the restored symlink
		// This triggers Lookup then Readlink
		var target string
		g.Eventually(func() error {
			var err error
			target, err = os.Readlink(linkPath)
			return err
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed(),
			"Readlink should work on restored symlink")

		if target != "target_path" {
			t.Errorf("symlink target = %q, want %q", target, "target_path")
		}

		t.Log("SymlinkReadlinkAfterExternalRestore passed")
	})

	// TestRmdirAfterExternalRestore tests that Rmdir works correctly
	// after directory is restored externally.
	t.Run("RmdirAfterExternalRestore", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "rmdir_restore")
		defer env.Cleanup()

		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Create a directory
		dirPath := filepath.Join(env.TargetPath, "mydir")
		if err := os.Mkdir(dirPath, 0755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}

		// Save snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "with dir", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// Delete directory
		if err := os.Remove(dirPath); err != nil {
			t.Fatalf("failed to delete directory: %v", err)
		}

		g := NewWithT(t)
		g.Eventually(func() bool {
			_, err := os.Stat(dirPath)
			return os.IsNotExist(err)
		}).WithTimeout(1 * time.Second).Should(BeTrue())

		// External restore (directory comes back)
		df, err := storage.OpenWithContext(env.DataFile, storage.DBContextCLI)
		if err != nil {
			t.Fatalf("failed to open DataFile: %v", err)
		}
		_, err = df.RestoreFromSnapshot("v1", nil)
		if err != nil {
			df.Close()
			t.Fatalf("restore failed: %v", err)
		}
		df.Close()

		// DELIBERATELY DO NOT CALL ReadDir here!
		time.Sleep(50 * time.Millisecond)

		// Now try to remove the restored directory
		// This triggers Lookup then Rmdir
		g.Eventually(func() error {
			return os.Remove(dirPath)
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed(),
			"Rmdir should work on restored directory")

		t.Log("RmdirAfterExternalRestore passed")
	})
}
