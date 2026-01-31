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
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

// TestVFSBasic groups all basic VFS tests under a single shared daemon
func TestVFSBasic(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "vfs_basic")
	defer shared.Cleanup()

	t.Run("BasicMountUnmount", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "mount_unmount")
		defer env.Cleanup()

		env.InitDataFile()

		// Verify mount exists
		info, err := os.Stat(env.Mount)
		if err != nil {
			t.Fatalf("mount point not accessible: %v", err)
		}
		if !info.IsDir() {
			t.Fatal("mount point is not a directory")
		}

		// Unmount
		env.Unmount()

		t.Log("Basic mount/unmount successful")
	})

	t.Run("ListEmptyDirectory", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "list_empty")
		defer env.Cleanup()

		env.InitDataFile()

		// List directory - should be empty
		entries, err := os.ReadDir(env.Mount)
		if err != nil {
			t.Fatalf("ReadDir failed: %v", err)
		}

		if len(entries) != 0 {
			t.Errorf("expected empty directory, got %d entries", len(entries))
			for _, e := range entries {
				t.Logf("  - %s", e.Name())
			}
		}

		t.Log("List empty directory successful")
	})

	t.Run("WriteAndReadFile", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "write_read")
		defer env.Cleanup()

		env.InitDataFile()

		// Write a file
		testContent := "Hello, LatentFS!"
		env.WriteFile("test.txt", testContent)

		// Read it back
		content := env.ReadFile("test.txt")
		if content != testContent {
			t.Errorf("content mismatch: got %q, want %q", content, testContent)
		}

		// List directory - should show the file
		entries, err := os.ReadDir(env.Mount)
		if err != nil {
			t.Fatalf("ReadDir failed: %v", err)
		}

		// Filter out macOS AppleDouble files (._*)
		var realEntries []os.DirEntry
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), "._") {
				realEntries = append(realEntries, e)
			}
		}

		if len(realEntries) != 1 {
			t.Errorf("expected 1 entry, got %d", len(realEntries))
		}
		if len(realEntries) > 0 && realEntries[0].Name() != "test.txt" {
			t.Errorf("expected test.txt, got %s", realEntries[0].Name())
		}

		t.Log("Write and read file successful")
	})

	t.Run("MultipleFiles", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "multi_files")
		defer env.Cleanup()

		env.InitDataFile()

		// Create multiple files
		files := map[string]string{
			"file1.txt": "content one",
			"file2.txt": "content two",
			"file3.txt": "content three",
		}

		for name, content := range files {
			env.WriteFile(name, content)
		}

		// List and verify
		entries, err := os.ReadDir(env.Mount)
		if err != nil {
			t.Fatalf("ReadDir failed: %v", err)
		}

		// Filter out macOS AppleDouble files (._*)
		var realEntries []os.DirEntry
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), "._") {
				realEntries = append(realEntries, e)
			}
		}

		if len(realEntries) != len(files) {
			t.Errorf("expected %d entries, got %d", len(files), len(realEntries))
		}

		// Verify each file's content
		for name, expectedContent := range files {
			content := env.ReadFile(name)
			if content != expectedContent {
				t.Errorf("file %s: got %q, want %q", name, content, expectedContent)
			}
		}

		t.Log("Multiple files test successful")
	})

	t.Run("CreateDirectory", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "mkdir")
		defer env.Cleanup()

		env.InitDataFile()

		// Create a directory
		dirPath := filepath.Join(env.Mount, "subdir")
		if err := os.Mkdir(dirPath, 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		// Verify it's a directory
		info, err := os.Stat(dirPath)
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}
		if !info.IsDir() {
			t.Error("expected directory, got file")
		}

		// Create file inside directory
		env.WriteFile("subdir/nested.txt", "nested content")

		// Read it back
		content := env.ReadFile("subdir/nested.txt")
		if content != "nested content" {
			t.Errorf("nested file content mismatch: got %q", content)
		}

		t.Log("Directory creation successful")
	})

	t.Run("DeleteFile", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "delete")
		defer env.Cleanup()

		env.InitDataFile()

		// Create and then delete a file
		env.WriteFile("to_delete.txt", "will be deleted")

		if !env.FileExists("to_delete.txt") {
			t.Fatal("file should exist before deletion")
		}

		env.DeleteFile("to_delete.txt")

		if env.FileExists("to_delete.txt") {
			t.Error("file should not exist after deletion")
		}

		t.Log("File deletion successful")
	})

	t.Run("AppendFile", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "append")
		defer env.Cleanup()

		env.InitDataFile()

		// Create initial file
		env.WriteFile("append.txt", "initial")

		// Append to it
		filePath := filepath.Join(env.Mount, "append.txt")
		f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			t.Fatalf("OpenFile for append failed: %v", err)
		}
		if _, err := f.WriteString(" appended"); err != nil {
			f.Close()
			t.Fatalf("append write failed: %v", err)
		}
		f.Close()

		// Verify
		content := env.ReadFile("append.txt")
		if content != "initial appended" {
			t.Errorf("append mismatch: got %q, want %q", content, "initial appended")
		}

		t.Log("File append successful")
	})

	t.Run("LsCommand", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "ls_cmd")
		defer env.Cleanup()

		env.InitDataFile()

		// Create some files
		env.WriteFile("alpha.txt", "a")
		env.WriteFile("beta.txt", "b")
		env.MkdirAll("gamma")

		// Run ls command (use trailing slash to list contents, not symlink info)
		cmd := exec.Command("ls", "-la", env.Mount+"/")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("ls command failed: %v\nOutput: %s", err, output)
		}

		outputStr := string(output)
		t.Logf("ls output:\n%s", outputStr)

		// Verify files appear in ls output
		if !strings.Contains(outputStr, "alpha.txt") {
			t.Error("ls output missing alpha.txt")
		}
		if !strings.Contains(outputStr, "beta.txt") {
			t.Error("ls output missing beta.txt")
		}
		if !strings.Contains(outputStr, "gamma") {
			t.Error("ls output missing gamma directory")
		}

		t.Log("ls command successful")
	})

	t.Run("LargeFile", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "large_file")
		defer env.Cleanup()

		env.InitDataFile()

		// Create a 1MB file
		size := 1024 * 1024
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		filePath := filepath.Join(env.Mount, "large.bin")
		if err := os.WriteFile(filePath, data, 0644); err != nil {
			t.Fatalf("write large file failed: %v", err)
		}

		// Read it back
		readData, err := os.ReadFile(filePath)
		if err != nil {
			t.Fatalf("read large file failed: %v", err)
		}

		if len(readData) != size {
			t.Errorf("size mismatch: got %d, want %d", len(readData), size)
		}

		// Verify content
		for i := 0; i < size; i++ {
			if readData[i] != data[i] {
				t.Errorf("content mismatch at byte %d: got %d, want %d", i, readData[i], data[i])
				break
			}
		}

		t.Log("Large file test successful")
	})

	t.Run("ConcurrentWrites", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "concurrent")
		defer env.Cleanup()

		env.InitDataFile()

		// Launch multiple goroutines writing different files
		numWorkers := 5
		done := make(chan error, numWorkers)

		for i := 0; i < numWorkers; i++ {
			workerID := i
			go func() {
				fileName := "worker_" + strconv.Itoa(workerID) + ".txt"
				content := "content from worker " + strconv.Itoa(workerID)
				filePath := filepath.Join(env.Mount, fileName)
				done <- os.WriteFile(filePath, []byte(content), 0644)
			}()
		}

		// Wait for all workers
		for i := 0; i < numWorkers; i++ {
			if err := <-done; err != nil {
				t.Errorf("worker failed: %v", err)
			}
		}

		// Verify all files exist and have correct content using Eventually
		g := NewWithT(t)
		for i := 0; i < numWorkers; i++ {
			workerID := i
			fileName := "worker_" + strconv.Itoa(workerID) + ".txt"
			expectedContent := "content from worker " + strconv.Itoa(workerID)
			g.Eventually(func() string {
				return env.ReadFile(fileName)
			}).WithTimeout(1 * time.Second).WithPolling(50 * time.Millisecond).Should(Equal(expectedContent))
		}

		t.Log("Concurrent writes successful")
	})

	t.Run("Rename", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "rename")
		defer env.Cleanup()

		env.InitDataFile()

		// Create a file
		env.WriteFile("original.txt", "rename me")

		// Rename it
		oldPath := filepath.Join(env.Mount, "original.txt")
		newPath := filepath.Join(env.Mount, "renamed.txt")
		if err := os.Rename(oldPath, newPath); err != nil {
			t.Fatalf("rename failed: %v", err)
		}

		// Verify old name is gone
		if env.FileExists("original.txt") {
			t.Error("original file should not exist after rename")
		}

		// Verify new name exists with correct content
		content := env.ReadFile("renamed.txt")
		if content != "rename me" {
			t.Errorf("renamed file content mismatch: got %q", content)
		}

		t.Log("Rename successful")
	})

	t.Run("MountLs", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "mount_ls")
		defer env.Cleanup()

		env.InitDataFile()

		// Check mount ls
		result := env.RunCLI("mount", "ls")
		if result.ExitCode != 0 {
			t.Fatalf("mount ls command failed: %s", result.Combined)
		}

		// Should show active mounts
		if !result.Contains("Active mounts") {
			t.Error("mount ls should show active mounts")
		}

		t.Logf("Mount ls output:\n%s", result.Combined)
		t.Log("Mount ls check successful")
	})
}

// TestVFSBasicOps groups additional VFS tests (file ops, timestamps, symlinks)
func TestVFSBasicOps(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "vfs_basic_ops")
	defer shared.Cleanup()

	t.Run("Truncate", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "truncate")
		defer env.Cleanup()

		env.InitDataFile()

		// Create a file with initial content
		filePath := filepath.Join(env.Mount, "truncate.txt")
		initialContent := "Hello, this is a test file for truncation!"
		if err := os.WriteFile(filePath, []byte(initialContent), 0644); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		// Verify initial size
		info, err := os.Stat(filePath)
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}
		if info.Size() != int64(len(initialContent)) {
			t.Errorf("initial size mismatch: got %d, want %d", info.Size(), len(initialContent))
		}

		// Truncate to smaller size
		if err := os.Truncate(filePath, 5); err != nil {
			t.Fatalf("Truncate to 5 failed: %v", err)
		}

		// Verify truncated content
		data, err := os.ReadFile(filePath)
		if err != nil {
			t.Fatalf("ReadFile after truncate failed: %v", err)
		}
		if string(data) != "Hello" {
			t.Errorf("truncated content mismatch: got %q, want %q", string(data), "Hello")
		}

		// Truncate to zero
		if err := os.Truncate(filePath, 0); err != nil {
			t.Fatalf("Truncate to 0 failed: %v", err)
		}

		info, err = os.Stat(filePath)
		if err != nil {
			t.Fatalf("Stat after zero truncate failed: %v", err)
		}
		if info.Size() != 0 {
			t.Errorf("size after zero truncate: got %d, want 0", info.Size())
		}

		t.Log("Truncate test successful")
	})

	t.Run("OpenExclusive", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "open_excl")
		defer env.Cleanup()

		env.InitDataFile()

		filePath := filepath.Join(env.Mount, "exclusive.txt")

		// Create file with O_EXCL should succeed
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			t.Fatalf("First O_EXCL create failed: %v", err)
		}
		f.Close()

		// Second create with O_EXCL should fail
		_, err = os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err == nil {
			t.Error("Second O_EXCL create should have failed")
		} else if !os.IsExist(err) {
			t.Logf("Got expected error (may vary by OS): %v", err)
		}

		t.Log("Open exclusive test successful")
	})

	t.Run("OpenTruncate", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "open_trunc")
		defer env.Cleanup()

		env.InitDataFile()

		filePath := filepath.Join(env.Mount, "trunc.txt")

		// Create file with content
		if err := os.WriteFile(filePath, []byte("original content"), 0644); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		// Open with O_TRUNC should clear content
		f, err := os.OpenFile(filePath, os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			t.Fatalf("Open with O_TRUNC failed: %v", err)
		}
		f.WriteString("new")
		f.Close()

		// Verify content was replaced
		data, err := os.ReadFile(filePath)
		if err != nil {
			t.Fatalf("ReadFile failed: %v", err)
		}
		if string(data) != "new" {
			t.Errorf("content after O_TRUNC: got %q, want %q", string(data), "new")
		}

		t.Log("Open truncate test successful")
	})

	t.Run("RenameDirectory", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "rename_dir")
		defer env.Cleanup()

		env.InitDataFile()

		// Create a directory with a file inside
		oldDir := filepath.Join(env.Mount, "olddir")
		if err := os.Mkdir(oldDir, 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		// Create file inside directory
		env.WriteFile("olddir/inside.txt", "content inside")

		// Rename directory
		newDir := filepath.Join(env.Mount, "newdir")
		if err := os.Rename(oldDir, newDir); err != nil {
			t.Fatalf("Rename directory failed: %v", err)
		}

		// Verify old path doesn't exist
		if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
			t.Error("old directory should not exist after rename")
		}

		// Verify new path exists and is a directory
		info, err := os.Stat(newDir)
		if err != nil {
			t.Fatalf("Stat new directory failed: %v", err)
		}
		if !info.IsDir() {
			t.Error("renamed path should be a directory")
		}

		// Verify file inside directory is accessible
		content := env.ReadFile("newdir/inside.txt")
		if content != "content inside" {
			t.Errorf("file content after dir rename: got %q", content)
		}

		t.Log("Rename directory test successful")
	})

	t.Run("RemoveDirectory", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "rmdir")
		defer env.Cleanup()

		env.InitDataFile()

		// Create an empty directory
		dirPath := filepath.Join(env.Mount, "emptydir")
		if err := os.Mkdir(dirPath, 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		// Verify it exists
		if _, err := os.Stat(dirPath); err != nil {
			t.Fatalf("directory should exist: %v", err)
		}

		// Remove it
		if err := os.Remove(dirPath); err != nil {
			t.Fatalf("Remove empty directory failed: %v", err)
		}

		// Verify it's gone
		if _, err := os.Stat(dirPath); !os.IsNotExist(err) {
			t.Error("directory should not exist after removal")
		}

		t.Log("Remove directory test successful")
	})

	t.Run("RemoveNonEmptyDirectory", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "rmdir_nonempty")
		defer env.Cleanup()

		env.InitDataFile()

		// Create a directory with a file inside
		dirPath := filepath.Join(env.Mount, "nonempty")
		if err := os.Mkdir(dirPath, 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}
		env.WriteFile("nonempty/file.txt", "content")

		// Try to remove non-empty directory - should fail
		err := os.Remove(dirPath)
		if err == nil {
			t.Error("Remove non-empty directory should have failed")
		}

		// Directory should still exist
		if _, err := os.Stat(dirPath); err != nil {
			t.Errorf("directory should still exist: %v", err)
		}

		t.Log("Remove non-empty directory test successful")
	})

	t.Run("StatAttributes", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "stat_attrs")
		defer env.Cleanup()

		env.InitDataFile()

		// Create a file with known content
		content := "Test content for stat"
		env.WriteFile("stattest.txt", content)

		filePath := filepath.Join(env.Mount, "stattest.txt")
		info, err := os.Stat(filePath)
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}

		// Check size
		if info.Size() != int64(len(content)) {
			t.Errorf("size mismatch: got %d, want %d", info.Size(), len(content))
		}

		// Check it's a regular file
		if !info.Mode().IsRegular() {
			t.Error("should be a regular file")
		}

		// Check it's not a directory
		if info.IsDir() {
			t.Error("should not be a directory")
		}

		// Check name
		if info.Name() != "stattest.txt" {
			t.Errorf("name mismatch: got %q", info.Name())
		}

		// Check directory stat
		dirPath := filepath.Join(env.Mount, "statdir")
		if err := os.Mkdir(dirPath, 0755); err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		dirInfo, err := os.Stat(dirPath)
		if err != nil {
			t.Fatalf("Stat directory failed: %v", err)
		}

		if !dirInfo.IsDir() {
			t.Error("directory should report IsDir=true")
		}

		t.Log("Stat attributes test successful")
	})

	t.Run("OverwriteFile", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "overwrite")
		defer env.Cleanup()

		env.InitDataFile()

		filePath := filepath.Join(env.Mount, "overwrite.txt")

		// Create file with initial content
		if err := os.WriteFile(filePath, []byte("AAAAAAAAAA"), 0644); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		// Open and write in the middle
		f, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
		if err != nil {
			t.Fatalf("OpenFile failed: %v", err)
		}

		// Seek to position 3 and write
		if _, err := f.Seek(3, 0); err != nil {
			f.Close()
			t.Fatalf("Seek failed: %v", err)
		}
		if _, err := f.WriteString("BBB"); err != nil {
			f.Close()
			t.Fatalf("Write failed: %v", err)
		}
		f.Close()

		// Read and verify
		data, err := os.ReadFile(filePath)
		if err != nil {
			t.Fatalf("ReadFile failed: %v", err)
		}

		expected := "AAABBBAAAA"
		if string(data) != expected {
			t.Errorf("overwrite result: got %q, want %q", string(data), expected)
		}

		t.Log("Overwrite file test successful")
	})

	t.Run("ReadPartial", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "read_partial")
		defer env.Cleanup()

		env.InitDataFile()

		filePath := filepath.Join(env.Mount, "partial.txt")

		// Create file with known content
		content := "0123456789ABCDEF"
		if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		// Open and read from middle
		f, err := os.Open(filePath)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer f.Close()

		// Seek to position 5
		if _, err := f.Seek(5, 0); err != nil {
			t.Fatalf("Seek failed: %v", err)
		}

		// Read 4 bytes
		buf := make([]byte, 4)
		n, err := f.Read(buf)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}

		if n != 4 {
			t.Errorf("read count: got %d, want 4", n)
		}

		expected := "5678"
		if string(buf) != expected {
			t.Errorf("partial read: got %q, want %q", string(buf), expected)
		}

		t.Log("Read partial test successful")
	})

	t.Run("FileTimestamps", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "timestamps")
		defer env.Cleanup()

		env.InitDataFile()

		filePath := filepath.Join(env.Mount, "timestamps.txt")

		// Create file
		if err := os.WriteFile(filePath, []byte("initial"), 0644); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}

		// Get initial mtime
		info1, err := os.Stat(filePath)
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}
		mtime1 := info1.ModTime()

		// Modify file and verify mtime changed using Eventually
		g := NewWithT(t)
		g.Eventually(func() bool {
			if err := os.WriteFile(filePath, []byte("modified content"), 0644); err != nil {
				return false
			}
			info2, err := os.Stat(filePath)
			if err != nil {
				return false
			}
			return info2.ModTime().After(mtime1)
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue(), "mtime should have increased")

		t.Log("File timestamps test successful")
	})

	t.Run("Symlink", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "symlink")
		defer env.Cleanup()

		env.InitDataFile()

		// Create a target file
		targetContent := "symlink target content"
		env.WriteFile("target.txt", targetContent)

		// Try to create symlink using ln -s command
		linkPath := filepath.Join(env.Mount, "link.txt")

		cmd := exec.Command("ln", "-s", "target.txt", linkPath)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Logf("ln -s failed (may not be supported): %v, output: %s", err, output)
			t.Skip("Symlinks not supported on this mount")
		}

		// Verify the symlink was created
		linkInfo, err := os.Lstat(linkPath)
		if err != nil {
			t.Fatalf("Lstat link failed: %v", err)
		}

		// Check it's a symlink
		if linkInfo.Mode()&os.ModeSymlink == 0 {
			t.Error("link should be a symlink")
		}

		// Read the symlink target
		readTarget, err := os.Readlink(linkPath)
		if err != nil {
			t.Fatalf("Readlink failed: %v", err)
		}

		if readTarget != "target.txt" {
			t.Errorf("symlink target: got %q, want %q", readTarget, "target.txt")
		}

		// Read through the symlink (follow it)
		contentBytes, err := os.ReadFile(linkPath)
		if err != nil {
			t.Fatalf("ReadFile through symlink failed: %v", err)
		}

		if string(contentBytes) != targetContent {
			t.Errorf("content through symlink: got %q, want %q", string(contentBytes), targetContent)
		}

		t.Log("Symlink test successful")
	})
}
