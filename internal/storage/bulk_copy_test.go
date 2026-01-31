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

package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// testBulkCopyEnv creates a temporary environment for bulk copy testing
func testBulkCopyEnv(t *testing.T) (*DataFile, string, func()) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "latentfs_bulk_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dataPath := filepath.Join(tmpDir, "test.latentfs")
	sourceDir := filepath.Join(tmpDir, "source")

	// Create source directory
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create source dir: %v", err)
	}

	df, err := Create(dataPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create data file: %v", err)
	}

	cleanup := func() {
		df.Close()
		os.RemoveAll(tmpDir)
	}

	return df, sourceDir, cleanup
}

// createTestFile creates a test file in the given directory
func createTestFile(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create parent dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
}

func TestBulkCopier_EmptyDirectory(t *testing.T) {
	df, sourceDir, cleanup := testBulkCopyEnv(t)
	defer cleanup()

	config := DefaultBulkCopyConfig()
	copier := NewBulkCopier(df, config)

	result, err := copier.CopyFromDirectory(sourceDir)
	if err != nil {
		t.Fatalf("CopyFromDirectory() error = %v", err)
	}

	if result.TotalFiles != 0 {
		t.Errorf("TotalFiles = %d, want 0", result.TotalFiles)
	}
	if result.CopiedFiles != 0 {
		t.Errorf("CopiedFiles = %d, want 0", result.CopiedFiles)
	}
}

func TestBulkCopier_SingleFile(t *testing.T) {
	df, sourceDir, cleanup := testBulkCopyEnv(t)
	defer cleanup()

	createTestFile(t, sourceDir, "test.txt", "hello world")

	config := DefaultBulkCopyConfig()
	copier := NewBulkCopier(df, config)

	result, err := copier.CopyFromDirectory(sourceDir)
	if err != nil {
		t.Fatalf("CopyFromDirectory() error = %v", err)
	}

	if result.TotalFiles != 1 {
		t.Errorf("TotalFiles = %d, want 1", result.TotalFiles)
	}
	if result.CopiedFiles != 1 {
		t.Errorf("CopiedFiles = %d, want 1", result.CopiedFiles)
	}

	// Verify file was copied
	ino, err := df.ResolvePath("/test.txt")
	if err != nil {
		t.Fatalf("ResolvePath() error = %v", err)
	}

	content, err := df.ReadContent(ino, 0, 100)
	if err != nil {
		t.Fatalf("ReadContent() error = %v", err)
	}
	if string(content) != "hello world" {
		t.Errorf("content = %q, want %q", string(content), "hello world")
	}
}

func TestBulkCopier_MultipleFiles(t *testing.T) {
	df, sourceDir, cleanup := testBulkCopyEnv(t)
	defer cleanup()

	createTestFile(t, sourceDir, "file1.txt", "content1")
	createTestFile(t, sourceDir, "file2.txt", "content2")
	createTestFile(t, sourceDir, "file3.txt", "content3")

	config := DefaultBulkCopyConfig()
	copier := NewBulkCopier(df, config)

	result, err := copier.CopyFromDirectory(sourceDir)
	if err != nil {
		t.Fatalf("CopyFromDirectory() error = %v", err)
	}

	if result.TotalFiles != 3 {
		t.Errorf("TotalFiles = %d, want 3", result.TotalFiles)
	}

	// Verify all files were copied
	for i := 1; i <= 3; i++ {
		path := filepath.Join("/", "file"+string(rune('0'+i))+".txt")
		ino, err := df.ResolvePath(path)
		if err != nil {
			t.Errorf("ResolvePath(%q) error = %v", path, err)
			continue
		}

		content, err := df.ReadContent(ino, 0, 100)
		if err != nil {
			t.Errorf("ReadContent() error = %v", err)
			continue
		}
		expected := "content" + string(rune('0'+i))
		if string(content) != expected {
			t.Errorf("content = %q, want %q", string(content), expected)
		}
	}
}

func TestBulkCopier_NestedDirectories(t *testing.T) {
	df, sourceDir, cleanup := testBulkCopyEnv(t)
	defer cleanup()

	createTestFile(t, sourceDir, "root.txt", "root content")
	createTestFile(t, sourceDir, "subdir/nested.txt", "nested content")
	createTestFile(t, sourceDir, "subdir/deeper/deep.txt", "deep content")

	config := DefaultBulkCopyConfig()
	copier := NewBulkCopier(df, config)

	result, err := copier.CopyFromDirectory(sourceDir)
	if err != nil {
		t.Fatalf("CopyFromDirectory() error = %v", err)
	}

	// 3 files + 2 directories = 5 items (directories are counted too)
	if result.TotalFiles < 3 {
		t.Errorf("TotalFiles = %d, want at least 3", result.TotalFiles)
	}

	// Verify nested file
	ino, err := df.ResolvePath("/subdir/deeper/deep.txt")
	if err != nil {
		t.Fatalf("ResolvePath() error = %v", err)
	}

	content, err := df.ReadContent(ino, 0, 100)
	if err != nil {
		t.Fatalf("ReadContent() error = %v", err)
	}
	if string(content) != "deep content" {
		t.Errorf("content = %q, want %q", string(content), "deep content")
	}
}

func TestBulkCopier_LargeFile(t *testing.T) {
	df, sourceDir, cleanup := testBulkCopyEnv(t)
	defer cleanup()

	// Create a file larger than ChunkSize (4KB)
	largeContent := make([]byte, ChunkSize*3+100) // ~12KB
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "large.bin"), largeContent, 0644); err != nil {
		t.Fatalf("failed to write large file: %v", err)
	}

	config := DefaultBulkCopyConfig()
	copier := NewBulkCopier(df, config)

	result, err := copier.CopyFromDirectory(sourceDir)
	if err != nil {
		t.Fatalf("CopyFromDirectory() error = %v", err)
	}

	if result.TotalBytes != int64(len(largeContent)) {
		t.Errorf("TotalBytes = %d, want %d", result.TotalBytes, len(largeContent))
	}

	// Verify content
	ino, err := df.ResolvePath("/large.bin")
	if err != nil {
		t.Fatalf("ResolvePath() error = %v", err)
	}

	content, err := df.ReadContent(ino, 0, len(largeContent))
	if err != nil {
		t.Fatalf("ReadContent() error = %v", err)
	}

	if len(content) != len(largeContent) {
		t.Errorf("content length = %d, want %d", len(content), len(largeContent))
	}

	// Verify first and last bytes
	if content[0] != largeContent[0] {
		t.Errorf("first byte = %d, want %d", content[0], largeContent[0])
	}
	if content[len(content)-1] != largeContent[len(largeContent)-1] {
		t.Errorf("last byte = %d, want %d", content[len(content)-1], largeContent[len(largeContent)-1])
	}
}

func TestBulkCopier_SkipsHiddenFiles(t *testing.T) {
	df, sourceDir, cleanup := testBulkCopyEnv(t)
	defer cleanup()

	createTestFile(t, sourceDir, "visible.txt", "visible")
	createTestFile(t, sourceDir, ".hidden", "hidden")
	createTestFile(t, sourceDir, "._appleDouble", "apple double")

	config := DefaultBulkCopyConfig()
	config.SkipHidden = true
	copier := NewBulkCopier(df, config)

	result, err := copier.CopyFromDirectory(sourceDir)
	if err != nil {
		t.Fatalf("CopyFromDirectory() error = %v", err)
	}

	if result.TotalFiles != 1 {
		t.Errorf("TotalFiles = %d, want 1 (hidden files should be skipped)", result.TotalFiles)
	}

	// Verify visible file exists
	_, err = df.ResolvePath("/visible.txt")
	if err != nil {
		t.Error("visible.txt should exist")
	}

	// Verify hidden files don't exist
	_, err = df.ResolvePath("/.hidden")
	if err == nil {
		t.Error(".hidden should not exist")
	}
}

func TestBulkCopier_IncludesHiddenFiles(t *testing.T) {
	df, sourceDir, cleanup := testBulkCopyEnv(t)
	defer cleanup()

	createTestFile(t, sourceDir, "visible.txt", "visible")
	createTestFile(t, sourceDir, ".hidden", "hidden")

	config := DefaultBulkCopyConfig()
	config.SkipHidden = false
	copier := NewBulkCopier(df, config)

	result, err := copier.CopyFromDirectory(sourceDir)
	if err != nil {
		t.Fatalf("CopyFromDirectory() error = %v", err)
	}

	if result.TotalFiles != 2 {
		t.Errorf("TotalFiles = %d, want 2 (hidden files should be included)", result.TotalFiles)
	}

	// Verify hidden file exists
	_, err = df.ResolvePath("/.hidden")
	if err != nil {
		t.Error(".hidden should exist when SkipHidden=false")
	}
}

func TestBulkCopier_Symlinks(t *testing.T) {
	df, sourceDir, cleanup := testBulkCopyEnv(t)
	defer cleanup()

	createTestFile(t, sourceDir, "target.txt", "target content")

	// Create symlink
	symlinkPath := filepath.Join(sourceDir, "link.txt")
	if err := os.Symlink("target.txt", symlinkPath); err != nil {
		t.Skipf("symlink creation failed (might not be supported): %v", err)
	}

	config := DefaultBulkCopyConfig()
	copier := NewBulkCopier(df, config)

	result, err := copier.CopyFromDirectory(sourceDir)
	if err != nil {
		t.Fatalf("CopyFromDirectory() error = %v", err)
	}

	if result.TotalFiles < 2 {
		t.Errorf("TotalFiles = %d, want at least 2", result.TotalFiles)
	}

	// Verify symlink
	ino, err := df.ResolvePath("/link.txt")
	if err != nil {
		t.Fatalf("ResolvePath(/link.txt) error = %v", err)
	}

	target, err := df.ReadSymlink(ino)
	if err != nil {
		t.Fatalf("ReadSymlink() error = %v", err)
	}
	if target != "target.txt" {
		t.Errorf("symlink target = %q, want %q", target, "target.txt")
	}
}

func TestBulkCopier_AllowPartial(t *testing.T) {
	df, sourceDir, cleanup := testBulkCopyEnv(t)
	defer cleanup()

	createTestFile(t, sourceDir, "readable.txt", "readable")

	// Create a file with no read permissions
	unreadablePath := filepath.Join(sourceDir, "unreadable.txt")
	if err := os.WriteFile(unreadablePath, []byte("unreadable"), 0000); err != nil {
		t.Fatalf("failed to create unreadable file: %v", err)
	}
	defer os.Chmod(unreadablePath, 0644) // Ensure cleanup can remove it

	config := DefaultBulkCopyConfig()
	config.AllowPartial = true
	copier := NewBulkCopier(df, config)

	result, err := copier.CopyFromDirectory(sourceDir)
	if err != nil {
		t.Fatalf("CopyFromDirectory() with AllowPartial should not error: %v", err)
	}

	// Should have skipped the unreadable file
	if len(result.SkippedFiles) != 1 {
		t.Errorf("SkippedFiles = %d, want 1", len(result.SkippedFiles))
	}

	// readable.txt should still be copied
	_, err = df.ResolvePath("/readable.txt")
	if err != nil {
		t.Error("readable.txt should exist")
	}
}

func TestBulkCopier_BatchSize(t *testing.T) {
	df, sourceDir, cleanup := testBulkCopyEnv(t)
	defer cleanup()

	// Create more files than the batch size
	for i := 0; i < 15; i++ {
		createTestFile(t, sourceDir, "file"+string(rune('a'+i))+".txt", "content")
	}

	config := DefaultBulkCopyConfig()
	config.BatchSize = 5 // Small batch size to test batching
	copier := NewBulkCopier(df, config)

	result, err := copier.CopyFromDirectory(sourceDir)
	if err != nil {
		t.Fatalf("CopyFromDirectory() error = %v", err)
	}

	if result.TotalFiles != 15 {
		t.Errorf("TotalFiles = %d, want 15", result.TotalFiles)
	}
	if result.CopiedFiles != 15 {
		t.Errorf("CopiedFiles = %d, want 15", result.CopiedFiles)
	}
}

func TestBulkCopier_PreservesPermissions(t *testing.T) {
	df, sourceDir, cleanup := testBulkCopyEnv(t)
	defer cleanup()

	// Create file with specific permissions
	filePath := filepath.Join(sourceDir, "executable.sh")
	if err := os.WriteFile(filePath, []byte("#!/bin/bash\necho hello"), 0755); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	config := DefaultBulkCopyConfig()
	copier := NewBulkCopier(df, config)

	_, err := copier.CopyFromDirectory(sourceDir)
	if err != nil {
		t.Fatalf("CopyFromDirectory() error = %v", err)
	}

	// Verify permissions
	ino, err := df.ResolvePath("/executable.sh")
	if err != nil {
		t.Fatalf("ResolvePath() error = %v", err)
	}

	inode, err := df.GetInode(ino)
	if err != nil {
		t.Fatalf("GetInode() error = %v", err)
	}

	// Check that execute bit is preserved (mode & 0111 should be non-zero)
	if inode.Mode&0111 == 0 {
		t.Errorf("execute permission not preserved, mode = %o", inode.Mode)
	}
}

func BenchmarkBulkCopier_ManySmallFiles(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		tmpDir, _ := os.MkdirTemp("", "latentfs_bench_*")
		dataPath := filepath.Join(tmpDir, "test.latentfs")
		sourceDir := filepath.Join(tmpDir, "source")
		os.MkdirAll(sourceDir, 0755)

		// Create 100 small files
		for j := 0; j < 100; j++ {
			os.WriteFile(filepath.Join(sourceDir, "file"+string(rune('0'+j/10))+string(rune('0'+j%10))+".txt"),
				[]byte("small content"), 0644)
		}

		df, _ := Create(dataPath)

		b.StartTimer()

		config := DefaultBulkCopyConfig()
		copier := NewBulkCopier(df, config)
		copier.CopyFromDirectory(sourceDir)

		b.StopTimer()
		df.Close()
		os.RemoveAll(tmpDir)
	}
}

func BenchmarkBulkCopier_FewLargeFiles(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		tmpDir, _ := os.MkdirTemp("", "latentfs_bench_*")
		dataPath := filepath.Join(tmpDir, "test.latentfs")
		sourceDir := filepath.Join(tmpDir, "source")
		os.MkdirAll(sourceDir, 0755)

		// Create 10 large files (100KB each)
		largeContent := make([]byte, 100*1024)
		for j := 0; j < 10; j++ {
			os.WriteFile(filepath.Join(sourceDir, "large"+string(rune('0'+j))+".bin"),
				largeContent, 0644)
		}

		df, _ := Create(dataPath)

		b.StartTimer()

		config := DefaultBulkCopyConfig()
		copier := NewBulkCopier(df, config)
		copier.CopyFromDirectory(sourceDir)

		b.StopTimer()
		df.Close()
		os.RemoveAll(tmpDir)
	}
}
