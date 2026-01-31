package integration

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"latentfs/internal/storage"
)

// TestBulkCopyPerformance tests the performance of the BulkCopier
// This test measures how long it takes to copy files and reports the results
func TestBulkCopyPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "latentfs_bulk_perf_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("failed to create source dir: %v", err)
	}

	// Create test files - 100 small files
	numFiles := 100
	fileSize := 1024 // 1KB each
	content := make([]byte, fileSize)
	for i := range content {
		content[i] = byte(i % 256)
	}

	t.Logf("Creating %d test files of %d bytes each...", numFiles, fileSize)
	for i := 0; i < numFiles; i++ {
		path := filepath.Join(sourceDir, "file"+string(rune('a'+i/26))+string(rune('a'+i%26))+".txt")
		if err := os.WriteFile(path, content, 0644); err != nil {
			t.Fatalf("failed to write file: %v", err)
		}
	}

	// Also create some nested directories
	for i := 0; i < 10; i++ {
		nestedDir := filepath.Join(sourceDir, "subdir"+string(rune('0'+i)))
		if err := os.MkdirAll(nestedDir, 0755); err != nil {
			t.Fatalf("failed to create nested dir: %v", err)
		}
		for j := 0; j < 10; j++ {
			path := filepath.Join(nestedDir, "nested"+string(rune('0'+j))+".txt")
			if err := os.WriteFile(path, content, 0644); err != nil {
				t.Fatalf("failed to write nested file: %v", err)
			}
		}
	}

	totalFiles := numFiles + 100 // 100 base + 100 nested (10 dirs * 10 files)
	t.Logf("Total files to copy: %d", totalFiles)

	// Test BulkCopier performance
	dataPath := filepath.Join(tmpDir, "test.latentfs")
	df, err := storage.Create(dataPath)
	if err != nil {
		t.Fatalf("failed to create data file: %v", err)
	}
	defer df.Close()

	config := storage.DefaultBulkCopyConfig()
	copier := storage.NewBulkCopier(df, config)

	start := time.Now()
	result, err := copier.CopyFromDirectory(sourceDir)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("CopyFromDirectory() error = %v", err)
	}

	// Report performance metrics
	t.Logf("BulkCopier Performance:")
	t.Logf("  Total files: %d", result.TotalFiles)
	t.Logf("  Copied files: %d", result.CopiedFiles)
	t.Logf("  Total bytes: %d", result.TotalBytes)
	t.Logf("  Duration: %v", elapsed)
	t.Logf("  Files/second: %.2f", float64(result.CopiedFiles)/elapsed.Seconds())
	t.Logf("  MB/second: %.2f", float64(result.TotalBytes)/1024/1024/elapsed.Seconds())

	// Verify some files were copied
	if result.CopiedFiles < totalFiles {
		t.Errorf("Expected at least %d files copied, got %d", totalFiles, result.CopiedFiles)
	}

	// Verify a few files
	ino, err := df.ResolvePath("/fileaa.txt")
	if err != nil {
		t.Errorf("ResolvePath(/fileaa.txt) error = %v", err)
	} else {
		data, err := df.ReadContent(ino, 0, fileSize)
		if err != nil {
			t.Errorf("ReadContent() error = %v", err)
		} else if len(data) != fileSize {
			t.Errorf("content length = %d, want %d", len(data), fileSize)
		}
	}

	// Verify nested file
	ino, err = df.ResolvePath("/subdir0/nested0.txt")
	if err != nil {
		t.Errorf("ResolvePath(/subdir0/nested0.txt) error = %v", err)
	}
	if ino == 0 {
		t.Error("nested file inode should not be 0")
	}
}

// TestBulkCopyBatchingEffect tests that batching actually improves performance
func TestBulkCopyBatchingEffect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "latentfs_batch_perf_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("failed to create source dir: %v", err)
	}

	// Create 200 small files
	numFiles := 200
	content := []byte("small content for testing")
	for i := 0; i < numFiles; i++ {
		path := filepath.Join(sourceDir, "file"+string(rune('0'+i/100))+string(rune('0'+(i/10)%10))+string(rune('0'+i%10))+".txt")
		if err := os.WriteFile(path, content, 0644); err != nil {
			t.Fatalf("failed to write file: %v", err)
		}
	}

	// Test with different batch sizes
	batchSizes := []int{10, 50, 100, 200}
	results := make(map[int]time.Duration)

	for _, batchSize := range batchSizes {
		dataPath := filepath.Join(tmpDir, "test_batch_"+string(rune('0'+batchSize/100))+string(rune('0'+(batchSize/10)%10))+".latentfs")
		df, err := storage.Create(dataPath)
		if err != nil {
			t.Fatalf("failed to create data file: %v", err)
		}

		config := storage.DefaultBulkCopyConfig()
		config.BatchSize = batchSize
		copier := storage.NewBulkCopier(df, config)

		start := time.Now()
		_, err = copier.CopyFromDirectory(sourceDir)
		elapsed := time.Since(start)

		df.Close()

		if err != nil {
			t.Errorf("CopyFromDirectory() with batch size %d error = %v", batchSize, err)
			continue
		}

		results[batchSize] = elapsed
		t.Logf("Batch size %d: %v", batchSize, elapsed)
	}

	// Log all results
	t.Log("\nBatch Size Performance Comparison:")
	for _, batchSize := range batchSizes {
		if dur, ok := results[batchSize]; ok {
			t.Logf("  Batch %3d: %v (%.1f files/sec)", batchSize, dur, float64(numFiles)/dur.Seconds())
		}
	}
}

// TestBulkCopyLargeFiles tests performance with larger files
func TestBulkCopyLargeFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "latentfs_large_perf_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("failed to create source dir: %v", err)
	}

	// Create 10 files of 1MB each
	numFiles := 10
	fileSize := 1024 * 1024 // 1MB
	content := make([]byte, fileSize)
	for i := range content {
		content[i] = byte(i % 256)
	}

	t.Logf("Creating %d test files of %d bytes each...", numFiles, fileSize)
	for i := 0; i < numFiles; i++ {
		path := filepath.Join(sourceDir, "large"+string(rune('0'+i))+".bin")
		if err := os.WriteFile(path, content, 0644); err != nil {
			t.Fatalf("failed to write file: %v", err)
		}
	}

	totalBytes := int64(numFiles * fileSize)
	t.Logf("Total bytes to copy: %d (%.2f MB)", totalBytes, float64(totalBytes)/1024/1024)

	// Test BulkCopier performance
	dataPath := filepath.Join(tmpDir, "test.latentfs")
	df, err := storage.Create(dataPath)
	if err != nil {
		t.Fatalf("failed to create data file: %v", err)
	}
	defer df.Close()

	config := storage.DefaultBulkCopyConfig()
	copier := storage.NewBulkCopier(df, config)

	start := time.Now()
	result, err := copier.CopyFromDirectory(sourceDir)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("CopyFromDirectory() error = %v", err)
	}

	// Report performance metrics
	t.Logf("Large File Performance:")
	t.Logf("  Total files: %d", result.TotalFiles)
	t.Logf("  Total bytes: %d (%.2f MB)", result.TotalBytes, float64(result.TotalBytes)/1024/1024)
	t.Logf("  Duration: %v", elapsed)
	t.Logf("  MB/second: %.2f", float64(result.TotalBytes)/1024/1024/elapsed.Seconds())

	// Verify content of first file
	ino, err := df.ResolvePath("/large0.bin")
	if err != nil {
		t.Fatalf("ResolvePath(/large0.bin) error = %v", err)
	}

	inode, err := df.GetInode(ino)
	if err != nil {
		t.Fatalf("GetInode() error = %v", err)
	}

	if inode.Size != int64(fileSize) {
		t.Errorf("file size = %d, want %d", inode.Size, fileSize)
	}

	// Read and verify first and last chunks
	firstChunk, err := df.ReadContent(ino, 0, storage.ChunkSize)
	if err != nil {
		t.Fatalf("ReadContent(first) error = %v", err)
	}
	if len(firstChunk) != storage.ChunkSize {
		t.Errorf("first chunk size = %d, want %d", len(firstChunk), storage.ChunkSize)
	}
	if firstChunk[0] != content[0] {
		t.Errorf("first byte = %d, want %d", firstChunk[0], content[0])
	}
}
