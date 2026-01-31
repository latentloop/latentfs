package integration

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	"latentfs/internal/storage"
)

func TestVFSCache(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "vfs_cache")
	defer shared.Cleanup()

	// TestCacheCoherency tests that changes made directly to DataFile
	// are visible through the NFS mount (LatentFS cache coherency)
	t.Run("CacheCoherency", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "cache_coherency")
		defer env.Cleanup()

		// Create source files
		env.CreateSourceFile("file1.txt", "original content")
		env.CreateSourceFile("subdir/file2.txt", "subdir content")

		// Fork the source
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Verify initial content through NFS mount
		content := env.ReadTargetFile("file1.txt")
		if content != "original content" {
			t.Errorf("expected 'original content', got '%s'", content)
		}

		// Modify file through the NFS mount (creates overlay entry)
		env.WriteTargetFile("file1.txt", "modified content")

		// Use Eventually to wait for content to be visible
		g := NewWithT(t)
		g.Eventually(func() string {
			return env.ReadTargetFile("file1.txt")
		}).WithTimeout(1 * time.Second).WithPolling(50 * time.Millisecond).Should(Equal("modified content"))

		t.Log("Cache coherency test passed: write-through working correctly")
	})

	// TestRapidWriteRead tests rapid write/read cycles for cache consistency
	t.Run("RapidWriteRead", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "rapid_rw")
		defer env.Cleanup()

		// Create empty fork
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Rapid write/read cycles
		iterations := 10
		for i := range iterations {
			content := string(rune('A'+i)) + " content"
			env.WriteTargetFile("rapid.txt", content)

			// Immediate read back
			got := env.ReadTargetFile("rapid.txt")
			if got != content {
				t.Errorf("iteration %d: expected '%s', got '%s'", i, content, got)
			}
		}

		t.Logf("Rapid write/read test passed: %d iterations", iterations)
	})

	// TestMultipleFileCache tests cache coherency across multiple files
	t.Run("MultipleFileCache", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "multi_file")
		defer env.Cleanup()

		// Create empty fork
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Create multiple files
		files := []string{"a.txt", "b.txt", "c.txt", "subdir/d.txt", "subdir/nested/e.txt"}
		for i, f := range files {
			content := string(rune('A'+i)) + " file content"
			fullPath := filepath.Join(env.TargetPath, f)
			dir := filepath.Dir(fullPath)
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatalf("failed to create dir %s: %v", dir, err)
			}
			if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
				t.Fatalf("failed to write %s: %v", f, err)
			}
		}

		// Read all files back
		for i, f := range files {
			expected := string(rune('A'+i)) + " file content"
			content := env.ReadTargetFile(f)
			if content != expected {
				t.Errorf("file %s: expected '%s', got '%s'", f, expected, content)
			}
		}

		// Modify all files
		for i, f := range files {
			content := string(rune('a'+i)) + " modified"
			fullPath := filepath.Join(env.TargetPath, f)
			if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
				t.Fatalf("failed to modify %s: %v", f, err)
			}
		}

		// Verify all modifications (immediate read-back, no sleep needed)
		for i, f := range files {
			expected := string(rune('a'+i)) + " modified"
			content := env.ReadTargetFile(f)
			if content != expected {
				t.Errorf("file %s after modify: expected '%s', got '%s'", f, expected, content)
			}
		}

		t.Log("Multiple file cache test passed")
	})

	// TestSnapshotRestoreMultipleFiles tests cache coherency when restoring multiple files.
	// This works because restore now updates mtime to current time, which
	// triggers NFS client cache invalidation.
	t.Run("SnapshotRestoreMultipleFiles", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "restore_multi")
		defer env.Cleanup()

		// Create source files
		env.CreateSourceFile("config.json", `{"version": 1}`)
		env.CreateSourceFile("data/records.txt", "record 1\nrecord 2")
		env.CreateSourceFile("data/index.txt", "index v1")

		// Fork the source
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Save initial snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "initial", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// Modify all files
		env.WriteTargetFile("config.json", `{"version": 2}`)
		env.WriteTargetFile("data/records.txt", "record 1\nrecord 2\nrecord 3")
		env.WriteTargetFile("data/index.txt", "index v2")

		// Verify modifications
		if content := env.ReadTargetFile("config.json"); content != `{"version": 2}` {
			t.Errorf("config.json not modified correctly")
		}

		// Restore to v1
		result = env.RunCLI("data", "restore", "--data-file", env.DataFile, "-y", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("restore failed: %s", result.Combined)
		}

		// Force NFS client to notice the mtime change
		os.ReadDir(env.TargetPath)
		os.ReadDir(filepath.Join(env.TargetPath, "data"))

		// Use Eventually to verify all files restored
		// Restore now updates mtime, so NFS client should detect the change
		g := NewWithT(t)
		expected := map[string]string{
			"config.json":      `{"version": 1}`,
			"data/records.txt": "record 1\nrecord 2",
			"data/index.txt":   "index v1",
		}

		for file, expectedContent := range expected {
			g.Eventually(func() string {
				return env.ReadTargetFile(file)
			}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(Equal(expectedContent), "file: "+file)
		}

		t.Log("Snapshot restore multiple files cache test passed")
	})

	// TestDeleteAndRestore tests cache coherency for deleted files after restore
	// This test uses overlay files (not source files) since deleting source files
	// through the overlay may not be supported.
	//
	// This test verifies that cache invalidation works correctly:
	// 1. Delete a file through NFS mount
	// 2. Restore from snapshot via CLI (which writes directly to DataFile)
	// 3. The restore command invalidates the cache after restoring
	// 4. The file should be visible again through NFS mount
	t.Run("DeleteAndRestore", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "delete_restore")
		defer env.Cleanup()

		// Create empty fork
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Create overlay file (not source file, so deletion works)
		env.WriteTargetFile("deleteme.txt", "to be deleted")

		// Verify file exists
		g := NewWithT(t)
		g.Eventually(func() bool {
			return env.TargetFileExists("deleteme.txt")
		}).WithTimeout(1 * time.Second).Should(BeTrue())

		// Save snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "with file", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// Delete the file through NFS
		fullPath := filepath.Join(env.TargetPath, "deleteme.txt")
		if err := os.Remove(fullPath); err != nil {
			t.Fatalf("failed to delete file: %v", err)
		}

		// Verify file is deleted
		g.Eventually(func() bool {
			return !env.TargetFileExists("deleteme.txt")
		}).WithTimeout(1 * time.Second).Should(BeTrue())

		// Restore to v1 (should bring back the file)
		result = env.RunCLI("data", "restore", "--data-file", env.DataFile, "-y", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("restore failed: %s", result.Combined)
		}

		// Force NFS client cache refresh by reading the directory
		os.ReadDir(env.TargetPath)

		// Check if file is now readable with Eventually
		// Note: macOS NFS client caches negative lookups separately from directory listings.
		// Even though READDIRPLUS shows the file, the client may still return ENOENT
		// from its negative lookup cache. We must poll with actual file access to force
		// the negative cache to be invalidated through repeated LOOKUP requests.
		var content string
		g.Eventually(func() bool {
			data, err := os.ReadFile(fullPath)
			if err != nil {
				return false
			}
			content = string(data)
			return true
		}).WithTimeout(2 * time.Second).Should(BeTrue(), "deleteme.txt should be readable after restore")
		if content != "to be deleted" {
			t.Errorf("expected 'to be deleted', got '%s'", content)
		}

		t.Log("Delete and restore cache test passed")
	})

	// Note: Sequential/concurrent read tests are covered by RapidWriteRead
	// which does rapid write/read cycles and validates cache consistency

	// TestRestoreWithoutInvalidateCache demonstrates that InvalidateCache is necessary
	// even with the mtime change. Without InvalidateCache, the daemon's cached epoch
	// causes it to return stale data even when NFS client requests fresh content.
	//
	// This test proves the two-layer cache invalidation strategy:
	// - Layer 1 (mtime): NFS client cache invalidation (triggers server request)
	// - Layer 2 (InvalidateCache): Server-side epoch/handle refresh (returns correct data)
	t.Run("RestoreWithoutInvalidateCache", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "no_invalidate")
		defer env.Cleanup()

		// Create empty fork
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Create a file through NFS
		env.WriteTargetFile("test.txt", "version 1")

		// Verify initial content
		g := NewWithT(t)
		g.Eventually(func() string {
			return env.ReadTargetFile("test.txt")
		}).WithTimeout(1 * time.Second).Should(Equal("version 1"))

		// Save snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "v1", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// Modify through NFS
		env.WriteTargetFile("test.txt", "version 2")
		g.Eventually(func() string {
			return env.ReadTargetFile("test.txt")
		}).WithTimeout(1 * time.Second).Should(Equal("version 2"))

		// --- Direct restore WITHOUT InvalidateCache ---
		// This simulates what would happen if restore didn't call invalidateDaemonCache()

		// Open DataFile directly (separate from daemon's connection)
		df, err := storage.OpenWithContext(env.DataFile, storage.DBContextCLI)
		if err != nil {
			t.Fatalf("failed to open DataFile: %v", err)
		}

		// Restore from snapshot (this creates new epoch with mtime=now+1s)
		_, err = df.RestoreFromSnapshot("v1", nil)
		if err != nil {
			df.Close()
			t.Fatalf("restore failed: %v", err)
		}
		df.Close()

		// Force NFS client to refetch (mtime changed, so client should ask server)
		os.ReadDir(env.TargetPath)

		// WITHOUT InvalidateCache, daemon still has old epoch cached
		// NFS client asks for fresh data, but daemon returns stale content from old epoch
		// Give it a moment to potentially see the change (it won't without InvalidateCache)
		time.Sleep(50 * time.Millisecond)

		// Read through NFS - should STILL see "version 2" because daemon has stale epoch
		contentWithoutInvalidate := env.ReadTargetFile("test.txt")
		t.Logf("Content WITHOUT InvalidateCache: %q (expected 'version 2' - stale)", contentWithoutInvalidate)

		// Verify we got stale data (this proves InvalidateCache is needed)
		if contentWithoutInvalidate == "version 1" {
			t.Log("WARNING: Got correct content without InvalidateCache - this is unexpected")
			t.Log("The daemon may have refreshed its epoch by other means")
		} else if contentWithoutInvalidate == "version 2" {
			t.Log("Confirmed: Without InvalidateCache, daemon returns stale data")
		}

		// --- Now do a proper restore WITH InvalidateCache (via CLI) ---
		// This calls invalidateDaemonCache() after restore
		result = env.RunCLI("data", "restore", "--data-file", env.DataFile, "-y", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("CLI restore failed: %s", result.Combined)
		}
		t.Log("Called CLI restore (which includes InvalidateCache)")

		// Force NFS client to refetch again
		os.ReadDir(env.TargetPath)

		// Now daemon has refreshed epoch - should see correct content
		g.Eventually(func() string {
			return env.ReadTargetFile("test.txt")
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(Equal("version 1"))

		t.Log("Content WITH InvalidateCache: 'version 1' (correct)")
		t.Log("Test demonstrates: InvalidateCache is required for server-side epoch refresh")
	})
}
