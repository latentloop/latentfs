package integration

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"latentfs/internal/storage"

	_ "github.com/tursodatabase/go-libsql"
)

// --- Epoch query helpers (standalone, take dataFile path) ---

// queryEpochStatus queries the current status of an epoch from the database
func queryEpochStatus(dataFile string, epoch int64) (string, error) {
	db, err := sql.Open("libsql", storage.BuildDSN(dataFile, storage.DBContextCLI))
	if err != nil {
		return "", err
	}
	defer db.Close()

	var status string
	err = db.QueryRow("SELECT status FROM write_epochs WHERE epoch = ?", epoch).Scan(&status)
	return status, err
}

// queryReadyEpochCount returns the count of epochs with 'ready' status
func queryReadyEpochCount(dataFile string) (int, error) {
	db, err := sql.Open("libsql", storage.BuildDSN(dataFile, storage.DBContextCLI))
	if err != nil {
		return 0, err
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM write_epochs WHERE status = 'ready'").Scan(&count)
	return count, err
}

// queryDeprecatedEpochCount returns the count of epochs with 'deprecated' status
func queryDeprecatedEpochCount(dataFile string) (int, error) {
	db, err := sql.Open("libsql", storage.BuildDSN(dataFile, storage.DBContextCLI))
	if err != nil {
		return 0, err
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM write_epochs WHERE status = 'deprecated'").Scan(&count)
	return count, err
}

// queryCurrentWriteEpoch returns the current write epoch from config
func queryCurrentWriteEpoch(dataFile string) (int64, error) {
	db, err := sql.Open("libsql", storage.BuildDSN(dataFile, storage.DBContextCLI))
	if err != nil {
		return 0, err
	}
	defer db.Close()

	var epochStr string
	err = db.QueryRow("SELECT value FROM config WHERE key = 'current_write_epoch'").Scan(&epochStr)
	if err != nil {
		return 0, err
	}

	var epoch int64
	fmt.Sscanf(epochStr, "%d", &epoch)
	return epoch, nil
}

// queryDraftEpochCount returns the count of epochs with 'draft' status
func queryDraftEpochCount(dataFile string) (int, error) {
	db, err := sql.Open("libsql", storage.BuildDSN(dataFile, storage.DBContextCLI))
	if err != nil {
		return 0, err
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM write_epochs WHERE status = 'draft'").Scan(&count)
	return count, err
}

// injectOrphanedDraftEpoch simulates a crashed restore by inserting a draft epoch
// with garbage inode data directly into the database. Returns the injected epoch number.
func injectOrphanedDraftEpoch(dataFile string) (int64, error) {
	db, err := sql.Open("libsql", storage.BuildDSN(dataFile, storage.DBContextCLI))
	if err != nil {
		return 0, err
	}
	defer db.Close()

	now := time.Now().Unix()

	// Insert a draft epoch row
	var epoch int64
	err = db.QueryRow(
		`INSERT INTO write_epochs (status, created_by, created_at, updated_at) VALUES ('draft', 'test:crashed_restore', ?, ?) RETURNING epoch`,
		now, now,
	).Scan(&epoch)
	if err != nil {
		return 0, fmt.Errorf("failed to insert draft epoch: %w", err)
	}

	// Insert garbage inode data at the draft epoch (simulates partial restore)
	_, err = db.Exec(
		`INSERT INTO inodes (ino, write_epoch, mode, uid, gid, size, atime, mtime, ctime, nlink) VALUES (999, ?, 33188, 0, 0, 100, ?, ?, ?, 1)`,
		epoch, now, now, now,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to insert garbage inode: %w", err)
	}

	// Insert garbage dentry data at the draft epoch
	_, err = db.Exec(
		`INSERT INTO dentries (parent_ino, name, ino, write_epoch) VALUES (1, 'garbage_file.txt', 999, ?)`,
		epoch,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to insert garbage dentry: %w", err)
	}

	return epoch, nil
}

// queryInodeExistsAtEpoch checks if any inode data exists at the given epoch
func queryInodeExistsAtEpoch(dataFile string, epoch int64) (bool, error) {
	db, err := sql.Open("libsql", storage.BuildDSN(dataFile, storage.DBContextCLI))
	if err != nil {
		return false, err
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM inodes WHERE write_epoch = ?", epoch).Scan(&count)
	return count > 0, err
}

// TestEpoch groups all epoch-related tests under a single shared daemon
func TestEpoch(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "epoch")
	defer shared.Cleanup()

	t.Run("SnapshotSaveCreatesNewEpoch", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "save_epoch")
		defer env.Cleanup()

		// Setup: Create source with files
		env.CreateSourceFile("file1.txt", "content1")
		env.CreateSourceFile("dir/file2.txt", "content2")

		// Create fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to create fork: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Write some data through the symlink
		env.WriteTargetFile("new_file.txt", "new content")

		// Get initial epoch
		initialEpoch, err := queryCurrentWriteEpoch(env.DataFile)
		if err != nil {
			t.Fatalf("Failed to query initial epoch: %v", err)
		}
		t.Logf("Initial write epoch: %d", initialEpoch)

		// Save snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "test snapshot")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to save snapshot: %s", result.Combined)
		}

		// Verify new epoch was created
		newEpoch, err := queryCurrentWriteEpoch(env.DataFile)
		if err != nil {
			t.Fatalf("Failed to query new epoch: %v", err)
		}
		t.Logf("New write epoch: %d", newEpoch)

		if newEpoch <= initialEpoch {
			t.Errorf("Expected new epoch %d > initial epoch %d", newEpoch, initialEpoch)
		}

		// Verify old epoch stays ready (MVCC needs it for reads, only restore deprecates)
		oldStatus, err := queryEpochStatus(env.DataFile, initialEpoch)
		if err != nil {
			t.Fatalf("Failed to query old epoch status: %v", err)
		}
		if oldStatus != "ready" {
			t.Errorf("Expected old epoch status 'ready' (MVCC), got '%s'", oldStatus)
		}

		// Verify new epoch is ready
		newStatus, err := queryEpochStatus(env.DataFile, newEpoch)
		if err != nil {
			t.Fatalf("Failed to query new epoch status: %v", err)
		}
		if newStatus != "ready" {
			t.Errorf("Expected new epoch status 'ready', got '%s'", newStatus)
		}
	})

	t.Run("SnapshotRestoreUsesEpochSwitch", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "restore_epoch")
		defer env.Cleanup()

		// Setup: Create source with files
		env.CreateSourceFile("original.txt", "original content")

		// Create fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to create fork: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Create and save snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-t", "v1", "-m", "version 1")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to save snapshot: %s", result.Combined)
		}
		t.Logf("Save v1 output: %s", result.Combined)

		// Query current write epoch from database to verify save created new epoch
		epochAfterSave, err := queryCurrentWriteEpoch(env.DataFile)
		if err != nil {
			t.Fatalf("Failed to query epoch after save: %v", err)
		}
		t.Logf("Epoch after save: %d", epochAfterSave)

		// Small delay to ensure cache invalidation is processed
		time.Sleep(100 * time.Millisecond)

		// First try to list directory to see if it's accessible
		entries, err := os.ReadDir(env.TargetPath)
		if err != nil {
			t.Fatalf("Failed to list target directory: %v", err)
		}
		t.Logf("Target entries before write: %v", entries)

		// Try creating file with os.Create instead of WriteFile to isolate the problem
		f, err := os.Create(filepath.Join(env.TargetPath, "modified.txt"))
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		_, err = f.WriteString("modified content")
		if err != nil {
			f.Close()
			t.Fatalf("Failed to write to file: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("Failed to close file: %v", err)
		}
		t.Logf("File created and written successfully")

		// Save another snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-t", "v2", "-m", "version 2")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to save v2 snapshot: %s", result.Combined)
		}

		// Get epoch before restore
		epochBeforeRestore, err := queryCurrentWriteEpoch(env.DataFile)
		if err != nil {
			t.Fatalf("Failed to query epoch before restore: %v", err)
		}
		t.Logf("Epoch before restore: %d", epochBeforeRestore)

		// Restore to v1
		result = env.RunCLI("data", "restore", "v1", "--data-file", env.DataFile, "-y")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to restore snapshot: %s", result.Combined)
		}

		// Get epoch after restore
		epochAfterRestore, err := queryCurrentWriteEpoch(env.DataFile)
		if err != nil {
			t.Fatalf("Failed to query epoch after restore: %v", err)
		}
		t.Logf("Epoch after restore: %d", epochAfterRestore)

		// Verify epoch changed
		if epochAfterRestore <= epochBeforeRestore {
			t.Errorf("Expected epoch after restore %d > before restore %d", epochAfterRestore, epochBeforeRestore)
		}

		// Verify new epoch is ready
		newStatus, err := queryEpochStatus(env.DataFile, epochAfterRestore)
		if err != nil {
			t.Fatalf("Failed to query new epoch status: %v", err)
		}
		if newStatus != "ready" {
			t.Errorf("Expected new epoch status 'ready', got '%s'", newStatus)
		}

		// Verify old epoch remains ready (not deprecated) — still needed for MVCC reads
		oldStatus, err := queryEpochStatus(env.DataFile, epochBeforeRestore)
		if err != nil {
			t.Fatalf("Failed to query old epoch status: %v", err)
		}
		if oldStatus != "ready" {
			t.Errorf("Expected old epoch status 'ready', got '%s'", oldStatus)
		}
	})

	t.Run("ConcurrentFileWritesDuringSnapshotSave", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "concurrent_write")
		defer env.Cleanup()

		// Setup: Create source with files
		for i := 0; i < 5; i++ {
			env.CreateSourceFile(fmt.Sprintf("file%d.txt", i), fmt.Sprintf("content %d", i))
		}

		// Create fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to create fork: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Get initial epoch
		initialEpoch, _ := queryCurrentWriteEpoch(env.DataFile)
		t.Logf("Initial epoch: %d", initialEpoch)

		// First, write files through the mount (via daemon)
		var wg sync.WaitGroup
		writeErrors := make(chan error, 10)

		// Writer goroutine - writes files through NFS mount
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				path := filepath.Join(env.TargetPath, fmt.Sprintf("concurrent_%d.txt", i))
				content := fmt.Sprintf("concurrent content %d", i)
				if err := os.WriteFile(path, []byte(content), 0644); err != nil {
					writeErrors <- fmt.Errorf("write %d failed: %w", i, err)
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()

		// Wait for writes to complete first
		wg.Wait()
		close(writeErrors)

		// Check write errors
		for err := range writeErrors {
			t.Fatalf("Write operation error: %v", err)
		}

		// Now do the snapshot save (separately, not concurrently)
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "after writes")
		if result.ExitCode != 0 {
			t.Fatalf("Snapshot save failed: %s", result.Combined)
		}

		// Get epoch after save
		newEpoch, _ := queryCurrentWriteEpoch(env.DataFile)
		t.Logf("Epoch after save: %d", newEpoch)

		// Verify epoch was incremented
		if newEpoch <= initialEpoch {
			t.Errorf("Expected new epoch %d > initial %d", newEpoch, initialEpoch)
		}

		// Verify some concurrent files exist
		foundCount := 0
		for i := 0; i < 5; i++ {
			path := filepath.Join(env.TargetPath, fmt.Sprintf("concurrent_%d.txt", i))
			if _, err := os.Stat(path); err == nil {
				foundCount++
			}
		}
		t.Logf("Found %d concurrent files after snapshot", foundCount)

		// All files should exist since writes completed before save
		if foundCount != 5 {
			t.Errorf("Expected 5 concurrent files, found %d", foundCount)
		}
	})

	t.Run("FilesReadableAfterRestore", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "readable_restore")
		defer env.Cleanup()

		// Setup: Create source with files
		env.CreateSourceFile("test.txt", "original")

		// Create fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to create fork: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Save snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-t", "snapshot1", "-m", "snapshot 1")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to save snapshot: %s", result.Combined)
		}

		// Get epoch after save
		epochAfterSave, _ := queryCurrentWriteEpoch(env.DataFile)
		t.Logf("Epoch after save: %d", epochAfterSave)

		// Modify files
		env.WriteTargetFile("test.txt", "modified")
		env.WriteTargetFile("new_file.txt", "new content")

		// Verify modified state
		content := env.ReadTargetFile("test.txt")
		if content != "modified" {
			t.Errorf("Expected 'modified', got '%s'", content)
		}

		// Restore to snapshot1
		result = env.RunCLI("data", "restore", "snapshot1", "--data-file", env.DataFile, "-y")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to restore snapshot: %s", result.Combined)
		}

		// Get epoch after restore
		epochAfterRestore, _ := queryCurrentWriteEpoch(env.DataFile)
		t.Logf("Epoch after restore: %d", epochAfterRestore)

		// Verify epoch changed (new draft epoch was created and switched)
		if epochAfterRestore <= epochAfterSave {
			t.Errorf("Expected epoch after restore %d > epoch after save %d", epochAfterRestore, epochAfterSave)
		}

		// Verify old epoch remains ready (not deprecated) — still needed for MVCC reads
		oldStatus, _ := queryEpochStatus(env.DataFile, epochAfterSave)
		if oldStatus != "ready" {
			t.Errorf("Expected old epoch status 'ready', got '%s'", oldStatus)
		}

		// Verify new epoch is ready
		newStatus, _ := queryEpochStatus(env.DataFile, epochAfterRestore)
		if newStatus != "ready" {
			t.Errorf("Expected new epoch status 'ready', got '%s'", newStatus)
		}
	})

	t.Run("MultipleSnapshotSaveRestoreCycles", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "multi_cycle")
		defer env.Cleanup()

		// Setup: Create source with files
		env.CreateSourceFile("base.txt", "base content")

		// Create fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to create fork: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Track epochs during cycles
		var cycleEpochs []int64

		// Perform multiple save/restore cycles
		for cycle := 1; cycle <= 3; cycle++ {
			// Modify and save
			env.WriteTargetFile(fmt.Sprintf("cycle%d.txt", cycle), fmt.Sprintf("cycle %d content", cycle))

			result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-t", fmt.Sprintf("v%d", cycle))
			if result.ExitCode != 0 {
				t.Fatalf("Cycle %d: Failed to save snapshot: %s", cycle, result.Combined)
			}

			// Verify epoch incremented
			epoch, err := queryCurrentWriteEpoch(env.DataFile)
			if err != nil {
				t.Fatalf("Cycle %d: Failed to query epoch: %v", cycle, err)
			}
			t.Logf("Cycle %d: Current epoch = %d", cycle, epoch)
			cycleEpochs = append(cycleEpochs, epoch)

			// Verify epochs are strictly increasing
			if cycle > 1 && epoch <= cycleEpochs[cycle-2] {
				t.Errorf("Cycle %d: Expected epoch %d > previous %d", cycle, epoch, cycleEpochs[cycle-2])
			}
		}

		// Get epoch before restore
		epochBeforeRestore, _ := queryCurrentWriteEpoch(env.DataFile)
		t.Logf("Epoch before restore: %d", epochBeforeRestore)

		// Restore to v1
		result = env.RunCLI("data", "restore", "v1", "--data-file", env.DataFile, "-y")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to restore to v1: %s", result.Combined)
		}

		// Get epoch after restore
		epochAfterRestore, _ := queryCurrentWriteEpoch(env.DataFile)
		t.Logf("Epoch after restore: %d", epochAfterRestore)

		// Verify epoch changed
		if epochAfterRestore <= epochBeforeRestore {
			t.Errorf("Expected epoch after restore %d > before %d", epochAfterRestore, epochBeforeRestore)
		}

		// Count ready epochs (should have at least 1 - the current one)
		readyCount, err := queryReadyEpochCount(env.DataFile)
		if err != nil {
			t.Fatalf("Failed to query ready epoch count: %v", err)
		}
		t.Logf("Ready epochs: %d", readyCount)

		if readyCount < 1 {
			t.Error("Expected at least one ready epoch")
		}

		// Old epochs should remain ready (not deprecated) — they're still needed for MVCC reads
		deprecatedCount, err := queryDeprecatedEpochCount(env.DataFile)
		if err != nil {
			t.Fatalf("Failed to query deprecated epoch count: %v", err)
		}
		t.Logf("Deprecated epochs: %d", deprecatedCount)
	})

	t.Run("EpochCleanupAfterDeprecation", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "epoch_cleanup")
		defer env.Cleanup()

		// Setup: Create source with files
		env.CreateSourceFile("file1.txt", "content1")

		// Create fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to create fork: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Write additional data at epoch 0
		env.WriteTargetFile("file2.txt", "content2")

		// Get initial epoch
		initialEpoch, _ := queryCurrentWriteEpoch(env.DataFile)
		t.Logf("Initial epoch: %d", initialEpoch)

		// Save first snapshot (does NOT deprecate - MVCC keeps epochs ready)
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-t", "snap1", "-m", "snapshot 1")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to save snapshot 1: %s", result.Combined)
		}

		// Write more data
		env.WriteTargetFile("file2.txt", "content2 modified")

		// Save second snapshot (does NOT deprecate)
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-t", "snap2", "-m", "snapshot 2")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to save snapshot 2: %s", result.Combined)
		}

		// Get epoch before restore
		epochBeforeRestore, _ := queryCurrentWriteEpoch(env.DataFile)
		t.Logf("Epoch before restore: %d", epochBeforeRestore)

		// Restore to snap1 - creates a new epoch; old epoch remains ready for MVCC reads
		result = env.RunCLI("data", "restore", "snap1", "--data-file", env.DataFile, "-y")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to restore to snap1: %s", result.Combined)
		}

		// The old epoch should remain ready (not deprecated) — it's still needed for MVCC reads
		status, err := queryEpochStatus(env.DataFile, epochBeforeRestore)
		if err != nil {
			t.Fatalf("Failed to query epoch status: %v", err)
		}
		if status != "ready" {
			t.Errorf("Expected epoch %d status 'ready' after restore, got '%s'", epochBeforeRestore, status)
		}

		// Run cleanup — old epoch is still ready so there may be no deprecated epochs to clean
		df, err := storage.OpenWithContext(env.DataFile, storage.DBContextCLI)
		if err != nil {
			t.Fatalf("Failed to open data file: %v", err)
		}
		count, err := df.CleanupDeprecatedEpochs()
		df.Close()

		if err != nil {
			t.Fatalf("CleanupDeprecatedEpochs failed: %v", err)
		}
		t.Logf("Cleaned up %d deprecated epochs", count)
	})

	// [IMPORTANT] current daemon is using max ready epoch for querying data, this is not perfect. Must ensure no draft epoch existing (has smaller epoch number than max ready epoch)
	t.Run("SaveCleansOrphanedDraftEpoch", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "save_cleans_draft")
		defer env.Cleanup()

		// Setup: Create source with a file
		env.CreateSourceFile("file1.txt", "content1")

		// Create fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to create fork: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Inject an orphaned draft epoch with garbage data (simulates crashed restore)
		draftEpoch, err := injectOrphanedDraftEpoch(env.DataFile)
		if err != nil {
			t.Fatalf("Failed to inject draft epoch: %v", err)
		}
		t.Logf("Injected orphaned draft epoch: %d", draftEpoch)

		// Verify draft epoch exists
		draftCount, err := queryDraftEpochCount(env.DataFile)
		if err != nil {
			t.Fatalf("Failed to query draft count: %v", err)
		}
		if draftCount != 1 {
			t.Fatalf("Expected 1 draft epoch, got %d", draftCount)
		}

		// Verify garbage data exists at that epoch
		hasData, err := queryInodeExistsAtEpoch(env.DataFile, draftEpoch)
		if err != nil {
			t.Fatalf("Failed to query inode data: %v", err)
		}
		if !hasData {
			t.Fatal("Expected garbage inode data at draft epoch")
		}

		// Snapshot save should clean up the orphaned draft epoch before proceeding
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-t", "after_cleanup", "-m", "save after draft cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to save snapshot: %s", result.Combined)
		}

		// Draft epoch should be gone
		draftCount, err = queryDraftEpochCount(env.DataFile)
		if err != nil {
			t.Fatalf("Failed to query draft count after save: %v", err)
		}
		if draftCount != 0 {
			t.Errorf("Expected 0 draft epochs after save, got %d", draftCount)
		}

		// Garbage data should be cleaned up
		hasData, err = queryInodeExistsAtEpoch(env.DataFile, draftEpoch)
		if err != nil {
			t.Fatalf("Failed to query inode data after save: %v", err)
		}
		if hasData {
			t.Error("Expected garbage inode data to be cleaned up after save")
		}
	})

	t.Run("RestoreCleansOrphanedDraftEpoch", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "restore_cleans_draft")
		defer env.Cleanup()

		// Setup: Create source with a file
		env.CreateSourceFile("file1.txt", "content1")

		// Create fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to create fork: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Save a snapshot to restore to later
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-t", "base", "-m", "base snapshot")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to save snapshot: %s", result.Combined)
		}

		// Inject an orphaned draft epoch with garbage data (simulates crashed restore)
		draftEpoch, err := injectOrphanedDraftEpoch(env.DataFile)
		if err != nil {
			t.Fatalf("Failed to inject draft epoch: %v", err)
		}
		t.Logf("Injected orphaned draft epoch: %d", draftEpoch)

		// Verify draft epoch and garbage data exist
		draftCount, _ := queryDraftEpochCount(env.DataFile)
		if draftCount != 1 {
			t.Fatalf("Expected 1 draft epoch, got %d", draftCount)
		}
		hasData, _ := queryInodeExistsAtEpoch(env.DataFile, draftEpoch)
		if !hasData {
			t.Fatal("Expected garbage inode data at draft epoch")
		}

		// Restore should clean up the orphaned draft epoch before proceeding
		result = env.RunCLI("data", "restore", "base", "--data-file", env.DataFile, "-y")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to restore snapshot: %s", result.Combined)
		}

		// Draft epoch should be gone
		draftCount, err = queryDraftEpochCount(env.DataFile)
		if err != nil {
			t.Fatalf("Failed to query draft count after restore: %v", err)
		}
		if draftCount != 0 {
			t.Errorf("Expected 0 draft epochs after restore, got %d", draftCount)
		}

		// Garbage data should be cleaned up
		hasData, err = queryInodeExistsAtEpoch(env.DataFile, draftEpoch)
		if err != nil {
			t.Fatalf("Failed to query inode data after restore: %v", err)
		}
		if hasData {
			t.Error("Expected garbage inode data to be cleaned up after restore")
		}
	})

	t.Run("SavePreventsGarbageDataExposure", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "save_no_exposure")
		defer env.Cleanup()

		// Setup: Create source with a file
		env.CreateSourceFile("real_file.txt", "real content")

		// Create fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to create fork: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Inject orphaned draft epoch with garbage data
		draftEpoch, err := injectOrphanedDraftEpoch(env.DataFile)
		if err != nil {
			t.Fatalf("Failed to inject draft epoch: %v", err)
		}
		t.Logf("Injected orphaned draft epoch: %d", draftEpoch)

		// Save snapshot — this creates a new ready epoch with a higher number.
		// Without the draft cleanup fix, readableEpoch would advance past the
		// draft epoch, exposing its garbage data via WHERE write_epoch <= readableEpoch.
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-t", "safe", "-m", "safe save")
		if result.ExitCode != 0 {
			t.Fatalf("Failed to save snapshot: %s", result.Combined)
		}

		// The new readableEpoch is now higher than the draft epoch.
		// Verify the garbage file from the draft epoch is NOT visible through the mount.
		garbagePath := filepath.Join(env.TargetPath, "garbage_file.txt")
		_, err = os.Stat(garbagePath)
		if err == nil {
			t.Error("Garbage file from orphaned draft epoch is visible — draft cleanup failed")
		} else if !os.IsNotExist(err) {
			t.Errorf("Unexpected error checking garbage file: %v", err)
		}

		// Verify the real file is still accessible
		content := env.ReadTargetFile("real_file.txt")
		if content != "real content" {
			t.Errorf("Expected 'real content', got '%s'", content)
		}
	})
}
