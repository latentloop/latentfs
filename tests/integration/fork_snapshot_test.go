package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/gomega"
)

func TestForkSnapshot(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "fork_snapshot")
	defer shared.Cleanup()

	t.Run("SaveAndRestore", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "save_restore")
		defer env.Cleanup()

		// Create fork with --source-empty
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Write initial files
		env.WriteTargetFile("file1.txt", "original content 1")
		env.WriteTargetFile("file2.txt", "original content 2")

		// Create a snapshot with save command
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "Initial snapshot", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		t.Logf("Save output: %s", result.Combined)

		// Verify snapshot was created
		if !result.Contains("Created snapshot") {
			t.Error("save should confirm snapshot creation")
		}

		// Modify the files
		env.WriteTargetFile("file1.txt", "modified content 1")
		env.WriteTargetFile("file2.txt", "modified content 2")

		// Verify modifications
		content := env.ReadTargetFile("file1.txt")
		if content != "modified content 1" {
			t.Errorf("Expected modified content, got %s", content)
		}

		// Unmount to release the database lock for restore
		env.RunCLI("unmount", env.TargetPath)
		env.WaitForUnmount(2*time.Second, env.TargetPath)

		// Restore from snapshot (full restore)
		result = env.RunCLI("data", "restore", "--data-file", env.DataFile, "v1", "--yes")
		if result.ExitCode != 0 {
			t.Fatalf("restore failed: %s", result.Combined)
		}

		t.Logf("Restore output: %s", result.Combined)

		// Remount to see restored content
		result = env.RunCLI("mount", env.TargetPath, "-d", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("remount failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Verify files are restored
		content = env.ReadTargetFile("file1.txt")
		if content != "original content 1" {
			t.Errorf("Expected 'original content 1' after restore, got %s", content)
		}

		content = env.ReadTargetFile("file2.txt")
		if content != "original content 2" {
			t.Errorf("Expected 'original content 2' after restore, got %s", content)
		}

		t.Log("Save and restore test successful")
	})

	t.Run("SaveAndRestorePartial", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "save_restore_partial")
		defer env.Cleanup()

		// Create fork with --source-empty
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Write initial files
		env.WriteTargetFile("file1.txt", "original file1")
		env.WriteTargetFile("file2.txt", "original file2")
		env.WriteTargetFile("file3.txt", "original file3")

		// Create a snapshot
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "Snapshot", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// Modify all files
		env.WriteTargetFile("file1.txt", "modified file1")
		env.WriteTargetFile("file2.txt", "modified file2")
		env.WriteTargetFile("file3.txt", "modified file3")

		// Unmount to release database lock for restore
		env.RunCLI("unmount", env.TargetPath)
		env.WaitForUnmount(2*time.Second, env.TargetPath)

		// Restore only file1.txt and file2.txt
		result = env.RunCLI("data", "restore", "--data-file", env.DataFile, "v1", "--yes", "--", "file1.txt", "file2.txt")
		if result.ExitCode != 0 {
			t.Fatalf("partial restore failed: %s", result.Combined)
		}

		t.Logf("Partial restore output: %s", result.Combined)

		// Remount to see restored content
		result = env.RunCLI("mount", env.TargetPath, "-d", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("remount failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Verify file1 and file2 are restored
		content := env.ReadTargetFile("file1.txt")
		if content != "original file1" {
			t.Errorf("file1.txt should be restored, got %s", content)
		}

		content = env.ReadTargetFile("file2.txt")
		if content != "original file2" {
			t.Errorf("file2.txt should be restored, got %s", content)
		}

		// Verify file3 is still modified
		content = env.ReadTargetFile("file3.txt")
		if content != "modified file3" {
			t.Errorf("file3.txt should still be modified, got %s", content)
		}

		t.Log("Save and restore partial test successful")
	})

	t.Run("SnapshotsList", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "snapshots_list")
		defer env.Cleanup()

		// Create fork with --source-empty
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// No snapshots initially
		result = env.RunCLI("data", "ls-saves", "--data-file", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("snapshots list failed: %s", result.Combined)
		}

		if !result.Contains("No snapshots") {
			t.Error("Should show 'No snapshots' when empty")
		}

		// Create files and snapshots
		env.WriteTargetFile("test.txt", "content")

		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "First snapshot", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		env.WriteTargetFile("test.txt", "updated content")

		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "Second snapshot", "-t", "v2")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// List snapshots
		result = env.RunCLI("data", "ls-saves", "--data-file", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("snapshots list failed: %s", result.Combined)
		}

		t.Logf("Snapshots output: %s", result.Combined)

		// Verify both snapshots are listed
		// Output format: snapshot <id> (tag: v1)
		if !result.Contains("tag: v1") {
			t.Error("Should show v1 tag")
		}
		if !result.Contains("tag: v2") {
			t.Error("Should show v2 tag")
		}
		if !result.Contains("First snapshot") {
			t.Error("Should show first snapshot message")
		}
		if !result.Contains("Second snapshot") {
			t.Error("Should show second snapshot message")
		}
		// ls-saves outputs individual snapshots without a count summary,
		// so we just verify both snapshots are present (already checked above)

		t.Log("Snapshots list test successful")
	})

	t.Run("SaveFromInsideMount", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "save_inside_mount")
		defer env.Cleanup()

		// Create fork with --source-empty
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Write a file
		env.WriteTargetFile("test.txt", "content")

		// Create snapshot using --data-file
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "Snapshot via data-file", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save via data-file failed: %s", result.Combined)
		}

		t.Logf("Save output: %s", result.Combined)

		// Verify snapshot was created
		if !result.Contains("Created snapshot") {
			t.Error("save should confirm snapshot creation")
		}

		// Verify can list snapshots
		result = env.RunCLI("data", "ls-saves", "--data-file", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("snapshots list failed: %s", result.Combined)
		}

		if !result.Contains("tag: v1") {
			t.Error("Should show v1 tag")
		}

		t.Log("Save from inside mount test successful")
	})

	t.Run("DuplicateTag", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "duplicate_tag")
		defer env.Cleanup()

		// Create fork with --source-empty
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Create first snapshot with tag
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "First", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("first save failed: %s", result.Combined)
		}

		// Try to create another snapshot with same tag
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "Second", "-t", "v1")
		if result.ExitCode == 0 {
			t.Fatal("save with duplicate tag should fail")
		}

		if !result.Contains("tag") || !result.Contains("exists") {
			t.Errorf("error should mention tag already exists, got: %s", result.Combined)
		}

		t.Log("Duplicate tag test successful")
	})

	t.Run("OverlayForkSavesOnlyOverlay", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "overlay_only")
		defer env.Cleanup()

		// Create source files
		if err := os.WriteFile(filepath.Join(env.SourceDir, "source_file.txt"), []byte("source content"), 0644); err != nil {
			t.Fatal(err)
		}

		// Create overlay (soft) fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Create an overlay file (new file in fork)
		overlayFile := filepath.Join(env.TargetPath, "overlay_file.txt")
		if err := os.WriteFile(overlayFile, []byte("overlay content"), 0644); err != nil {
			t.Fatalf("Failed to create overlay file: %v", err)
		}

		// Create snapshot WITHOUT --full flag - should only include overlay
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "Overlay only", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}
		t.Logf("Save output: %s", result.Combined)

		// Output should NOT mention source folder (overlay-only save)
		if result.Contains("source folder") {
			t.Error("save output should NOT mention 'source folder' for overlay-only save")
		}

		// Restore to verify snapshot contains only overlay files
		result = env.RunCLI("data", "restore", "v1", "--data-file", env.DataFile, "-y")
		if result.ExitCode != 0 {
			t.Fatalf("restore failed: %s", result.Combined)
		}
		t.Logf("Restore output: %s", result.Combined)

		// Verify restore shows only overlay file
		if !result.Contains("overlay_file.txt") {
			t.Error("restore should include overlay_file.txt")
		}
		// Source file should NOT be in the snapshot
		if result.Contains("source_file.txt") {
			t.Error("restore should NOT include source_file.txt for overlay-only save")
		}

		t.Log("Overlay fork saves only overlay test successful")
	})

	t.Run("OverlayForkFullSaveIncludesSource", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "full_snapshot")
		defer env.Cleanup()

		// Create source files
		if err := os.WriteFile(filepath.Join(env.SourceDir, "source_file.txt"), []byte("source content"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(env.SourceDir, "subdir"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(env.SourceDir, "subdir", "nested.txt"), []byte("nested content"), 0644); err != nil {
			t.Fatal(err)
		}

		// Create overlay (soft) fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Verify source files are accessible through mount
		sourceContent, err := os.ReadFile(filepath.Join(env.TargetPath, "source_file.txt"))
		if err != nil {
			t.Fatalf("Failed to read source file through mount: %v", err)
		}
		if string(sourceContent) != "source content" {
			t.Errorf("source_file.txt content = %s, want 'source content'", sourceContent)
		}

		// Create an overlay file (new file in fork)
		overlayFile := filepath.Join(env.TargetPath, "overlay_file.txt")
		if err := os.WriteFile(overlayFile, []byte("overlay content"), 0644); err != nil {
			t.Fatalf("Failed to create overlay file: %v", err)
		}

		// Create snapshot WITH --full flag - should include both source and overlay files
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "-m", "Full snapshot", "-t", "v1", "--full")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}
		t.Logf("Save output: %s", result.Combined)

		// Verify output mentions source folder (indicates full snapshot)
		if !result.Contains("source folder") {
			t.Error("save output should mention 'source folder' for full snapshot")
		}

		// Restore to verify snapshot contains all files
		result = env.RunCLI("data", "restore", "v1", "--data-file", env.DataFile, "-y")
		if result.ExitCode != 0 {
			t.Fatalf("restore failed: %s", result.Combined)
		}
		t.Logf("Restore output: %s", result.Combined)

		// Verify restore output shows all expected files
		if !result.Contains("source_file.txt") {
			t.Error("restore should include source_file.txt")
		}
		if !result.Contains("overlay_file.txt") {
			t.Error("restore should include overlay_file.txt")
		}
		if !result.Contains("subdir") {
			t.Error("restore should include subdir")
		}
		if !result.Contains("nested.txt") {
			t.Error("restore should include nested.txt")
		}

		// Verify file count (should be at least 4 files: overlay, source, subdir, nested)
		if !result.Contains("file(s) restored") {
			t.Error("restore output should show files restored count")
		}

		t.Log("Overlay fork full save includes source test successful")
	})

	t.Run("DataCommandsWithShortcut", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "data_shortcut")
		defer env.Cleanup()

		// Create fork with --source-empty
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Write a file
		env.WriteTargetFile("test.txt", "test content")

		// Test save with -d shortcut (instead of --data-file)
		result = env.RunCLI("data", "save", "-d", env.DataFile, "-m", "Shortcut test", "-t", "shortcut_v1")
		if result.ExitCode != 0 {
			t.Fatalf("save with -d shortcut failed: %s", result.Combined)
		}
		if !result.Contains("Created snapshot") {
			t.Error("save should confirm snapshot creation")
		}
		t.Logf("Save with -d output: %s", result.Combined)

		// Test ls-saves with -d shortcut
		result = env.RunCLI("data", "ls-saves", "-d", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("ls-saves with -d shortcut failed: %s", result.Combined)
		}
		if !result.Contains("shortcut_v1") {
			t.Error("ls-saves should list the snapshot tag")
		}
		t.Logf("ls-saves with -d output: %s", result.Combined)

		// Unmount to release database lock
		env.RunCLI("unmount", env.TargetPath)
		env.WaitForUnmount(2*time.Second, env.TargetPath)

		// Test restore with -d shortcut
		result = env.RunCLI("data", "restore", "shortcut_v1", "-d", env.DataFile, "-y")
		if result.ExitCode != 0 {
			t.Fatalf("restore with -d shortcut failed: %s", result.Combined)
		}
		if !result.Contains("Restored") {
			t.Error("restore should confirm files were restored")
		}
		t.Logf("Restore with -d output: %s", result.Combined)

		t.Log("Data commands with -d shortcut test successful")
	})

	// Test overlay restore handles tombstones correctly
	// This is a regression test for the overlay snapshot restore bug:
	// When a file is deleted (tombstoned) after a snapshot, restoring
	// that snapshot should make the file visible again via anti-tombstones.
	t.Run("OverlayRestoreRecoversTombstonedFiles", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "overlay_restore_tombstone")
		defer env.Cleanup()

		// Create source folder with files
		if err := os.MkdirAll(filepath.Join(env.SourceDir, "subdir"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(env.SourceDir, "file1.txt"), []byte("source file 1"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(env.SourceDir, "subdir", "file2.txt"), []byte("source file 2"), 0644); err != nil {
			t.Fatal(err)
		}

		// Create soft-fork (overlay)
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Verify source files are visible through mount
		if _, err := os.Stat(filepath.Join(env.TargetPath, "file1.txt")); err != nil {
			t.Fatalf("file1.txt should be visible through mount: %v", err)
		}
		if _, err := os.Stat(filepath.Join(env.TargetPath, "subdir", "file2.txt")); err != nil {
			t.Fatalf("subdir/file2.txt should be visible through mount: %v", err)
		}

		// Save overlay snapshot (before any deletions)
		result = env.RunCLI("data", "save", "-d", env.DataFile, "-m", "Before deletions", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}
		t.Logf("Snapshot saved: %s", result.Combined)

		// Delete files from mount (creates tombstones)
		if err := os.Remove(filepath.Join(env.TargetPath, "file1.txt")); err != nil {
			t.Fatalf("failed to delete file1.txt: %v", err)
		}
		if err := os.RemoveAll(filepath.Join(env.TargetPath, "subdir")); err != nil {
			t.Fatalf("failed to delete subdir: %v", err)
		}

		// Verify files are now hidden (tombstoned)
		if _, err := os.Stat(filepath.Join(env.TargetPath, "file1.txt")); !os.IsNotExist(err) {
			t.Error("file1.txt should be hidden after deletion")
		}
		if _, err := os.Stat(filepath.Join(env.TargetPath, "subdir")); !os.IsNotExist(err) {
			t.Error("subdir should be hidden after deletion")
		}

		// Unmount to release database lock for restore
		env.RunCLI("unmount", env.TargetPath)
		env.WaitForUnmount(2*time.Second, env.TargetPath)

		// Restore from snapshot v1 (should create anti-tombstones)
		result = env.RunCLI("data", "restore", "v1", "-d", env.DataFile, "-y")
		if result.ExitCode != 0 {
			t.Fatalf("restore failed: %s", result.Combined)
		}
		t.Logf("Restore output: %s", result.Combined)

		// Remount to see restored state
		result = env.RunCLI("mount", env.TargetPath, "-d", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("remount failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Verify files are visible again (anti-tombstones work)
		if _, err := os.Stat(filepath.Join(env.TargetPath, "file1.txt")); err != nil {
			t.Errorf("file1.txt should be visible after restore: %v", err)
		}
		content, err := os.ReadFile(filepath.Join(env.TargetPath, "file1.txt"))
		if err != nil {
			t.Errorf("failed to read file1.txt: %v", err)
		} else if string(content) != "source file 1" {
			t.Errorf("file1.txt content = %q, want %q", content, "source file 1")
		}

		if _, err := os.Stat(filepath.Join(env.TargetPath, "subdir")); err != nil {
			t.Errorf("subdir should be visible after restore: %v", err)
		}
		if _, err := os.Stat(filepath.Join(env.TargetPath, "subdir", "file2.txt")); err != nil {
			t.Errorf("subdir/file2.txt should be visible after restore: %v", err)
		}
		content, err = os.ReadFile(filepath.Join(env.TargetPath, "subdir", "file2.txt"))
		if err != nil {
			t.Errorf("failed to read subdir/file2.txt: %v", err)
		} else if string(content) != "source file 2" {
			t.Errorf("subdir/file2.txt content = %q, want %q", content, "source file 2")
		}

		t.Log("Overlay restore recovers tombstoned files test successful")
	})

	// Test overlay restore hides files created after snapshot
	t.Run("OverlayRestoreHidesPostSnapshotFiles", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "overlay_restore_hide")
		defer env.Cleanup()

		// Create source folder with initial file
		if err := os.WriteFile(filepath.Join(env.SourceDir, "original.txt"), []byte("original"), 0644); err != nil {
			t.Fatal(err)
		}

		// Create soft-fork (overlay)
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Save overlay snapshot (before creating new files)
		result = env.RunCLI("data", "save", "-d", env.DataFile, "-m", "Initial state", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// Create new file in overlay AFTER snapshot
		if err := os.WriteFile(filepath.Join(env.TargetPath, "new_file.txt"), []byte("new content"), 0644); err != nil {
			t.Fatalf("failed to create new_file.txt: %v", err)
		}

		// Verify new file exists
		if _, err := os.Stat(filepath.Join(env.TargetPath, "new_file.txt")); err != nil {
			t.Fatalf("new_file.txt should exist: %v", err)
		}

		// Unmount to release database lock
		env.RunCLI("unmount", env.TargetPath)
		env.WaitForUnmount(2*time.Second, env.TargetPath)

		// Restore from snapshot v1 (should hide post-snapshot files)
		result = env.RunCLI("data", "restore", "v1", "-d", env.DataFile, "-y")
		if result.ExitCode != 0 {
			t.Fatalf("restore failed: %s", result.Combined)
		}

		// Remount to see restored state
		result = env.RunCLI("mount", env.TargetPath, "-d", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("remount failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Verify new file is hidden after restore
		if _, err := os.Stat(filepath.Join(env.TargetPath, "new_file.txt")); !os.IsNotExist(err) {
			t.Error("new_file.txt should be hidden after restore (created after snapshot)")
		}

		// Original file should still be visible
		if _, err := os.Stat(filepath.Join(env.TargetPath, "original.txt")); err != nil {
			t.Errorf("original.txt should be visible after restore: %v", err)
		}

		t.Log("Overlay restore hides post-snapshot files test successful")
	})

	// Test live overlay restore without unmount/remount.
	// This tests the cache invalidation via IPC - matching test_save_restore_overlay_0.sh
	// Key difference from tests above: restore happens while mount is active.
	t.Run("OverlayRestoreLiveWithCacheInvalidation", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "overlay_restore_live")
		defer env.Cleanup()

		// Create source folder with a directory containing multiple files
		testDir := filepath.Join(env.SourceDir, "testdir")
		if err := os.MkdirAll(testDir, 0755); err != nil {
			t.Fatal(err)
		}
		for i := 1; i <= 5; i++ {
			content := fmt.Sprintf("source file %d content", i)
			if err := os.WriteFile(filepath.Join(testDir, fmt.Sprintf("file%d.txt", i)), []byte(content), 0644); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(env.SourceDir, "root.txt"), []byte("root file"), 0644); err != nil {
			t.Fatal(err)
		}

		// Create soft-fork (overlay)
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Count initial files
		initialFiles, _ := os.ReadDir(filepath.Join(env.TargetPath, "testdir"))
		t.Logf("Initial file count in testdir: %d", len(initialFiles))

		// Save overlay snapshot (before any modifications)
		result = env.RunCLI("data", "save", "-d", env.DataFile, "-m", "Before modifications", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("save failed: %s", result.Combined)
		}

		// Delete the directory (creates tombstones) - like shell script step 6
		if err := os.RemoveAll(filepath.Join(env.TargetPath, "testdir")); err != nil {
			t.Fatalf("failed to delete testdir: %v", err)
		}
		t.Log("Deleted testdir")

		// Verify deletion
		if _, err := os.Stat(filepath.Join(env.TargetPath, "testdir")); !os.IsNotExist(err) {
			t.Fatal("testdir should be hidden after deletion")
		}

		// Create new files AFTER snapshot - like shell script step 7
		newFilesDir := filepath.Join(env.TargetPath, "_test_new_files")
		if err := os.MkdirAll(newFilesDir, 0755); err != nil {
			t.Fatalf("failed to create new files dir: %v", err)
		}
		for i := 1; i <= 3; i++ {
			if err := os.WriteFile(filepath.Join(newFilesDir, fmt.Sprintf("new%d.txt", i)), []byte(fmt.Sprintf("new %d", i)), 0644); err != nil {
				t.Fatal(err)
			}
		}
		newRootFile := filepath.Join(env.TargetPath, "_test_root_file.txt")
		if err := os.WriteFile(newRootFile, []byte("new root file"), 0644); err != nil {
			t.Fatal(err)
		}
		t.Logf("Created new files: %s (3 files) and %s", newFilesDir, newRootFile)

		// Verify new files exist
		if _, err := os.Stat(newFilesDir); err != nil {
			t.Fatalf("new files dir should exist: %v", err)
		}
		if _, err := os.Stat(newRootFile); err != nil {
			t.Fatalf("new root file should exist: %v", err)
		}

		// LIVE RESTORE - no unmount! This tests IPC cache invalidation.
		// This is the key difference from the tests above.
		result = env.RunCLI("data", "restore", "v1", "-d", env.DataFile, "-y")
		if result.ExitCode != 0 {
			t.Fatalf("restore failed: %s", result.Combined)
		}
		t.Logf("Live restore output: %s", result.Combined)

		// Verify deleted directory is restored (anti-tombstones work)
		// Use Eventually to handle NFS cache delays
		g := gomega.NewWithT(t)
		g.Eventually(func() bool {
			_, err := os.Stat(filepath.Join(env.TargetPath, "testdir"))
			return err == nil
		}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(gomega.BeTrue(),
			"testdir should be visible after live restore")

		// Verify file count matches
		restoredFiles, err := os.ReadDir(filepath.Join(env.TargetPath, "testdir"))
		if err != nil {
			t.Fatalf("failed to read testdir: %v", err)
		}
		if len(restoredFiles) != len(initialFiles) {
			t.Errorf("file count mismatch: got %d, want %d", len(restoredFiles), len(initialFiles))
		}
		t.Logf("Restored file count in testdir: %d", len(restoredFiles))

		// Verify file contents are correct
		content, err := os.ReadFile(filepath.Join(env.TargetPath, "testdir", "file1.txt"))
		if err != nil {
			t.Errorf("failed to read file1.txt: %v", err)
		} else if string(content) != "source file 1 content" {
			t.Errorf("file1.txt content = %q, want %q", content, "source file 1 content")
		}

		// Verify new files are hidden (anti-tombstones for post-snapshot entries)
		g.Eventually(func() bool {
			_, err := os.Stat(newFilesDir)
			return os.IsNotExist(err)
		}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(gomega.BeTrue(),
			"new files dir should be hidden after live restore")

		g.Eventually(func() bool {
			_, err := os.Stat(newRootFile)
			return os.IsNotExist(err)
		}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(gomega.BeTrue(),
			"new root file should be hidden after live restore")

		t.Log("Overlay live restore with cache invalidation test successful")
	})
}
