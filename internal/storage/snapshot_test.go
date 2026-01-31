package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/tursodatabase/go-libsql"
)

func TestSnapshotCreateAndRestore(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "snapshot_test_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dataFilePath := filepath.Join(tmpDir, "test.latentfs")

	// Create data file
	df, err := Create(dataFilePath)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file
	ino, err := df.CreateInode(ModeFile | 0644)
	if err != nil {
		t.Fatal(err)
	}
	if err := df.CreateDentry(RootIno, "test.txt", ino); err != nil {
		t.Fatal(err)
	}
	if err := df.WriteContent(ino, 0, []byte("original content")); err != nil {
		t.Fatal(err)
	}

	// Create a snapshot with tag v1
	snap, err := df.CreateSnapshot("Initial snapshot", "v1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Created snapshot %s", snap.ID[:8])

	// Modify the file
	if err := df.TruncateContent(ino, 0); err != nil {
		t.Fatal(err)
	}
	if err := df.WriteContent(ino, 0, []byte("modified content")); err != nil {
		t.Fatal(err)
	}

	// Verify modification
	content, err := df.ReadContent(ino, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "modified content" {
		t.Errorf("Expected modified content, got %s", content)
	}

	// List snapshots
	snapshots, err := df.ListSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 {
		t.Errorf("Expected 1 snapshot, got %d", len(snapshots))
	}

	// Get snapshot by tag
	foundSnap, err := df.GetSnapshot("v1")
	if err != nil {
		t.Fatal(err)
	}
	if foundSnap.ID != snap.ID {
		t.Error("Snapshot ID mismatch")
	}

	// Restore from snapshot (full restore)
	result, err := df.RestoreFromSnapshot("v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Restored %d files", len(result.RestoredPaths))

	// Read file content after restore - need to look up the new inode
	dentry, err := df.Lookup(RootIno, "test.txt")
	if err != nil {
		t.Fatal(err)
	}
	content, err = df.ReadContent(dentry.Ino, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "original content" {
		t.Errorf("Expected 'original content' after restore, got %s", content)
	}

	df.Close()
	t.Log("Test passed!")
}

// TestSnapshotRestoreMtimeUpdated verifies that restore updates mtime to current time
// instead of preserving the original snapshot mtime. This is important for:
// 1. NFS cache coherency (NFS uses mtime to detect changes)
// 2. Build systems (make, etc.) that rely on mtime
func TestSnapshotRestoreMtimeUpdated(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "snapshot_mtime_test_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dataFilePath := filepath.Join(tmpDir, "test.latentfs")

	// Create data file
	df, err := Create(dataFilePath)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file
	ino, err := df.CreateInode(ModeFile | 0644)
	if err != nil {
		t.Fatal(err)
	}
	if err := df.CreateDentry(RootIno, "test.txt", ino); err != nil {
		t.Fatal(err)
	}
	if err := df.WriteContent(ino, 0, []byte("original content")); err != nil {
		t.Fatal(err)
	}

	// Get the original mtime
	origInode, err := df.GetInode(ino)
	if err != nil {
		t.Fatal(err)
	}
	origMtime := origInode.Mtime

	// Create a snapshot
	_, err = df.CreateSnapshot("Snapshot", "v1")
	if err != nil {
		t.Fatal(err)
	}

	// Wait a bit to ensure time difference is detectable
	// (mtime granularity is 1 second)
	time.Sleep(1100 * time.Millisecond)

	// Modify the file
	if err := df.TruncateContent(ino, 0); err != nil {
		t.Fatal(err)
	}
	if err := df.WriteContent(ino, 0, []byte("modified content")); err != nil {
		t.Fatal(err)
	}

	// Restore from snapshot
	beforeRestore := time.Now()
	result, err := df.RestoreFromSnapshot("v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	afterRestore := time.Now()

	if len(result.RestoredPaths) == 0 {
		t.Fatal("Expected at least one restored path")
	}

	// Look up the restored file
	dentry, err := df.Lookup(RootIno, "test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Get the restored inode
	restoredInode, err := df.GetInode(dentry.Ino)
	if err != nil {
		t.Fatal(err)
	}

	// Verify content is restored
	content, _ := df.ReadContent(dentry.Ino, 0, 100)
	if string(content) != "original content" {
		t.Errorf("Content not restored: got %s, want 'original content'", content)
	}

	// Verify mtime is NOT the original snapshot mtime
	// It should be updated to a time around when restore happened
	if restoredInode.Mtime.Equal(origMtime) {
		t.Errorf("Mtime should be updated after restore, but got original mtime: %v", origMtime)
	}

	// Verify mtime is within the restore time window
	if restoredInode.Mtime.Before(beforeRestore.Add(-1*time.Second)) ||
		restoredInode.Mtime.After(afterRestore.Add(1*time.Second)) {
		t.Errorf("Mtime should be around restore time. Got %v, expected between %v and %v",
			restoredInode.Mtime, beforeRestore, afterRestore)
	}

	df.Close()
	t.Logf("Restore mtime test passed! Original mtime: %v, Restored mtime: %v",
		origMtime, restoredInode.Mtime)
}

func TestSnapshotPartialRestore(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "snapshot_partial_test_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dataFilePath := filepath.Join(tmpDir, "test.latentfs")

	// Create data file
	df, err := Create(dataFilePath)
	if err != nil {
		t.Fatal(err)
	}

	// Create file1.txt
	ino1, _ := df.CreateInode(ModeFile | 0644)
	df.CreateDentry(RootIno, "file1.txt", ino1)
	df.WriteContent(ino1, 0, []byte("file1 original"))

	// Create file2.txt
	ino2, _ := df.CreateInode(ModeFile | 0644)
	df.CreateDentry(RootIno, "file2.txt", ino2)
	df.WriteContent(ino2, 0, []byte("file2 original"))

	// Create snapshot
	_, err = df.CreateSnapshot("Snapshot", "v1")
	if err != nil {
		t.Fatal(err)
	}

	// Modify both files
	df.TruncateContent(ino1, 0)
	df.WriteContent(ino1, 0, []byte("file1 modified"))
	df.TruncateContent(ino2, 0)
	df.WriteContent(ino2, 0, []byte("file2 modified"))

	// Restore only file1.txt
	result, err := df.RestoreFromSnapshot("v1", []string{"file1.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.RestoredPaths) != 1 {
		t.Errorf("Expected 1 restored path, got %d", len(result.RestoredPaths))
	}

	// Verify file1.txt is restored
	dentry1, _ := df.Lookup(RootIno, "file1.txt")
	content1, _ := df.ReadContent(dentry1.Ino, 0, 100)
	if string(content1) != "file1 original" {
		t.Errorf("file1.txt should be restored, got: %s", content1)
	}

	// Verify file2.txt is still modified
	content2, _ := df.ReadContent(ino2, 0, 100)
	if string(content2) != "file2 modified" {
		t.Errorf("file2.txt should still be modified, got: %s", content2)
	}

	df.Close()
	t.Log("Partial restore test passed!")
}

func TestSnapshotDuplicateTag(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "snapshot_dup_test_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dataFilePath := filepath.Join(tmpDir, "test.latentfs")

	// Create data file
	df, err := Create(dataFilePath)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	// Create first snapshot with tag
	_, err = df.CreateSnapshot("First", "v1")
	if err != nil {
		t.Fatal(err)
	}

	// Try to create another snapshot with same tag
	_, err = df.CreateSnapshot("Second", "v1")
	if err == nil {
		t.Error("Should have failed with duplicate tag")
	}
}

func TestSnapshotContentDeduplication(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "snapshot_dedup_test_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dataFilePath := filepath.Join(tmpDir, "test.latentfs")

	// Create data file
	df, err := Create(dataFilePath)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	// Create file with specific content
	ino, _ := df.CreateInode(ModeFile | 0644)
	df.CreateDentry(RootIno, "file.txt", ino)
	df.WriteContent(ino, 0, []byte("shared content data"))

	// Create first snapshot
	_, err = df.CreateSnapshot("First snapshot", "v1")
	if err != nil {
		t.Fatal(err)
	}

	// Create second snapshot with same content (should deduplicate)
	_, err = df.CreateSnapshot("Second snapshot", "v2")
	if err != nil {
		t.Fatal(err)
	}

	// Create third snapshot with same content
	_, err = df.CreateSnapshot("Third snapshot", "v3")
	if err != nil {
		t.Fatal(err)
	}

	// Check content_blocks table - should only have 1 block for the shared content
	var blockCount int
	err = df.db.QueryRow("SELECT COUNT(*) FROM content_blocks").Scan(&blockCount)
	if err != nil {
		t.Fatal(err)
	}

	// Should be 1 block (the content is under ChunkSize so it's 1 chunk)
	if blockCount != 1 {
		t.Errorf("Expected 1 content block (deduplicated), got %d", blockCount)
	}

	// Check snapshot_content references - should be 3 (one per snapshot)
	var refCount int
	err = df.db.QueryRow("SELECT COUNT(*) FROM snapshot_content").Scan(&refCount)
	if err != nil {
		t.Fatal(err)
	}

	if refCount != 3 {
		t.Errorf("Expected 3 snapshot content references, got %d", refCount)
	}

	// Verify all snapshots can be restored correctly
	for _, tag := range []string{"v1", "v2", "v3"} {
		result, err := df.RestoreFromSnapshot(tag, nil)
		if err != nil {
			t.Fatalf("Failed to restore from %s: %v", tag, err)
		}
		if len(result.RestoredPaths) == 0 {
			t.Errorf("No files restored from %s", tag)
		}

		// Verify content
		dentry, err := df.Lookup(RootIno, "file.txt")
		if err != nil {
			t.Fatal(err)
		}
		content, _ := df.ReadContent(dentry.Ino, 0, 100)
		if string(content) != "shared content data" {
			t.Errorf("Expected 'shared content data' after restore from %s, got %s", tag, content)
		}
	}

	t.Log("Content deduplication test passed!")
}

func TestSnapshotWithSourceFolder(t *testing.T) {
	// Create temp directories
	tmpDir, err := os.MkdirTemp("", "snapshot_source_test_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create source folder with files
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create source files
	if err := os.WriteFile(filepath.Join(sourceDir, "source_file.txt"), []byte("source content"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(sourceDir, "subdir"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "subdir", "nested.txt"), []byte("nested content"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create data file (simulating soft-fork)
	dataFilePath := filepath.Join(tmpDir, "test.latentfs")
	df, err := Create(dataFilePath)
	if err != nil {
		t.Fatal(err)
	}

	// Set source folder (this is what soft-fork does)
	if err := df.SetSourceFolder(sourceDir); err != nil {
		t.Fatal(err)
	}

	// Verify source folder is set
	if got := df.GetSourceFolder(); got != sourceDir {
		t.Errorf("GetSourceFolder() = %s, want %s", got, sourceDir)
	}

	// Create an overlay file (simulating a modified file)
	overlayMode := uint32(ModeFile | 0644)
	overlayIno, err := df.CreateInode(overlayMode)
	if err != nil {
		t.Fatal(err)
	}
	if err := df.CreateDentry(RootIno, "overlay_file.txt", overlayIno); err != nil {
		t.Fatal(err)
	}
	if err := df.WriteContent(overlayIno, 0, []byte("overlay content")); err != nil {
		t.Fatal(err)
	}

	// Create snapshot with source folder (no symlink target exclusion for this test)
	result, err := df.CreateSnapshotWithSource("Test snapshot", "v1", sourceDir, "", false)
	if err != nil {
		t.Fatalf("CreateSnapshotWithSource failed: %v", err)
	}

	if result.Snapshot == nil {
		t.Fatal("Snapshot should not be nil")
	}
	if result.Snapshot.Tag != "v1" {
		t.Errorf("Tag = %s, want v1", result.Snapshot.Tag)
	}
	if len(result.SkippedFiles) > 0 {
		t.Errorf("Unexpected skipped files: %v", result.SkippedFiles)
	}

	// Close and reopen to test restore
	df.Close()
	df, err = Open(dataFilePath)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	// Get the snapshot
	snapshot, err := df.GetSnapshot("v1")
	if err != nil {
		t.Fatal(err)
	}

	// Restore the snapshot (nil paths = restore all)
	restoreResult, err := df.RestoreFromSnapshot(snapshot.ID, nil)
	if err != nil {
		t.Fatalf("RestoreFromSnapshot failed: %v", err)
	}

	// Verify restored files include both source and overlay
	restoredPaths := make(map[string]bool)
	for _, p := range restoreResult.RestoredPaths {
		restoredPaths[p] = true
	}

	// Check overlay file is restored (paths have leading slash)
	if !restoredPaths["/overlay_file.txt"] {
		t.Error("overlay_file.txt should be restored")
	}

	// Check source files are restored
	if !restoredPaths["/source_file.txt"] {
		t.Error("source_file.txt should be restored")
	}
	if !restoredPaths["/subdir/nested.txt"] {
		t.Error("subdir/nested.txt should be restored")
	}

	// Verify content of restored files
	overlayDentry, err := df.Lookup(RootIno, "overlay_file.txt")
	if err != nil {
		t.Fatalf("Failed to lookup overlay_file.txt: %v", err)
	}
	overlayContent, _ := df.ReadContent(overlayDentry.Ino, 0, 100)
	if string(overlayContent) != "overlay content" {
		t.Errorf("overlay_file.txt content = %s, want 'overlay content'", overlayContent)
	}

	sourceDentry, err := df.Lookup(RootIno, "source_file.txt")
	if err != nil {
		t.Fatalf("Failed to lookup source_file.txt: %v", err)
	}
	sourceContent, _ := df.ReadContent(sourceDentry.Ino, 0, 100)
	if string(sourceContent) != "source content" {
		t.Errorf("source_file.txt content = %s, want 'source content'", sourceContent)
	}

	t.Log("Snapshot with source folder test passed!")
}

func TestSnapshotWithSourceOverlayPrecedence(t *testing.T) {
	// Test that overlay files take precedence over source files
	tmpDir, err := os.MkdirTemp("", "snapshot_precedence_test_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create source folder with a file
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "file.txt"), []byte("source version"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create data file
	dataFilePath := filepath.Join(tmpDir, "test.latentfs")
	df, err := Create(dataFilePath)
	if err != nil {
		t.Fatal(err)
	}

	// Set source folder
	if err := df.SetSourceFolder(sourceDir); err != nil {
		t.Fatal(err)
	}

	// Create overlay file with same name (simulating modified file)
	overlayMode := uint32(ModeFile | 0644)
	overlayIno, err := df.CreateInode(overlayMode)
	if err != nil {
		t.Fatal(err)
	}
	if err := df.CreateDentry(RootIno, "file.txt", overlayIno); err != nil {
		t.Fatal(err)
	}
	if err := df.WriteContent(overlayIno, 0, []byte("overlay version")); err != nil {
		t.Fatal(err)
	}

	// Create snapshot with source folder (no symlink target exclusion for this test)
	result, err := df.CreateSnapshotWithSource("Test", "v1", sourceDir, "", false)
	if err != nil {
		t.Fatal(err)
	}

	df.Close()
	df, err = Open(dataFilePath)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	// Restore and verify overlay version is used
	_, err = df.RestoreFromSnapshot(result.Snapshot.ID, nil)
	if err != nil {
		t.Fatal(err)
	}

	dentry, err := df.Lookup(RootIno, "file.txt")
	if err != nil {
		t.Fatal(err)
	}
	content, _ := df.ReadContent(dentry.Ino, 0, 100)
	if string(content) != "overlay version" {
		t.Errorf("file.txt content = %s, want 'overlay version' (overlay should take precedence)", content)
	}

	t.Log("Snapshot overlay precedence test passed!")
}

func TestComputeHiddenPath(t *testing.T) {
	tests := []struct {
		name         string
		sourceFolder string
		symlinkTarget string
		want         string
	}{
		{
			name:          "target inside source root",
			sourceFolder:  "/tmp/source",
			symlinkTarget: "/tmp/source/target",
			want:          "target",
		},
		{
			name:          "target nested inside source",
			sourceFolder:  "/tmp/source",
			symlinkTarget: "/tmp/source/level1/level2/target",
			want:          "level1/level2/target",
		},
		{
			name:          "target outside source",
			sourceFolder:  "/tmp/source",
			symlinkTarget: "/tmp/other/target",
			want:          "",
		},
		{
			name:          "target is source parent",
			sourceFolder:  "/tmp/source/child",
			symlinkTarget: "/tmp/source",
			want:          "",
		},
		{
			name:          "empty source",
			sourceFolder:  "",
			symlinkTarget: "/tmp/target",
			want:          "",
		},
		{
			name:          "empty target",
			sourceFolder:  "/tmp/source",
			symlinkTarget: "",
			want:          "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeHiddenPath(tt.sourceFolder, tt.symlinkTarget)
			if got != tt.want {
				t.Errorf("computeHiddenPath(%q, %q) = %q, want %q",
					tt.sourceFolder, tt.symlinkTarget, got, tt.want)
			}
		})
	}
}

func TestSnapshotWithTargetInsideSource(t *testing.T) {
	// Create temp directory that acts as both source and container for target
	tmpDir, err := os.MkdirTemp("", "snapshot_target_inside_test_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Source folder is the temp directory itself
	sourceDir := tmpDir

	// Target path is INSIDE source folder (simulating fork target inside source)
	targetPath := filepath.Join(sourceDir, "fork_target")

	// Create some source files
	if err := os.WriteFile(filepath.Join(sourceDir, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(sourceDir, "subdir"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "subdir", "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create the target directory (in real fork, this would be a symlink)
	// For testing, just create a directory to simulate what would exist
	if err := os.MkdirAll(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(targetPath, "inside_target.txt"), []byte("should be excluded"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create the .latentfs data file (normally next to the target symlink)
	dataFilePath := targetPath + ".latentfs"
	df, err := Create(dataFilePath)
	if err != nil {
		t.Fatal(err)
	}

	// Set source folder (this is what soft-fork does)
	if err := df.SetSourceFolder(sourceDir); err != nil {
		t.Fatal(err)
	}

	// Create snapshot with source folder and symlink target
	// The symlink target is now passed as a parameter (retrieved from daemon at runtime)
	result, err := df.CreateSnapshotWithSource("Test snapshot", "v1", sourceDir, targetPath, false)
	if err != nil {
		t.Fatalf("CreateSnapshotWithSource failed: %v", err)
	}

	if result.Snapshot == nil {
		t.Fatal("Snapshot should not be nil")
	}

	// Restore the snapshot to the overlay (this copies snapshot contents to the live filesystem)
	_, err = df.RestoreFromSnapshot(result.Snapshot.ID, nil)
	if err != nil {
		t.Fatalf("RestoreFromSnapshot failed: %v", err)
	}

	// Verify file1.txt was included by looking it up in the overlay
	if _, err := df.Lookup(RootIno, "file1.txt"); err != nil {
		t.Error("file1.txt should be in snapshot")
	}

	// Verify subdir exists
	subdirDentry, err := df.Lookup(RootIno, "subdir")
	if err != nil {
		t.Error("subdir should be in snapshot")
	} else {
		// Verify subdir/file2.txt was included
		if _, err := df.Lookup(subdirDentry.Ino, "file2.txt"); err != nil {
			t.Error("subdir/file2.txt should be in snapshot")
		}
	}

	// Verify fork_target was EXCLUDED (the hidden path)
	if _, err := df.Lookup(RootIno, "fork_target"); err == nil {
		t.Error("fork_target should be EXCLUDED from snapshot (it's the symlink target)")
	}

	// Verify fork_target.latentfs was also EXCLUDED
	if _, err := df.Lookup(RootIno, "fork_target.latentfs"); err == nil {
		t.Error("fork_target.latentfs should be EXCLUDED from snapshot")
	}

	df.Close()
	t.Log("Snapshot with target inside source test passed - target folder properly excluded!")
}

func TestSnapshotWithNestedTargetInsideSource(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "snapshot_nested_target_test_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	sourceDir := tmpDir

	// Create nested directory structure
	nestedDir := filepath.Join(sourceDir, "level1", "level2")
	if err := os.MkdirAll(nestedDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Target path is nested INSIDE source folder
	targetPath := filepath.Join(nestedDir, "fork_target")

	// Create source files at various levels
	if err := os.WriteFile(filepath.Join(sourceDir, "root.txt"), []byte("root"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "level1", "level1.txt"), []byte("level1"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nestedDir, "level2.txt"), []byte("level2"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create the target directory
	if err := os.MkdirAll(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(targetPath, "excluded.txt"), []byte("should be excluded"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create data file
	dataFilePath := targetPath + ".latentfs"
	df, err := Create(dataFilePath)
	if err != nil {
		t.Fatal(err)
	}

	// Set source folder (this is what soft-fork does)
	if err := df.SetSourceFolder(sourceDir); err != nil {
		t.Fatal(err)
	}

	// Create snapshot with symlink target passed as parameter
	result, err := df.CreateSnapshotWithSource("Test", "v1", sourceDir, targetPath, false)
	if err != nil {
		t.Fatalf("CreateSnapshotWithSource failed: %v", err)
	}

	// Restore the snapshot to overlay
	_, err = df.RestoreFromSnapshot(result.Snapshot.ID, nil)
	if err != nil {
		t.Fatalf("RestoreFromSnapshot failed: %v", err)
	}

	// Verify files at each level were included
	if _, err := df.Lookup(RootIno, "root.txt"); err != nil {
		t.Error("root.txt should be in snapshot")
	}

	level1Dentry, err := df.Lookup(RootIno, "level1")
	if err != nil {
		t.Error("level1 should be in snapshot")
	} else {
		if _, err := df.Lookup(level1Dentry.Ino, "level1.txt"); err != nil {
			t.Error("level1/level1.txt should be in snapshot")
		}

		level2Dentry, err := df.Lookup(level1Dentry.Ino, "level2")
		if err != nil {
			t.Error("level1/level2 should be in snapshot")
		} else {
			if _, err := df.Lookup(level2Dentry.Ino, "level2.txt"); err != nil {
				t.Error("level1/level2/level2.txt should be in snapshot")
			}

			// Verify nested target was EXCLUDED
			if _, err := df.Lookup(level2Dentry.Ino, "fork_target"); err == nil {
				t.Error("level1/level2/fork_target should be EXCLUDED from snapshot")
			}
		}
	}

	df.Close()
	t.Log("Snapshot with nested target inside source test passed!")
}
