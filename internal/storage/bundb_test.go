package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBunDB_GetInode(t *testing.T) {
	// Create a temporary data file
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.latentfs")

	df, err := Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create data file: %v", err)
	}
	defer df.Close()

	// Create a Bun wrapper
	bunDB := NewBunDB(df.DB())

	ctx := context.Background()

	// Test getting the root inode (ino=1) which is created by default
	inode, err := bunDB.GetInode(ctx, RootIno, 0)
	if err != nil {
		t.Fatalf("Failed to get root inode: %v", err)
	}

	if inode.Ino != RootIno {
		t.Errorf("Expected ino %d, got %d", RootIno, inode.Ino)
	}

	if inode.Mode != int64(DefaultDirMode) {
		t.Errorf("Expected mode %o, got %o", DefaultDirMode, inode.Mode)
	}
}

func TestBunDB_InodeConversion(t *testing.T) {
	// Test conversion between Inode and InodeModel
	now := time.Now().Truncate(time.Second) // Truncate to second precision (Unix timestamps)

	original := &Inode{
		Ino:   42,
		Mode:  DefaultFileMode,
		Uid:   1000,
		Gid:   1000,
		Size:  1024,
		Atime: now,
		Mtime: now,
		Ctime: now,
		Nlink: 1,
	}

	// Convert to model
	model := InodeModelFromInode(original, 5)

	if model.Ino != original.Ino {
		t.Errorf("Ino mismatch: expected %d, got %d", original.Ino, model.Ino)
	}
	if model.WriteEpoch != 5 {
		t.Errorf("WriteEpoch mismatch: expected 5, got %d", model.WriteEpoch)
	}
	if model.Mode != int64(original.Mode) {
		t.Errorf("Mode mismatch: expected %d, got %d", original.Mode, model.Mode)
	}

	// Convert back
	converted := model.ToInode()

	if converted.Ino != original.Ino {
		t.Errorf("Converted Ino mismatch: expected %d, got %d", original.Ino, converted.Ino)
	}
	if converted.Mode != original.Mode {
		t.Errorf("Converted Mode mismatch: expected %o, got %o", original.Mode, converted.Mode)
	}
	if converted.Size != original.Size {
		t.Errorf("Converted Size mismatch: expected %d, got %d", original.Size, converted.Size)
	}
	if !converted.Mtime.Equal(original.Mtime) {
		t.Errorf("Converted Mtime mismatch: expected %v, got %v", original.Mtime, converted.Mtime)
	}
}

func TestBunDB_InsertAndGetInode(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.latentfs")

	df, err := Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create data file: %v", err)
	}
	defer df.Close()

	bunDB := NewBunDB(df.DB())
	ctx := context.Background()

	// Insert a new inode using Bun
	now := time.Now().Unix()
	newInode := &InodeModel{
		Ino:        100,
		WriteEpoch: 0,
		Mode:       int64(DefaultFileMode),
		UID:        1000,
		GID:        1000,
		Size:       512,
		Atime:      now,
		Mtime:      now,
		Ctime:      now,
		Nlink:      1,
	}

	err = bunDB.InsertInode(ctx, newInode)
	if err != nil {
		t.Fatalf("Failed to insert inode: %v", err)
	}

	// Retrieve it back
	retrieved, err := bunDB.GetInode(ctx, 100, 0)
	if err != nil {
		t.Fatalf("Failed to get inserted inode: %v", err)
	}

	if retrieved.Ino != newInode.Ino {
		t.Errorf("Ino mismatch: expected %d, got %d", newInode.Ino, retrieved.Ino)
	}
	if retrieved.Size != newInode.Size {
		t.Errorf("Size mismatch: expected %d, got %d", newInode.Size, retrieved.Size)
	}
}

func TestBunDB_EpochScope(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.latentfs")

	df, err := Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create data file: %v", err)
	}
	defer df.Close()

	bunDB := NewBunDB(df.DB())
	ctx := context.Background()

	now := time.Now().Unix()

	// Insert multiple versions of the same inode at different epochs
	for epoch := int64(0); epoch <= 3; epoch++ {
		inode := &InodeModel{
			Ino:        200,
			WriteEpoch: epoch,
			Mode:       int64(DefaultFileMode),
			UID:        1000,
			GID:        1000,
			Size:       100 * (epoch + 1), // Size increases with epoch
			Atime:      now,
			Mtime:      now,
			Ctime:      now,
			Nlink:      1,
		}
		if err := bunDB.InsertInode(ctx, inode); err != nil {
			t.Fatalf("Failed to insert inode at epoch %d: %v", epoch, err)
		}
	}

	// Test reading at different epochs
	testCases := []struct {
		maxEpoch     int64
		expectedSize int64
	}{
		{0, 100},
		{1, 200},
		{2, 300},
		{3, 400},
		{10, 400}, // Beyond latest epoch, should still get epoch 3
	}

	for _, tc := range testCases {
		inode, err := bunDB.GetInode(ctx, 200, tc.maxEpoch)
		if err != nil {
			t.Fatalf("Failed to get inode at epoch %d: %v", tc.maxEpoch, err)
		}
		if inode.Size != tc.expectedSize {
			t.Errorf("At maxEpoch %d: expected size %d, got %d", tc.maxEpoch, tc.expectedSize, inode.Size)
		}
	}
}

func TestBunDB_ListDentries(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.latentfs")

	df, err := Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create data file: %v", err)
	}
	defer df.Close()

	bunDB := NewBunDB(df.DB())
	ctx := context.Background()
	now := time.Now().Unix()

	// Insert some directory entries (with corresponding inodes)
	entries := []struct {
		name string
		ino  int64
		mode int64
	}{
		{"file1.txt", 10, int64(DefaultFileMode)},
		{"file2.txt", 11, int64(DefaultFileMode)},
		{"subdir", 12, int64(DefaultDirMode)},
	}

	for _, e := range entries {
		// Create inode first (ListDentries requires inodes to exist)
		inode := &InodeModel{
			Ino:        e.ino,
			WriteEpoch: 0,
			Mode:       e.mode,
			UID:        1000,
			GID:        1000,
			Size:       0,
			Atime:      now,
			Mtime:      now,
			Ctime:      now,
			Nlink:      1,
		}
		if err := bunDB.InsertInode(ctx, inode); err != nil {
			t.Fatalf("Failed to insert inode for %s: %v", e.name, err)
		}

		dentry := &DentryModel{
			ParentIno:  RootIno,
			Name:       e.name,
			WriteEpoch: 0,
			Ino:        e.ino,
		}
		if err := bunDB.InsertDentry(ctx, dentry); err != nil {
			t.Fatalf("Failed to insert dentry %s: %v", e.name, err)
		}
	}

	// List all entries
	dentries, err := bunDB.ListDentries(ctx, RootIno, 0)
	if err != nil {
		t.Fatalf("Failed to list dentries: %v", err)
	}

	if len(dentries) != len(entries) {
		t.Errorf("Expected %d dentries, got %d", len(entries), len(dentries))
	}
}

func TestBunDB_DentryTombstone(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.latentfs")

	df, err := Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create data file: %v", err)
	}
	defer df.Close()

	bunDB := NewBunDB(df.DB())
	ctx := context.Background()
	now := time.Now().Unix()

	// Create inode first (ListDentries requires inodes to exist)
	inode := &InodeModel{
		Ino:        50,
		WriteEpoch: 0,
		Mode:       int64(DefaultFileMode),
		UID:        1000,
		GID:        1000,
		Size:       0,
		Atime:      now,
		Mtime:      now,
		Ctime:      now,
		Nlink:      1,
	}
	if err := bunDB.InsertInode(ctx, inode); err != nil {
		t.Fatalf("Failed to insert inode: %v", err)
	}

	// Insert a dentry at epoch 0
	dentry := &DentryModel{
		ParentIno:  RootIno,
		Name:       "deleted.txt",
		WriteEpoch: 0,
		Ino:        50,
	}
	if err := bunDB.InsertDentry(ctx, dentry); err != nil {
		t.Fatalf("Failed to insert dentry: %v", err)
	}

	// Insert a tombstone at epoch 1
	tombstone := &DentryModel{
		ParentIno:  RootIno,
		Name:       "deleted.txt",
		WriteEpoch: 1,
		Ino:        0, // Tombstone
	}
	if err := bunDB.InsertDentry(ctx, tombstone); err != nil {
		t.Fatalf("Failed to insert tombstone: %v", err)
	}

	// At epoch 0, we should see the entry
	dentries, err := bunDB.ListDentries(ctx, RootIno, 0)
	if err != nil {
		t.Fatalf("Failed to list dentries at epoch 0: %v", err)
	}
	if len(dentries) != 1 {
		t.Errorf("Expected 1 dentry at epoch 0, got %d", len(dentries))
	}

	// At epoch 1, we should NOT see the entry (tombstone filters it out)
	dentries, err = bunDB.ListDentries(ctx, RootIno, 1)
	if err != nil {
		t.Fatalf("Failed to list dentries at epoch 1: %v", err)
	}
	if len(dentries) != 0 {
		t.Errorf("Expected 0 dentries at epoch 1 (tombstone), got %d", len(dentries))
	}
}

func TestBunDB_Config(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.latentfs")

	df, err := Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create data file: %v", err)
	}
	defer df.Close()

	bunDB := NewBunDB(df.DB())
	ctx := context.Background()

	// Test set and get config
	err = bunDB.SetConfigValue(ctx, "test_key", "test_value")
	if err != nil {
		t.Fatalf("Failed to set config: %v", err)
	}

	value, err := bunDB.GetConfigValue(ctx, "test_key")
	if err != nil {
		t.Fatalf("Failed to get config: %v", err)
	}
	if value != "test_value" {
		t.Errorf("Expected 'test_value', got '%s'", value)
	}

	// Test upsert
	err = bunDB.SetConfigValue(ctx, "test_key", "updated_value")
	if err != nil {
		t.Fatalf("Failed to update config: %v", err)
	}

	value, err = bunDB.GetConfigValue(ctx, "test_key")
	if err != nil {
		t.Fatalf("Failed to get updated config: %v", err)
	}
	if value != "updated_value" {
		t.Errorf("Expected 'updated_value', got '%s'", value)
	}
}

func TestBunDB_MountEntry(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_meta.latentfs")

	mf, err := CreateMeta(dbPath)
	if err != nil {
		t.Fatalf("Failed to create meta file: %v", err)
	}
	defer mf.Close()

	bunDB := NewBunDB(mf.DB())
	ctx := context.Background()

	now := time.Now().Unix()

	// Insert a mount entry
	entry := &MountEntryModel{
		SubPath:       "/project/foo",
		DataFile:      "/data/foo.latentfs",
		SourceFolder:  "/source/foo",
		ForkType:      "soft",
		SymlinkTarget: "/symlink/foo",
		CreatedAt:     now,
	}

	err = bunDB.InsertMountEntry(ctx, entry)
	if err != nil {
		t.Fatalf("Failed to insert mount entry: %v", err)
	}

	// Get it back
	retrieved, err := bunDB.GetMountEntry(ctx, "/project/foo")
	if err != nil {
		t.Fatalf("Failed to get mount entry: %v", err)
	}

	if retrieved.SubPath != entry.SubPath {
		t.Errorf("SubPath mismatch: expected %s, got %s", entry.SubPath, retrieved.SubPath)
	}
	if retrieved.DataFile != entry.DataFile {
		t.Errorf("DataFile mismatch: expected %s, got %s", entry.DataFile, retrieved.DataFile)
	}
	if retrieved.ForkType != entry.ForkType {
		t.Errorf("ForkType mismatch: expected %s, got %s", entry.ForkType, retrieved.ForkType)
	}

	// Test conversion to MountEntry
	mountEntry := retrieved.ToMountEntry()
	if mountEntry.SubPath != entry.SubPath {
		t.Errorf("Converted SubPath mismatch: expected %s, got %s", entry.SubPath, mountEntry.SubPath)
	}

	// Delete
	_, err = bunDB.DeleteMountEntry(ctx, "/project/foo")
	if err != nil {
		t.Fatalf("Failed to delete mount entry: %v", err)
	}

	// Verify deletion
	deletedEntry, err := bunDB.GetMountEntry(ctx, "/project/foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if deletedEntry != nil {
		t.Error("Expected nil for deleted mount entry, got non-nil")
	}
}

// TestBunDB_VerifyDBAccess verifies that the BunDB wrapper accesses the same underlying database
func TestBunDB_VerifyDBAccess(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.latentfs")

	df, err := Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create data file: %v", err)
	}
	defer df.Close()

	// Create a file using the original raw SQL interface
	newIno, err := df.CreateInode(DefaultFileMode)
	if err != nil {
		t.Fatalf("Failed to create inode via raw SQL: %v", err)
	}

	// Read it back using Bun
	bunDB := NewBunDB(df.DB())
	ctx := context.Background()

	// Get all inodes (we expect root + the one we created)
	var inodes []InodeModel
	err = bunDB.NewSelect().Model(&inodes).Where("write_epoch <= ?", 0).Scan(ctx)
	if err != nil {
		t.Fatalf("Failed to query inodes via Bun: %v", err)
	}

	if len(inodes) != 2 { // root (ino=1) + our new inode (ino=2)
		t.Errorf("Expected 2 inodes, got %d", len(inodes))
	}

	// Verify the new inode
	var found bool
	for _, inode := range inodes {
		if inode.Ino == newIno {
			found = true
			if inode.Mode != int64(DefaultFileMode) {
				t.Errorf("Expected mode %o, got %o", DefaultFileMode, inode.Mode)
			}
			break
		}
	}
	if !found {
		t.Errorf("New inode (ino=%d) not found via Bun query", newIno)
	}
}

func TestMain(m *testing.M) {
	// Run tests
	os.Exit(m.Run())
}
