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

func createTestDataFileWithEpoch(t *testing.T) (*DataFile, func()) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "epoch_test")
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(tmpDir, "test.latentfs")
	df, err := Create(path)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatal(err)
	}

	cleanup := func() {
		df.Close()
		os.RemoveAll(tmpDir)
	}

	return df, cleanup
}

func TestEpochInitialization(t *testing.T) {
	df, cleanup := createTestDataFileWithEpoch(t)
	defer cleanup()

	// Verify initial epoch is 0 and status is 'ready'
	if df.GetCurrentWriteEpoch() != 0 {
		t.Errorf("initial write epoch = %d, want 0", df.GetCurrentWriteEpoch())
	}
	if df.GetReadableEpoch() != 0 {
		t.Errorf("initial readable epoch = %d, want 0", df.GetReadableEpoch())
	}
}

func TestCreateWriteEpoch(t *testing.T) {
	df, cleanup := createTestDataFileWithEpoch(t)
	defer cleanup()

	// Create a new ready epoch
	epoch, err := df.CreateWriteEpoch("ready", "test:create")
	if err != nil {
		t.Fatalf("CreateWriteEpoch failed: %v", err)
	}
	if epoch != 1 {
		t.Errorf("new epoch = %d, want 1", epoch)
	}

	// Create a draft epoch
	epoch2, err := df.CreateWriteEpoch("draft", "test:draft")
	if err != nil {
		t.Fatalf("CreateWriteEpoch (draft) failed: %v", err)
	}
	if epoch2 != 2 {
		t.Errorf("draft epoch = %d, want 2", epoch2)
	}
}

func TestSetEpochStatus(t *testing.T) {
	df, cleanup := createTestDataFileWithEpoch(t)
	defer cleanup()

	// Create a draft epoch
	epoch, err := df.CreateWriteEpoch("draft", "test:status")
	if err != nil {
		t.Fatalf("CreateWriteEpoch failed: %v", err)
	}

	// Verify readable epoch is still 0 (draft not visible)
	if err := df.RefreshEpochs(); err != nil {
		t.Fatalf("RefreshEpochs failed: %v", err)
	}
	if df.GetReadableEpoch() != 0 {
		t.Errorf("readable epoch after draft = %d, want 0", df.GetReadableEpoch())
	}

	// Set to ready
	if err := df.SetEpochStatus(epoch, "ready"); err != nil {
		t.Fatalf("SetEpochStatus failed: %v", err)
	}

	// Refresh and verify
	if err := df.RefreshEpochs(); err != nil {
		t.Fatalf("RefreshEpochs failed: %v", err)
	}
	if df.GetReadableEpoch() != epoch {
		t.Errorf("readable epoch after ready = %d, want %d", df.GetReadableEpoch(), epoch)
	}
}

func TestSetCurrentWriteEpoch(t *testing.T) {
	df, cleanup := createTestDataFileWithEpoch(t)
	defer cleanup()

	// Create a new epoch
	epoch, err := df.CreateWriteEpoch("ready", "test:current")
	if err != nil {
		t.Fatalf("CreateWriteEpoch failed: %v", err)
	}

	// Set as current
	if err := df.SetCurrentWriteEpoch(epoch); err != nil {
		t.Fatalf("SetCurrentWriteEpoch failed: %v", err)
	}

	// Verify
	if df.GetCurrentWriteEpoch() != epoch {
		t.Errorf("current write epoch = %d, want %d", df.GetCurrentWriteEpoch(), epoch)
	}
}

func TestEpochAwareInodeOperations(t *testing.T) {
	df, cleanup := createTestDataFileWithEpoch(t)
	defer cleanup()

	// Create a file at epoch 0
	ino, err := df.CreateInode(DefaultFileMode)
	if err != nil {
		t.Fatalf("CreateInode failed: %v", err)
	}

	// Verify we can read it
	inode, err := df.GetInode(ino)
	if err != nil {
		t.Fatalf("GetInode failed: %v", err)
	}
	if inode.Ino != ino {
		t.Errorf("inode.Ino = %d, want %d", inode.Ino, ino)
	}

	// Create new epoch
	newEpoch, err := df.CreateWriteEpoch("ready", "test:inode_ops")
	if err != nil {
		t.Fatalf("CreateWriteEpoch failed: %v", err)
	}
	if err := df.SetCurrentWriteEpoch(newEpoch); err != nil {
		t.Fatalf("SetCurrentWriteEpoch failed: %v", err)
	}
	if err := df.RefreshEpochs(); err != nil {
		t.Fatalf("RefreshEpochs failed: %v", err)
	}

	// Update inode at new epoch
	newSize := int64(1024)
	if err := df.UpdateInode(ino, &InodeUpdate{Size: &newSize}); err != nil {
		t.Fatalf("UpdateInode failed: %v", err)
	}

	// Verify updated value
	inode2, err := df.GetInode(ino)
	if err != nil {
		t.Fatalf("GetInode (after update) failed: %v", err)
	}
	if inode2.Size != newSize {
		t.Errorf("inode.Size = %d, want %d", inode2.Size, newSize)
	}
}

func TestEpochAwareDentryOperations(t *testing.T) {
	df, cleanup := createTestDataFileWithEpoch(t)
	defer cleanup()

	// Create a directory at epoch 0
	dirIno, err := df.CreateInode(DefaultDirMode)
	if err != nil {
		t.Fatalf("CreateInode (dir) failed: %v", err)
	}

	// Create dentry in root
	if err := df.CreateDentry(RootIno, "testdir", dirIno); err != nil {
		t.Fatalf("CreateDentry failed: %v", err)
	}

	// Verify lookup works
	dentry, err := df.Lookup(RootIno, "testdir")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if dentry.Ino != dirIno {
		t.Errorf("dentry.Ino = %d, want %d", dentry.Ino, dirIno)
	}

	// Delete dentry (creates tombstone at current epoch)
	if err := df.DeleteDentry(RootIno, "testdir"); err != nil {
		t.Fatalf("DeleteDentry failed: %v", err)
	}

	// Verify lookup returns not found
	_, err = df.Lookup(RootIno, "testdir")
	if err == nil {
		t.Error("expected Lookup to return error after delete")
	}
}

func TestEpochAwareContentOperations(t *testing.T) {
	df, cleanup := createTestDataFileWithEpoch(t)
	defer cleanup()

	// Create a file
	ino, err := df.CreateInode(DefaultFileMode)
	if err != nil {
		t.Fatalf("CreateInode failed: %v", err)
	}

	// Write content at epoch 0
	content := []byte("Hello, World!")
	if err := df.WriteContent(ino, 0, content); err != nil {
		t.Fatalf("WriteContent failed: %v", err)
	}

	// Read content back
	data, err := df.ReadContent(ino, 0, len(content))
	if err != nil {
		t.Fatalf("ReadContent failed: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("content = %q, want %q", string(data), string(content))
	}

	// Create new epoch and update content
	newEpoch, err := df.CreateWriteEpoch("ready", "test:content_ops")
	if err != nil {
		t.Fatalf("CreateWriteEpoch failed: %v", err)
	}
	if err := df.SetCurrentWriteEpoch(newEpoch); err != nil {
		t.Fatalf("SetCurrentWriteEpoch failed: %v", err)
	}
	if err := df.RefreshEpochs(); err != nil {
		t.Fatalf("RefreshEpochs failed: %v", err)
	}

	// Write new content
	newContent := []byte("New content!")
	if err := df.WriteContent(ino, 0, newContent); err != nil {
		t.Fatalf("WriteContent (new) failed: %v", err)
	}

	// Read content back
	data2, err := df.ReadContent(ino, 0, len(newContent))
	if err != nil {
		t.Fatalf("ReadContent (new) failed: %v", err)
	}
	if string(data2) != string(newContent) {
		t.Errorf("new content = %q, want %q", string(data2), string(newContent))
	}
}

func TestEpochAwareSymlinkOperations(t *testing.T) {
	df, cleanup := createTestDataFileWithEpoch(t)
	defer cleanup()

	// Create a symlink inode
	ino, err := df.CreateInode(ModeSymlink | 0777)
	if err != nil {
		t.Fatalf("CreateInode (symlink) failed: %v", err)
	}

	// Create symlink at epoch 0
	target := "/path/to/target"
	if err := df.CreateSymlink(ino, target); err != nil {
		t.Fatalf("CreateSymlink failed: %v", err)
	}

	// Read symlink
	readTarget, err := df.ReadSymlink(ino)
	if err != nil {
		t.Fatalf("ReadSymlink failed: %v", err)
	}
	if readTarget != target {
		t.Errorf("symlink target = %q, want %q", readTarget, target)
	}
}

func TestRefreshEpochsClearsLookupCache(t *testing.T) {
	df, cleanup := createTestDataFileWithEpoch(t)
	defer cleanup()

	// Create a file and dentry so Lookup populates the cache
	ino, err := df.CreateInode(DefaultFileMode)
	if err != nil {
		t.Fatalf("CreateInode failed: %v", err)
	}
	if err := df.CreateDentry(RootIno, "cached_file", ino); err != nil {
		t.Fatalf("CreateDentry failed: %v", err)
	}

	// Lookup to populate cache
	d, err := df.Lookup(RootIno, "cached_file")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if d.Ino != ino {
		t.Fatalf("Lookup returned wrong ino: got %d, want %d", d.Ino, ino)
	}

	// Verify cache is populated
	if df.lookupCache == nil {
		t.Fatal("lookupCache should be non-nil after Lookup")
	}
	if len(df.lookupCache) != 1 {
		t.Fatalf("lookupCache should have 1 entry, got %d", len(df.lookupCache))
	}

	// RefreshEpochs should clear the cache
	if err := df.RefreshEpochs(); err != nil {
		t.Fatalf("RefreshEpochs failed: %v", err)
	}
	if df.lookupCache != nil {
		t.Error("lookupCache should be nil after RefreshEpochs")
	}
}

func TestSetCurrentWriteEpochDoesNotClearCache(t *testing.T) {
	df, cleanup := createTestDataFileWithEpoch(t)
	defer cleanup()

	// Create a file and dentry so Lookup populates the cache
	ino, err := df.CreateInode(DefaultFileMode)
	if err != nil {
		t.Fatalf("CreateInode failed: %v", err)
	}
	if err := df.CreateDentry(RootIno, "keep_cached", ino); err != nil {
		t.Fatalf("CreateDentry failed: %v", err)
	}

	// Lookup to populate cache
	if _, err := df.Lookup(RootIno, "keep_cached"); err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if df.lookupCache == nil || len(df.lookupCache) == 0 {
		t.Fatal("lookupCache should be populated after Lookup")
	}

	// Create a new epoch and set it as current write epoch
	epoch, err := df.CreateWriteEpoch("ready", "test:cache")
	if err != nil {
		t.Fatalf("CreateWriteEpoch failed: %v", err)
	}
	if err := df.SetCurrentWriteEpoch(epoch); err != nil {
		t.Fatalf("SetCurrentWriteEpoch failed: %v", err)
	}

	// Cache should still be populated — SetCurrentWriteEpoch doesn't change readableEpoch
	if df.lookupCache == nil || len(df.lookupCache) == 0 {
		t.Error("lookupCache should NOT be cleared by SetCurrentWriteEpoch alone")
	}
}

func TestLookupCacheInvalidatedOnDentryMutations(t *testing.T) {
	df, cleanup := createTestDataFileWithEpoch(t)
	defer cleanup()

	// Create file and dentry
	ino, err := df.CreateInode(DefaultFileMode)
	if err != nil {
		t.Fatalf("CreateInode failed: %v", err)
	}
	if err := df.CreateDentry(RootIno, "mutated_file", ino); err != nil {
		t.Fatalf("CreateDentry failed: %v", err)
	}

	// Lookup to populate cache
	d, err := df.Lookup(RootIno, "mutated_file")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if d.Ino != ino {
		t.Fatalf("wrong ino: got %d, want %d", d.Ino, ino)
	}

	// Delete the dentry (creates tombstone) — should invalidate cache
	if err := df.DeleteDentry(RootIno, "mutated_file"); err != nil {
		t.Fatalf("DeleteDentry failed: %v", err)
	}

	// Cache entry for this key should be gone
	key := lookupCacheKey{RootIno, "mutated_file"}
	if df.lookupCache != nil {
		if _, ok := df.lookupCache[key]; ok {
			t.Error("cache entry should be invalidated after DeleteDentry")
		}
	}

	// Lookup should now return not found (tombstone)
	_, err = df.Lookup(RootIno, "mutated_file")
	if err == nil {
		t.Error("expected Lookup to return error after DeleteDentry")
	}
}

func TestLookupCacheInvalidatedOnRename(t *testing.T) {
	df, cleanup := createTestDataFileWithEpoch(t)
	defer cleanup()

	// Create file and dentry
	ino, err := df.CreateInode(DefaultFileMode)
	if err != nil {
		t.Fatalf("CreateInode failed: %v", err)
	}
	if err := df.CreateDentry(RootIno, "old_name", ino); err != nil {
		t.Fatalf("CreateDentry failed: %v", err)
	}

	// Lookup to populate cache
	if _, err := df.Lookup(RootIno, "old_name"); err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}

	// Rename should invalidate both old and new cache entries
	if err := df.RenameDentry(RootIno, "old_name", RootIno, "new_name"); err != nil {
		t.Fatalf("RenameDentry failed: %v", err)
	}

	// Old name should be gone (tombstone)
	_, err = df.Lookup(RootIno, "old_name")
	if err == nil {
		t.Error("expected old name Lookup to fail after rename")
	}

	// New name should resolve to the same ino
	d, err := df.Lookup(RootIno, "new_name")
	if err != nil {
		t.Fatalf("Lookup new name failed: %v", err)
	}
	if d.Ino != ino {
		t.Errorf("new name ino = %d, want %d", d.Ino, ino)
	}
}

func TestCleanupDeprecatedEpochs(t *testing.T) {
	df, cleanup := createTestDataFileWithEpoch(t)
	defer cleanup()

	// Create data at epoch 0
	ino, err := df.CreateInode(DefaultFileMode)
	if err != nil {
		t.Fatalf("CreateInode failed: %v", err)
	}
	if err := df.WriteContent(ino, 0, []byte("test")); err != nil {
		t.Fatalf("WriteContent failed: %v", err)
	}

	// Create new epoch
	newEpoch, err := df.CreateWriteEpoch("ready", "test:cleanup")
	if err != nil {
		t.Fatalf("CreateWriteEpoch failed: %v", err)
	}
	if err := df.SetCurrentWriteEpoch(newEpoch); err != nil {
		t.Fatalf("SetCurrentWriteEpoch failed: %v", err)
	}

	// Mark old epoch as deprecated
	if err := df.SetEpochStatus(0, "deprecated"); err != nil {
		t.Fatalf("SetEpochStatus failed: %v", err)
	}

	// Cleanup deprecated epochs
	count, err := df.CleanupDeprecatedEpochs()
	if err != nil {
		t.Fatalf("CleanupDeprecatedEpochs failed: %v", err)
	}
	if count != 1 {
		t.Errorf("cleaned epochs = %d, want 1", count)
	}
}
