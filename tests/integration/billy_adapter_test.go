package integration

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// TestBillyAdapter groups tests that exercise the BillyAdapter code paths
// (Stat, Remove, Rename, Chmod) through the NFS mount. All operations go through
// the NFS client → go-nfs server → BillyAdapter → LatentFS pipeline.
func TestBillyAdapter(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "billy_adapter")
	defer shared.Cleanup()

	t.Run("StatFile", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "stat_file")
		defer env.Cleanup()

		env.InitDataFile()
		env.WriteFile("hello.txt", "world")

		info, err := os.Stat(filepath.Join(env.Mount, "hello.txt"))
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}
		if info.Name() != "hello.txt" {
			t.Errorf("Name = %q, want %q", info.Name(), "hello.txt")
		}
		if info.Size() != 5 {
			t.Errorf("Size = %d, want 5", info.Size())
		}
		if info.IsDir() {
			t.Error("IsDir = true, want false")
		}
	})

	t.Run("StatDirectory", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "stat_dir")
		defer env.Cleanup()

		env.InitDataFile()
		env.MkdirAll("subdir")

		info, err := os.Stat(filepath.Join(env.Mount, "subdir"))
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}
		if !info.IsDir() {
			t.Error("IsDir = false, want true")
		}
	})

	t.Run("StatNonExistent", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "stat_noent")
		defer env.Cleanup()

		env.InitDataFile()

		_, err := os.Stat(filepath.Join(env.Mount, "does_not_exist.txt"))
		if err == nil {
			t.Fatal("Stat should have failed for non-existent file")
		}
		if !os.IsNotExist(err) {
			t.Errorf("expected IsNotExist error, got: %v", err)
		}
	})

	t.Run("RemoveFile", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "remove_file")
		defer env.Cleanup()

		env.InitDataFile()
		env.WriteFile("to_delete.txt", "bye")

		if err := os.Remove(filepath.Join(env.Mount, "to_delete.txt")); err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		if env.FileExists("to_delete.txt") {
			t.Error("file should not exist after removal")
		}
	})

	t.Run("RemoveEmptyDirectory", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "remove_empty_dir")
		defer env.Cleanup()

		env.InitDataFile()
		env.MkdirAll("empty_dir")

		if err := os.Remove(filepath.Join(env.Mount, "empty_dir")); err != nil {
			t.Fatalf("Remove empty dir failed: %v", err)
		}

		if env.FileExists("empty_dir") {
			t.Error("directory should not exist after removal")
		}
	})

	t.Run("RemoveNonEmptyDirectory", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "remove_nonempty_dir")
		defer env.Cleanup()

		env.InitDataFile()
		env.WriteFile("nonempty/child.txt", "content")

		err := os.Remove(filepath.Join(env.Mount, "nonempty"))
		if err == nil {
			t.Fatal("Remove on non-empty directory should fail")
		}
		// NFS may report this as ENOTEMPTY or as an I/O error
		var pathErr *os.PathError
		if !errors.As(err, &pathErr) {
			t.Fatalf("expected *os.PathError, got %T: %v", err, err)
		}
	})

	t.Run("RemoveNonExistent", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "remove_noent")
		defer env.Cleanup()

		env.InitDataFile()

		err := os.Remove(filepath.Join(env.Mount, "ghost.txt"))
		if err == nil {
			t.Fatal("Remove should fail for non-existent file")
		}
		if !os.IsNotExist(err) {
			t.Errorf("expected IsNotExist error, got: %v", err)
		}
	})

	t.Run("RenameFile", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "rename_file")
		defer env.Cleanup()

		env.InitDataFile()
		env.WriteFile("old_name.txt", "renamed content")

		oldPath := filepath.Join(env.Mount, "old_name.txt")
		newPath := filepath.Join(env.Mount, "new_name.txt")
		if err := os.Rename(oldPath, newPath); err != nil {
			t.Fatalf("Rename failed: %v", err)
		}

		if env.FileExists("old_name.txt") {
			t.Error("old file should not exist after rename")
		}
		content := env.ReadFile("new_name.txt")
		if content != "renamed content" {
			t.Errorf("content = %q, want %q", content, "renamed content")
		}
	})

	t.Run("RenameDirectory", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "rename_dir")
		defer env.Cleanup()

		env.InitDataFile()
		env.MkdirAll("old_dir")
		env.WriteFile("old_dir/file.txt", "inside dir")

		oldPath := filepath.Join(env.Mount, "old_dir")
		newPath := filepath.Join(env.Mount, "new_dir")
		if err := os.Rename(oldPath, newPath); err != nil {
			t.Fatalf("Rename directory failed: %v", err)
		}

		if env.FileExists("old_dir") {
			t.Error("old directory should not exist after rename")
		}
		content := env.ReadFile("new_dir/file.txt")
		if content != "inside dir" {
			t.Errorf("content = %q, want %q", content, "inside dir")
		}
	})

	t.Run("ChmodFile", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "chmod_file")
		defer env.Cleanup()

		env.InitDataFile()
		env.WriteFile("chmod_test.txt", "test")

		filePath := filepath.Join(env.Mount, "chmod_test.txt")
		if err := os.Chmod(filePath, 0755); err != nil {
			t.Fatalf("Chmod failed: %v", err)
		}

		info, err := os.Stat(filePath)
		if err != nil {
			t.Fatalf("Stat after chmod failed: %v", err)
		}
		// NFS may not preserve exact mode bits, so check executable bit
		if info.Mode()&0100 == 0 {
			t.Errorf("expected executable mode, got %o", info.Mode())
		}
	})

	t.Run("ChmodDirectory", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "chmod_dir")
		defer env.Cleanup()

		env.InitDataFile()
		env.MkdirAll("chmod_dir")

		dirPath := filepath.Join(env.Mount, "chmod_dir")
		if err := os.Chmod(dirPath, 0700); err != nil {
			t.Fatalf("Chmod directory failed: %v", err)
		}

		info, err := os.Stat(dirPath)
		if err != nil {
			t.Fatalf("Stat after chmod failed: %v", err)
		}
		perm := info.Mode().Perm()
		if perm&0700 != 0700 {
			t.Errorf("expected at least 0700 permissions, got %o", perm)
		}
	})

	t.Run("LstatFile", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "lstat_file")
		defer env.Cleanup()

		env.InitDataFile()
		env.WriteFile("lstat_test.txt", "content")

		info, err := os.Lstat(filepath.Join(env.Mount, "lstat_test.txt"))
		if err != nil {
			t.Fatalf("Lstat failed: %v", err)
		}
		if info.IsDir() {
			t.Error("Lstat should report file, not directory")
		}
		if info.Size() != 7 {
			t.Errorf("Size = %d, want 7", info.Size())
		}
	})

	t.Run("RmRfOverlayDirectory", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "rm_rf_overlay")
		defer env.Cleanup()

		env.InitDataFile()

		// Create a directory tree
		env.WriteFile("rmdir/a.txt", "a")
		env.WriteFile("rmdir/b.txt", "b")
		env.WriteFile("rmdir/sub/c.txt", "c")

		// rm -rf the directory
		dirPath := filepath.Join(env.Mount, "rmdir")
		if err := os.RemoveAll(dirPath); err != nil {
			t.Fatalf("RemoveAll failed: %v", err)
		}

		if env.FileExists("rmdir") {
			t.Error("directory should not exist after RemoveAll")
		}
	})
}

// TestBillyAdapterSoftFork tests BillyAdapter operations on soft-fork mounts
// where source-only files need tombstone handling.
func TestBillyAdapterSoftFork(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "billy_adapter_fork")
	defer shared.Cleanup()

	t.Run("StatSourceOnlyFile", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "stat_source")
		defer env.Cleanup()

		env.CreateSourceFile("source.txt", "source content")
		env.ForkWithSource()

		info, err := os.Stat(filepath.Join(env.TargetPath, "source.txt"))
		if err != nil {
			t.Fatalf("Stat source-only file failed: %v", err)
		}
		if info.Size() != 14 {
			t.Errorf("Size = %d, want 14", info.Size())
		}
	})

	t.Run("RemoveSourceOnlyFile", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "remove_source")
		defer env.Cleanup()

		env.CreateSourceFile("to_delete.txt", "goodbye")
		env.CreateSourceFile("to_keep.txt", "stay")
		env.ForkWithSource()

		// Delete through the fork mount
		if err := os.Remove(filepath.Join(env.TargetPath, "to_delete.txt")); err != nil {
			t.Fatalf("Remove source-only file failed: %v", err)
		}

		// Tombstone should hide the file
		if env.TargetFileExists("to_delete.txt") {
			t.Error("deleted source-only file should not be visible")
		}

		// Other file should still be visible
		if !env.TargetFileExists("to_keep.txt") {
			t.Error("to_keep.txt should still be visible")
		}

		// Original source should be untouched
		data, err := os.ReadFile(filepath.Join(env.SourceDir, "to_delete.txt"))
		if err != nil {
			t.Fatalf("source file should still exist on disk: %v", err)
		}
		if string(data) != "goodbye" {
			t.Errorf("source content = %q, want %q", string(data), "goodbye")
		}
	})

	t.Run("RemoveSourceOnlyNonExistent", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "remove_noent_source")
		defer env.Cleanup()

		env.CreateSourceFile("exists.txt", "ok")
		env.ForkWithSource()

		err := os.Remove(filepath.Join(env.TargetPath, "not_in_source.txt"))
		if err == nil {
			t.Fatal("Remove should fail for file not in source")
		}
	})

	t.Run("StatSourceOnlyDirectory", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "stat_source_dir")
		defer env.Cleanup()

		env.CreateSourceFile("mydir/file.txt", "content")
		env.ForkWithSource()

		info, err := os.Stat(filepath.Join(env.TargetPath, "mydir"))
		if err != nil {
			t.Fatalf("Stat source-only directory failed: %v", err)
		}
		if !info.IsDir() {
			t.Error("expected directory")
		}
	})

	// Regression test for the soft-fork rm-rf bug:
	// Deleting source-only files creates tombstones + overlay parent scaffolding.
	// Then deleting the (now overlay) parent directory must succeed because
	// tombstone-only children are logically empty.
	t.Run("RmRfSourceOnlyDirectory", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "rmrf_source_dir")
		defer env.Cleanup()

		env.CreateSourceFile("cli/perf/syscall/file1.go", "package syscall")
		env.CreateSourceFile("cli/perf/syscall/file2.go", "package syscall")
		env.CreateSourceFile("cli/perf/bench.go", "package perf")
		env.CreateSourceFile("cli/main.go", "package main")
		env.ForkWithSource()

		// rm -rf the entire directory tree through the NFS mount
		dirPath := filepath.Join(env.TargetPath, "cli")
		if err := os.RemoveAll(dirPath); err != nil {
			t.Fatalf("RemoveAll on source-only directory tree failed: %v", err)
		}

		if env.TargetFileExists("cli") {
			t.Error("cli/ should not exist after RemoveAll")
		}

		// Source should be untouched
		srcCli := filepath.Join(env.SourceDir, "cli", "main.go")
		if _, err := os.Stat(srcCli); err != nil {
			t.Errorf("source file should still exist: %v", err)
		}
	})

	// Regression test for MetaFS timestamp fix:
	// MetaFS.rootAttributes() previously used time.Now() for mtime/ctime/atime,
	// causing NFS clients with noac mounts to see mtime changing on every GETATTR.
	// The client interprets changing mtime as "directory modified", invalidates its
	// name cache, and restarts path resolution — creating an infinite loop.
	// Fix: MetaFS uses a stable startTime set once at creation.
	// This test verifies rm -rf completes within a reasonable deadline (not hanging).
	t.Run("RmRfSourceOnlyWithDeadline", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "rmrf_deadline")
		defer env.Cleanup()

		// Create a moderately deep source tree
		env.CreateSourceFile("proj/src/main.go", "package main")
		env.CreateSourceFile("proj/src/util.go", "package main")
		env.CreateSourceFile("proj/src/lib/core.go", "package lib")
		env.CreateSourceFile("proj/src/lib/helper.go", "package lib")
		env.CreateSourceFile("proj/tests/main_test.go", "package main")
		env.CreateSourceFile("proj/tests/util_test.go", "package main")
		env.CreateSourceFile("proj/docs/readme.md", "# readme")
		env.CreateSourceFile("proj/docs/api.md", "# api")
		env.ForkWithSource()

		dirPath := filepath.Join(env.TargetPath, "proj")

		// The old bug caused rm -rf to hang indefinitely.
		// With the fix, this should complete in seconds.
		done := make(chan error, 1)
		go func() {
			done <- os.RemoveAll(dirPath)
		}()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("RemoveAll failed: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("RemoveAll timed out — possible MetaFS timestamp regression (NFS revalidation loop)")
		}

		if env.TargetFileExists("proj") {
			t.Error("proj/ should not exist after RemoveAll")
		}
	})

	t.Run("ChmodSourceOnlyFile", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "chmod_source")
		defer env.Cleanup()

		env.CreateSourceFile("chmod_source.txt", "content")
		env.ForkWithSource()

		// Chmod on source-only file — source-only handles don't have overlay inodes,
		// so SetAttr is expected to fail (no copy-on-write for metadata-only changes).
		// NFS may report this as EIO, EACCES, or "RPC struct is bad".
		filePath := filepath.Join(env.TargetPath, "chmod_source.txt")
		err := os.Chmod(filePath, 0755)
		if err != nil {
			t.Logf("Chmod on source-only file returned expected error: %v", err)
			return
		}
		t.Log("Chmod on source-only file succeeded (copy-on-write triggered)")
	})
}

// TestDentryTombstoneBlocksSource verifies that dentry tombstones (ino=0) in the
// overlay correctly block source-folder fallback across all VFS operations.
//
// Background: When a file is deleted in a soft-fork, GetDentry returns ErrNotFound
// for tombstones (ino=0), which is indistinguishable from "never existed." Without
// tombstone-aware guards, the VFS falls through to the source folder and exposes
// deleted files. These tests exercise each VFS operation that has source fallback.
//
// Note: These tests go through NFS → BillyAdapter → LatentFS, so they exercise
// the full stack. However, the NFS attribute cache can mask bugs by serving stale
// negative lookups. We use ReadDir (READDIRPLUS) which re-queries the server to
// verify directory contents after deletion.
func TestDentryTombstoneBlocksSource(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "tombstone_blocks")
	defer shared.Cleanup()

	// StatDeletedSourceFile: After deleting a source-only file, os.Stat should
	// return ENOENT (exercises GetAttrByPath source fallback).
	t.Run("StatDeletedSourceFile", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "stat_deleted")
		defer env.Cleanup()

		env.CreateSourceFile("file.txt", "content")
		env.CreateSourceFile("keep.txt", "stay")
		env.ForkWithSource()

		// Delete through the mount
		if err := os.Remove(filepath.Join(env.TargetPath, "file.txt")); err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		// Stat should return not-exist (tombstone blocks source fallback)
		_, err := os.Stat(filepath.Join(env.TargetPath, "file.txt"))
		if err == nil {
			t.Error("Stat on deleted source file should fail, but succeeded")
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expected ErrNotExist, got: %v", err)
		}

		// Sibling should still be visible
		if !env.TargetFileExists("keep.txt") {
			t.Error("keep.txt should still be visible")
		}
	})

	// ReadDirExcludesDeletedSourceEntries: After deleting source-only files,
	// ReadDir on the parent should not list them (exercises ReadDir + ListDentryTombstoneNames).
	t.Run("ReadDirExcludesDeletedSourceEntries", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "readdir_tombstone")
		defer env.Cleanup()

		env.CreateSourceFile("a.txt", "aaa")
		env.CreateSourceFile("b.txt", "bbb")
		env.CreateSourceFile("c.txt", "ccc")
		env.ForkWithSource()

		// Delete b.txt through the mount
		if err := os.Remove(filepath.Join(env.TargetPath, "b.txt")); err != nil {
			t.Fatalf("Remove b.txt failed: %v", err)
		}

		// ReadDir on root should list a.txt and c.txt but NOT b.txt
		entries, err := os.ReadDir(env.TargetPath)
		if err != nil {
			t.Fatalf("ReadDir failed: %v", err)
		}

		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		sort.Strings(names)

		if len(names) != 2 {
			t.Fatalf("expected 2 entries, got %d: %v", len(names), names)
		}
		if names[0] != "a.txt" || names[1] != "c.txt" {
			t.Errorf("expected [a.txt c.txt], got %v", names)
		}
	})

	// OpenDeletedSourceFile: After deleting a source-only file, opening it should
	// return ENOENT (exercises Open source fallback).
	t.Run("OpenDeletedSourceFile", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "open_deleted")
		defer env.Cleanup()

		env.CreateSourceFile("openme.txt", "data")
		env.ForkWithSource()

		// Delete through the mount
		if err := os.Remove(filepath.Join(env.TargetPath, "openme.txt")); err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		// Open should fail
		f, err := os.Open(filepath.Join(env.TargetPath, "openme.txt"))
		if err == nil {
			f.Close()
			t.Error("Open on deleted source file should fail, but succeeded")
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expected ErrNotExist, got: %v", err)
		}
	})

	// OpenDirDeletedSourceDir: After deleting a source-only directory (via rm -rf),
	// opening it should return ENOENT (exercises OpenDir source fallback).
	t.Run("OpenDirDeletedSourceDir", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "opendir_deleted")
		defer env.Cleanup()

		env.CreateSourceFile("mydir/file1.txt", "one")
		env.CreateSourceFile("mydir/file2.txt", "two")
		env.ForkWithSource()

		// rm -rf the directory
		dirPath := filepath.Join(env.TargetPath, "mydir")
		if err := os.RemoveAll(dirPath); err != nil {
			t.Fatalf("RemoveAll failed: %v", err)
		}

		// ReadDir on the deleted directory should fail
		_, err := os.ReadDir(dirPath)
		if err == nil {
			t.Error("ReadDir on deleted source directory should fail, but succeeded")
		}

		// Stat should also fail
		_, err = os.Stat(dirPath)
		if err == nil {
			t.Error("Stat on deleted source directory should fail, but succeeded")
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expected ErrNotExist, got: %v", err)
		}
	})

	// ReadDirAfterRmRfSubdir: After rm -rf on a source-only subdirectory,
	// ReadDir on the parent should not list the deleted subdirectory.
	t.Run("ReadDirAfterRmRfSubdir", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "readdir_rmrf")
		defer env.Cleanup()

		env.CreateSourceFile("keep_dir/file.txt", "keep")
		env.CreateSourceFile("del_dir/file.txt", "delete")
		env.CreateSourceFile("root.txt", "root")
		env.ForkWithSource()

		// rm -rf del_dir
		if err := os.RemoveAll(filepath.Join(env.TargetPath, "del_dir")); err != nil {
			t.Fatalf("RemoveAll del_dir failed: %v", err)
		}

		// ReadDir on root should show keep_dir and root.txt but NOT del_dir
		entries, err := os.ReadDir(env.TargetPath)
		if err != nil {
			t.Fatalf("ReadDir failed: %v", err)
		}

		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		sort.Strings(names)

		for _, name := range names {
			if name == "del_dir" {
				t.Error("del_dir should not appear in ReadDir after rm -rf")
			}
		}
		if len(names) != 2 {
			t.Errorf("expected 2 entries [keep_dir root.txt], got %d: %v", len(names), names)
		}
	})

	// LookupDeletedNestedSourceFile: After deleting a file nested in a source-only
	// directory, Stat on the nested path should fail (exercises Lookup per-component
	// tombstone check).
	t.Run("LookupDeletedNestedSourceFile", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "lookup_nested")
		defer env.Cleanup()

		env.CreateSourceFile("a/b/c.txt", "deep")
		env.CreateSourceFile("a/b/d.txt", "keep")
		env.ForkWithSource()

		// Delete c.txt (nested 2 levels deep)
		if err := os.Remove(filepath.Join(env.TargetPath, "a", "b", "c.txt")); err != nil {
			t.Fatalf("Remove nested file failed: %v", err)
		}

		// Stat on deleted nested file should fail
		_, err := os.Stat(filepath.Join(env.TargetPath, "a", "b", "c.txt"))
		if err == nil {
			t.Error("Stat on deleted nested source file should fail, but succeeded")
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expected ErrNotExist, got: %v", err)
		}

		// Sibling should still be accessible
		if !env.TargetFileExists("a/b/d.txt") {
			t.Error("a/b/d.txt should still be visible")
		}
	})
}
