package integration

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

// TestVFSAdvance consolidates all "target inside source" and snapshot exclusion tests.
// These tests share a single daemon to reduce lifecycle overhead.
func TestVFSAdvance(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "vfs_advance")
	defer shared.Cleanup()

	t.Run("ForkTargetInsideSource", func(t *testing.T) {
		g := NewWithT(t)

		testDir := filepath.Join(shared.TestDir, "target_inside_source")
		if err := os.MkdirAll(testDir, 0755); err != nil {
			t.Fatalf("Failed to create test directory: %v", err)
		}

		// Source folder is the test directory itself
		sourceDir := testDir
		// Target is INSIDE source folder
		targetPath := filepath.Join(sourceDir, "fork_target")
		defer func() {
			shared.RunCLI("unmount", targetPath)
			os.Remove(targetPath)
		}()

		// Create some files in the source directory
		if err := os.WriteFile(filepath.Join(sourceDir, "file1.txt"), []byte("content1"), 0644); err != nil {
			t.Fatalf("Failed to create file1.txt: %v", err)
		}
		if err := os.MkdirAll(filepath.Join(sourceDir, "subdir"), 0755); err != nil {
			t.Fatalf("Failed to create subdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(sourceDir, "subdir", "file2.txt"), []byte("content2"), 0644); err != nil {
			t.Fatalf("Failed to create file2.txt: %v", err)
		}

		// Create soft-fork with target INSIDE source
		result := shared.RunCLI("fork", sourceDir, "-m", targetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for symlink to be created and accessible
		g.Eventually(func() bool {
			info, err := os.Lstat(targetPath)
			if err != nil {
				return false
			}
			return info.Mode()&os.ModeSymlink != 0
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue(), "symlink should be created")

		// List the fork mount and verify target folder is HIDDEN
		entries, err := os.ReadDir(targetPath)
		if err != nil {
			t.Fatalf("Failed to read target directory: %v", err)
		}

		var entryNames []string
		for _, e := range entries {
			entryNames = append(entryNames, e.Name())
		}
		t.Logf("Entries visible through fork mount: %v", entryNames)

		foundFile1 := false
		foundSubdir := false
		foundForkTarget := false

		for _, name := range entryNames {
			switch name {
			case "file1.txt":
				foundFile1 = true
			case "subdir":
				foundSubdir = true
			case "fork_target":
				foundForkTarget = true
			}
		}

		if !foundFile1 {
			t.Error("file1.txt should be visible through fork mount")
		}
		if !foundSubdir {
			t.Error("subdir should be visible through fork mount")
		}
		if foundForkTarget {
			t.Error("fork_target (the symlink) should be HIDDEN to prevent infinite loop")
		}

		// Verify we can read files through the mount
		content, err := os.ReadFile(filepath.Join(targetPath, "file1.txt"))
		if err != nil {
			t.Fatalf("Failed to read file1.txt through fork: %v", err)
		}
		if string(content) != "content1" {
			t.Errorf("Expected 'content1', got %q", string(content))
		}

		content, err = os.ReadFile(filepath.Join(targetPath, "subdir", "file2.txt"))
		if err != nil {
			t.Fatalf("Failed to read subdir/file2.txt through fork: %v", err)
		}
		if string(content) != "content2" {
			t.Errorf("Expected 'content2', got %q", string(content))
		}

		// Verify attempting to access the hidden path directly fails
		_, err = os.Stat(filepath.Join(targetPath, "fork_target"))
		if err == nil {
			t.Error("Accessing fork_target through mount should fail (path should be hidden)")
		} else if !os.IsNotExist(err) {
			t.Errorf("Expected ENOENT error, got: %v", err)
		}
	})

	t.Run("ForkTargetInsideSourceNested", func(t *testing.T) {
		g := NewWithT(t)

		testDir := filepath.Join(shared.TestDir, "nested_target")
		if err := os.MkdirAll(testDir, 0755); err != nil {
			t.Fatalf("Failed to create test directory: %v", err)
		}

		sourceDir := testDir
		nestedDir := filepath.Join(sourceDir, "level1", "level2")
		if err := os.MkdirAll(nestedDir, 0755); err != nil {
			t.Fatalf("Failed to create nested directory: %v", err)
		}

		// Target is nested INSIDE source folder
		targetPath := filepath.Join(nestedDir, "fork_target")
		defer func() {
			shared.RunCLI("unmount", targetPath)
			os.Remove(targetPath)
		}()

		// Create some files
		if err := os.WriteFile(filepath.Join(sourceDir, "root.txt"), []byte("root"), 0644); err != nil {
			t.Fatalf("Failed to create root.txt: %v", err)
		}
		if err := os.WriteFile(filepath.Join(sourceDir, "level1", "level1.txt"), []byte("level1"), 0644); err != nil {
			t.Fatalf("Failed to create level1.txt: %v", err)
		}
		if err := os.WriteFile(filepath.Join(nestedDir, "level2.txt"), []byte("level2"), 0644); err != nil {
			t.Fatalf("Failed to create level2.txt: %v", err)
		}

		// Create soft-fork with nested target INSIDE source
		result := shared.RunCLI("fork", sourceDir, "-m", targetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for symlink to be created and accessible
		g.Eventually(func() bool {
			info, err := os.Lstat(targetPath)
			if err != nil {
				return false
			}
			return info.Mode()&os.ModeSymlink != 0
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue(), "symlink should be created")

		// List the nested directory through the fork mount
		nestedThroughFork := filepath.Join(targetPath, "level1", "level2")
		entries, err := os.ReadDir(nestedThroughFork)
		if err != nil {
			t.Fatalf("Failed to read nested directory through fork: %v", err)
		}

		var entryNames []string
		for _, e := range entries {
			entryNames = append(entryNames, e.Name())
		}
		t.Logf("Entries visible in level1/level2 through fork: %v", entryNames)

		foundLevel2 := false
		foundForkTarget := false

		for _, name := range entryNames {
			switch name {
			case "level2.txt":
				foundLevel2 = true
			case "fork_target":
				foundForkTarget = true
			}
		}

		if !foundLevel2 {
			t.Error("level2.txt should be visible through fork mount")
		}
		if foundForkTarget {
			t.Error("fork_target should be HIDDEN in nested directory")
		}

		// Verify we can traverse the full path
		content, err := os.ReadFile(filepath.Join(targetPath, "root.txt"))
		if err != nil {
			t.Fatalf("Failed to read root.txt: %v", err)
		}
		if string(content) != "root" {
			t.Errorf("Expected 'root', got %q", string(content))
		}

		content, err = os.ReadFile(filepath.Join(targetPath, "level1", "level2", "level2.txt"))
		if err != nil {
			t.Fatalf("Failed to read level2.txt through nested path: %v", err)
		}
		if string(content) != "level2" {
			t.Errorf("Expected 'level2', got %q", string(content))
		}
	})

	t.Run("ForkTargetOutsideSource", func(t *testing.T) {
		g := NewWithT(t)

		testDir := filepath.Join(shared.TestDir, "outside_source")
		if err := os.MkdirAll(testDir, 0755); err != nil {
			t.Fatalf("Failed to create test directory: %v", err)
		}

		// Source and target are separate (normal case)
		sourceDir := filepath.Join(testDir, "source")
		targetPath := filepath.Join(testDir, "target")

		if err := os.MkdirAll(sourceDir, 0755); err != nil {
			t.Fatalf("Failed to create source directory: %v", err)
		}
		defer func() {
			shared.RunCLI("unmount", targetPath)
			os.Remove(targetPath)
		}()

		// Create files in source
		if err := os.WriteFile(filepath.Join(sourceDir, "file1.txt"), []byte("content1"), 0644); err != nil {
			t.Fatalf("Failed to create file1.txt: %v", err)
		}

		// Create soft-fork with target OUTSIDE source (normal case)
		result := shared.RunCLI("fork", sourceDir, "-m", targetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be accessible
		g.Eventually(func() error {
			_, err := os.ReadDir(targetPath)
			return err
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		// List the fork mount
		entries, err := os.ReadDir(targetPath)
		if err != nil {
			t.Fatalf("Failed to read target directory: %v", err)
		}

		var entryNames []string
		for _, e := range entries {
			entryNames = append(entryNames, e.Name())
		}
		t.Logf("Entries visible through fork mount: %v", entryNames)

		// Should see file1.txt (nothing should be hidden)
		foundFile1 := false
		for _, name := range entryNames {
			if name == "file1.txt" {
				foundFile1 = true
			}
		}

		if !foundFile1 {
			t.Error("file1.txt should be visible through fork mount")
		}

		// Verify we can read the file
		content, err := os.ReadFile(filepath.Join(targetPath, "file1.txt"))
		if err != nil {
			t.Fatalf("Failed to read file1.txt: %v", err)
		}
		if string(content) != "content1" {
			t.Errorf("Expected 'content1', got %q", string(content))
		}
	})

	t.Run("ForkTargetInsideSourceWriteToOverlay", func(t *testing.T) {
		g := NewWithT(t)

		testDir := filepath.Join(shared.TestDir, "inside_source_write")
		if err := os.MkdirAll(testDir, 0755); err != nil {
			t.Fatalf("Failed to create test directory: %v", err)
		}

		sourceDir := testDir
		targetPath := filepath.Join(sourceDir, "fork_target")
		defer func() {
			shared.RunCLI("unmount", targetPath)
			os.Remove(targetPath)
		}()

		// Create a file in source
		if err := os.WriteFile(filepath.Join(sourceDir, "source_file.txt"), []byte("from source"), 0644); err != nil {
			t.Fatalf("Failed to create source_file.txt: %v", err)
		}

		// Create soft-fork with target INSIDE source
		result := shared.RunCLI("fork", sourceDir, "-m", targetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be accessible
		g.Eventually(func() error {
			_, err := os.ReadDir(targetPath)
			return err
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		// Write a new file through the fork mount
		newFilePath := filepath.Join(targetPath, "new_file.txt")
		if err := os.WriteFile(newFilePath, []byte("new content"), 0644); err != nil {
			t.Fatalf("Failed to write new file: %v", err)
		}

		// Read it back
		content, err := os.ReadFile(newFilePath)
		if err != nil {
			t.Fatalf("Failed to read new file: %v", err)
		}
		if string(content) != "new content" {
			t.Errorf("Expected 'new content', got %q", string(content))
		}

		// Modify a source file through the fork (copy-on-write)
		modifiedPath := filepath.Join(targetPath, "source_file.txt")
		if err := os.WriteFile(modifiedPath, []byte("modified in fork"), 0644); err != nil {
			t.Fatalf("Failed to modify source file: %v", err)
		}

		// Read through fork - should get modified content
		content, err = os.ReadFile(modifiedPath)
		if err != nil {
			t.Fatalf("Failed to read modified file: %v", err)
		}
		if string(content) != "modified in fork" {
			t.Errorf("Expected 'modified in fork', got %q", string(content))
		}

		// Read source directly - should be unchanged
		sourceContent, err := os.ReadFile(filepath.Join(sourceDir, "source_file.txt"))
		if err != nil {
			t.Fatalf("Failed to read source file directly: %v", err)
		}
		if string(sourceContent) != "from source" {
			t.Errorf("Source file should be unchanged, got %q", string(sourceContent))
		}

		// List entries - should see both source_file.txt and new_file.txt, but NOT fork_target
		entries, err := os.ReadDir(targetPath)
		if err != nil {
			t.Fatalf("Failed to list target directory: %v", err)
		}

		var entryNames []string
		for _, e := range entries {
			entryNames = append(entryNames, e.Name())
		}
		t.Logf("Entries after writes: %v", entryNames)

		foundSourceFile := false
		foundNewFile := false
		foundForkTarget := false

		for _, name := range entryNames {
			switch name {
			case "source_file.txt":
				foundSourceFile = true
			case "new_file.txt":
				foundNewFile = true
			case "fork_target":
				foundForkTarget = true
			}
		}

		if !foundSourceFile {
			t.Error("source_file.txt should be visible")
		}
		if !foundNewFile {
			t.Error("new_file.txt should be visible")
		}
		if foundForkTarget {
			t.Error("fork_target should still be hidden after writes")
		}
	})

	t.Run("SnapshotExcludesTargetInsideSource", func(t *testing.T) {
		g := NewWithT(t)

		testDir := filepath.Join(shared.TestDir, "snapshot_exclude")
		if err := os.MkdirAll(testDir, 0755); err != nil {
			t.Fatalf("Failed to create test directory: %v", err)
		}

		sourceDir := testDir
		targetPath := filepath.Join(sourceDir, "fork_target")
		dataFile := targetPath + ".latentfs"
		defer func() {
			shared.RunCLI("unmount", targetPath)
			os.Remove(targetPath)
		}()

		// Create some files in the source directory
		if err := os.WriteFile(filepath.Join(sourceDir, "source_file.txt"), []byte("source content"), 0644); err != nil {
			t.Fatalf("Failed to create source_file.txt: %v", err)
		}
		if err := os.MkdirAll(filepath.Join(sourceDir, "subdir"), 0755); err != nil {
			t.Fatalf("Failed to create subdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(sourceDir, "subdir", "nested.txt"), []byte("nested content"), 0644); err != nil {
			t.Fatalf("Failed to create nested.txt: %v", err)
		}

		// Create soft-fork with target INSIDE source
		result := shared.RunCLI("fork", sourceDir, "-m", targetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be accessible
		g.Eventually(func() error {
			_, err := os.ReadDir(targetPath)
			return err
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		// Write a file through the fork (to overlay)
		overlayFilePath := filepath.Join(targetPath, "overlay_file.txt")
		if err := os.WriteFile(overlayFilePath, []byte("overlay content"), 0644); err != nil {
			t.Fatalf("Failed to write overlay file: %v", err)
		}

		// Create a snapshot using 'data save'
		result = shared.RunCLI("data", "save", "-m", "test snapshot", "-t", "v1", "--data-file", dataFile)
		if result.ExitCode != 0 {
			t.Fatalf("data save failed: %s", result.Combined)
		}

		// Delete the overlay file to simulate testing restore
		if err := os.Remove(overlayFilePath); err != nil {
			t.Logf("Note: could not remove overlay file: %v", err)
		}

		// Restore the snapshot (with -y to skip confirmation)
		result = shared.RunCLI("data", "restore", "v1", "-y", "--data-file", dataFile)
		if result.ExitCode != 0 {
			t.Fatalf("data restore failed: %s", result.Combined)
		}

		// Verify source files are accessible through the mount (from restored snapshot).
		// CheckForExternalChanges in GetAttrByPath detects restore immediately — no retry needed.
		content, err := os.ReadFile(filepath.Join(targetPath, "source_file.txt"))
		if err != nil {
			t.Fatalf("source_file.txt should be accessible after restore: %v", err)
		}
		if string(content) != "source content" {
			t.Errorf("source_file.txt content = %q, want %q", string(content), "source content")
		}

		content, err = os.ReadFile(filepath.Join(targetPath, "subdir", "nested.txt"))
		if err != nil {
			t.Fatalf("subdir/nested.txt should be accessible after restore: %v", err)
		}
		if string(content) != "nested content" {
			t.Errorf("subdir/nested.txt content = %q, want %q", string(content), "nested content")
		}

		// Verify overlay file was restored
		content, err = os.ReadFile(filepath.Join(targetPath, "overlay_file.txt"))
		if err != nil {
			t.Fatalf("overlay_file.txt should be restored: %v", err)
		}
		if string(content) != "overlay content" {
			t.Errorf("overlay_file.txt content = %q, want %q", string(content), "overlay content")
		}

		// Verify fork_target (the symlink) is still hidden via the mount
		entries, err := os.ReadDir(targetPath)
		if err != nil {
			t.Fatalf("Failed to read target directory: %v", err)
		}
		for _, e := range entries {
			if e.Name() == "fork_target" {
				t.Error("fork_target should be hidden from listing (symlink target)")
			}
		}
	})

	t.Run("SnapshotExcludesNestedTarget", func(t *testing.T) {
		g := NewWithT(t)

		testDir := filepath.Join(shared.TestDir, "snapshot_nested_exclude")
		if err := os.MkdirAll(testDir, 0755); err != nil {
			t.Fatalf("Failed to create test directory: %v", err)
		}

		sourceDir := testDir
		nestedDir := filepath.Join(sourceDir, "level1", "level2")
		if err := os.MkdirAll(nestedDir, 0755); err != nil {
			t.Fatalf("Failed to create nested directory: %v", err)
		}

		targetPath := filepath.Join(nestedDir, "fork_target")
		dataFile := targetPath + ".latentfs"
		defer func() {
			shared.RunCLI("unmount", targetPath)
			os.Remove(targetPath)
		}()

		// Create source files at various levels
		if err := os.WriteFile(filepath.Join(sourceDir, "root.txt"), []byte("root"), 0644); err != nil {
			t.Fatalf("Failed to create root.txt: %v", err)
		}
		if err := os.WriteFile(filepath.Join(sourceDir, "level1", "level1.txt"), []byte("level1"), 0644); err != nil {
			t.Fatalf("Failed to create level1.txt: %v", err)
		}
		if err := os.WriteFile(filepath.Join(nestedDir, "level2.txt"), []byte("level2"), 0644); err != nil {
			t.Fatalf("Failed to create level2.txt: %v", err)
		}

		// Create soft-fork with nested target INSIDE source
		result := shared.RunCLI("fork", sourceDir, "-m", targetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be accessible
		g.Eventually(func() error {
			_, err := os.ReadDir(targetPath)
			return err
		}).WithTimeout(2 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		// Create a snapshot
		result = shared.RunCLI("data", "save", "-m", "nested test", "-t", "v1", "--data-file", dataFile)
		if result.ExitCode != 0 {
			t.Fatalf("data save failed: %s", result.Combined)
		}

		// Restore the snapshot (with -y to skip confirmation)
		result = shared.RunCLI("data", "restore", "v1", "-y", "--data-file", dataFile)
		if result.ExitCode != 0 {
			t.Fatalf("data restore failed: %s", result.Combined)
		}

		// Verify files at each level are accessible through the mount
		content, err := os.ReadFile(filepath.Join(targetPath, "root.txt"))
		if err != nil {
			t.Errorf("root.txt should be accessible: %v", err)
		} else if string(content) != "root" {
			t.Errorf("root.txt content = %q, want %q", string(content), "root")
		}

		content, err = os.ReadFile(filepath.Join(targetPath, "level1", "level1.txt"))
		if err != nil {
			t.Errorf("level1/level1.txt should be accessible: %v", err)
		} else if string(content) != "level1" {
			t.Errorf("level1/level1.txt content = %q, want %q", string(content), "level1")
		}

		content, err = os.ReadFile(filepath.Join(targetPath, "level1", "level2", "level2.txt"))
		if err != nil {
			t.Errorf("level1/level2/level2.txt should be accessible: %v", err)
		} else if string(content) != "level2" {
			t.Errorf("level1/level2/level2.txt content = %q, want %q", string(content), "level2")
		}

		// Verify nested fork_target is hidden via the mount
		entries, err := os.ReadDir(filepath.Join(targetPath, "level1", "level2"))
		if err != nil {
			t.Fatalf("Failed to read nested directory: %v", err)
		}
		for _, e := range entries {
			if e.Name() == "fork_target" {
				t.Error("fork_target should be hidden from listing in nested directory")
			}
		}
	})
}
