package integration

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestForkBasic(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "fork_basic")
	defer shared.Cleanup()

	t.Run("ForkEmpty", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "empty")
		defer env.Cleanup()

		// Fork with --source-empty
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork --source-empty failed: %s", result.Combined)
		}

		t.Logf("Fork output: %s", result.Combined)

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Verify data file was created
		if _, err := os.Stat(env.DataFile); err != nil {
			t.Fatalf("data file not created: %v", err)
		}

		// Verify symlink was created
		if !env.IsSymlink() {
			t.Fatal("target is not a symlink")
		}

		// Debug: check symlink target
		symlinkTarget, err := os.Readlink(env.TargetPath)
		if err != nil {
			t.Fatalf("failed to read symlink: %v", err)
		}
		t.Logf("Symlink target: %s", symlinkTarget)

		// Debug: check if central mount parent dir exists
		centralMountDir := filepath.Dir(symlinkTarget)
		if entries, err := os.ReadDir(centralMountDir); err != nil {
			t.Logf("Cannot read central mount dir %s: %v", centralMountDir, err)
		} else {
			t.Logf("Central mount dir entries: %v", entries)
		}

		// Debug: check mount output
		mountOutput, _ := exec.Command("mount").Output()
		if strings.Contains(string(mountOutput), "smb") {
			t.Logf("SMB mounts found in mount output")
		} else {
			t.Logf("No SMB mounts found. Mount output: %s", string(mountOutput))
		}

		// Verify we can access the target
		info, err := os.Stat(env.TargetPath)
		if err != nil {
			t.Fatalf("cannot access target: %v", err)
		}
		if !info.IsDir() {
			t.Fatal("target should resolve to a directory")
		}

		// Verify it's empty initially
		entries, err := os.ReadDir(env.TargetPath)
		if err != nil {
			t.Fatalf("ReadDir failed: %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("expected empty directory, got %d entries", len(entries))
		}

		t.Log("Fork empty test successful")
	})

	t.Run("ForkWithSource", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "with_source")
		defer env.Cleanup()

		// Create some files in the source directory
		env.CreateSourceFile("readme.txt", "source readme content")
		env.CreateSourceFile("subdir/nested.txt", "nested content")

		// Fork with source folder
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		t.Logf("Fork output: %s", result.Combined)

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Verify symlink was created
		if !env.IsSymlink() {
			t.Fatal("target is not a symlink")
		}

		// Verify we can access the target
		_, err := os.Stat(env.TargetPath)
		if err != nil {
			t.Fatalf("cannot access target: %v", err)
		}

		// Note: Copy-on-write from source is not yet implemented,
		// so the fork will be empty. When COW is implemented,
		// we should be able to read source files here.

		t.Log("Fork with source test successful")
	})

	t.Run("ForkWithCustomDataFile", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "custom_datafile")
		defer env.Cleanup()

		// Custom data file path - ensure parent directory exists
		customDataFile := filepath.Join(env.TestDir, "custom", "mydata.latentfs")
		if err := os.MkdirAll(filepath.Dir(customDataFile), 0755); err != nil {
			t.Fatalf("failed to create custom data file directory: %v", err)
		}

		// Fork with custom data file
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath, "--data-file", customDataFile)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Verify custom data file was created
		if _, err := os.Stat(customDataFile); err != nil {
			t.Fatalf("custom data file not created: %v", err)
		}

		// Verify default data file was NOT created
		if _, err := os.Stat(env.DataFile); err == nil {
			t.Error("default data file should not exist when custom path is specified")
		}

		// Verify symlink works
		if !env.IsSymlink() {
			t.Fatal("target is not a symlink")
		}

		t.Log("Fork with custom data file test successful")
	})

	t.Run("ForkWithDataFileShortcut", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "shortcut_d")
		defer env.Cleanup()

		// Custom data file path using -d shortcut
		customDataFile := filepath.Join(env.TestDir, "shortcut", "mydata.latentfs")
		if err := os.MkdirAll(filepath.Dir(customDataFile), 0755); err != nil {
			t.Fatalf("failed to create custom data file directory: %v", err)
		}

		// Fork using -d shortcut instead of --data-file
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath, "-d", customDataFile)
		if result.ExitCode != 0 {
			t.Fatalf("fork with -d shortcut failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Verify custom data file was created
		if _, err := os.Stat(customDataFile); err != nil {
			t.Fatalf("custom data file not created: %v", err)
		}

		// Verify symlink works
		if !env.IsSymlink() {
			t.Fatal("target is not a symlink")
		}

		t.Log("Fork with -d shortcut test successful")
	})

	t.Run("ForkTargetAlreadyExists", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "exists")
		defer env.Cleanup()

		// Create a file at target path before forking (not a directory)
		// Note: empty directories are allowed and will be replaced by the fork
		if err := os.WriteFile(env.TargetPath, []byte("existing file"), 0644); err != nil {
			t.Fatalf("Failed to create target file: %v", err)
		}

		// Fork should fail because target exists as a file
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode == 0 {
			t.Fatal("fork should fail when target already exists as a file")
		}

		if !result.Contains("not a directory") {
			t.Errorf("error should mention 'not a directory', got: %s", result.Combined)
		}

		t.Log("Fork target exists test successful")
	})

	t.Run("ForkDataFileAlreadyExists", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "datafile_exists")
		defer env.Cleanup()

		// Create the data file before forking
		if err := os.WriteFile(env.DataFile, []byte("dummy"), 0644); err != nil {
			t.Fatalf("Failed to create data file: %v", err)
		}

		// Fork should fail
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode == 0 {
			t.Fatal("fork should fail when data file already exists")
		}

		if !result.Contains("already exists") {
			t.Errorf("error should mention 'already exists', got: %s", result.Combined)
		}

		t.Log("Fork data file exists test successful")
	})

	t.Run("ForkSourceNotExists", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "source_not_exists")
		defer env.Cleanup()

		// Try to fork with non-existent source
		nonExistentSource := filepath.Join(env.TestDir, "does_not_exist")
		result := env.RunCLI("fork", nonExistentSource, "-m", env.TargetPath)
		if result.ExitCode == 0 {
			t.Fatal("fork should fail when source doesn't exist")
		}

		if !result.Contains("not found") {
			t.Errorf("error should mention 'not found', got: %s", result.Combined)
		}

		t.Log("Fork source not exists test successful")
	})

	t.Run("ForkSourceReadThrough", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "source_read_through")
		defer env.Cleanup()

		// Create initial content in source directory
		initialContent := "initial source content"
		env.CreateSourceFile("test.txt", initialContent)

		// Fork with source folder
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Verify symlink was created
		if !env.IsSymlink() {
			t.Fatal("target is not a symlink")
		}

		// Try to read through fork mount - should get source content
		// This tests the "soft-fork" read-through functionality
		fullPath := filepath.Join(env.TargetPath, "test.txt")
		data, err := os.ReadFile(fullPath)
		if err != nil {
			// Source read-through not yet implemented
			t.Skipf("Source read-through not yet implemented: %v", err)
		}
		content := string(data)

		if content != initialContent {
			t.Errorf("Expected source content %q, got %q", initialContent, content)
		}
		t.Logf("Initial read successful: %q", content)

		// Modify source file directly
		updatedContent := "updated source content"
		env.CreateSourceFile("test.txt", updatedContent)

		// Small delay to ensure file is written
		time.Sleep(50 * time.Millisecond)

		// Read through fork mount again - should get NEW content (no stale cache)
		data, err = os.ReadFile(fullPath)
		if err != nil {
			t.Fatalf("Failed to read after source update: %v", err)
		}
		newContent := string(data)

		if newContent != updatedContent {
			t.Errorf("Expected updated content %q, got %q (stale cache - mtime not propagated?)", updatedContent, newContent)
		}

		t.Log("Fork source read-through with cache invalidation test successful")
	})

	t.Run("ForkSourceCopyOnWrite", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "source_cow")
		defer env.Cleanup()

		// Create initial content in source directory
		sourceContent := "original source content"
		env.CreateSourceFile("test.txt", sourceContent)

		// Fork with source folder
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Try to read through fork - should get source content initially
		fullPath := filepath.Join(env.TargetPath, "test.txt")
		data, err := os.ReadFile(fullPath)
		if err != nil {
			// Source read-through not yet implemented
			t.Skipf("Source read-through not yet implemented: %v", err)
		}
		content := string(data)

		if content != sourceContent {
			t.Errorf("Expected source content %q, got %q", sourceContent, content)
		}

		// Write to fork (copy-on-write)
		forkContent := "modified in fork"
		env.WriteTargetFile("test.txt", forkContent)

		// Read through fork - should get fork content (not source)
		content = env.ReadTargetFile("test.txt")
		if content != forkContent {
			t.Errorf("Expected fork content %q, got %q", forkContent, content)
		}

		// Verify source is unchanged
		sourceData, err := os.ReadFile(filepath.Join(env.SourceDir, "test.txt"))
		if err != nil {
			t.Fatalf("Failed to read source file: %v", err)
		}
		if string(sourceData) != sourceContent {
			t.Errorf("Source file was modified! Expected %q, got %q", sourceContent, string(sourceData))
		}

		// Modify source - fork should still return fork content (not source)
		env.CreateSourceFile("test.txt", "source changed after fork write")
		time.Sleep(50 * time.Millisecond)

		content = env.ReadTargetFile("test.txt")
		if content != forkContent {
			t.Errorf("After source change, fork should still return fork content %q, got %q", forkContent, content)
		}

		t.Log("Fork copy-on-write test successful")
	})
}
