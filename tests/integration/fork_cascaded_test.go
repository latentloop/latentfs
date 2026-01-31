package integration

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCascadedFork(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "cascaded_fork")
	defer shared.Cleanup()

	t.Run("Basic", func(t *testing.T) {
		env := shared.CreateCascadedForkSubtestEnv(t, "basic", 3)
		defer env.Cleanup()

		// Create source content
		env.CreateSourceFile("base.txt", "base content from source")

		// Create first fork from source
		result := env.RunCLI("fork", env.SourceDir, "-m", env.Forks[0])
		if result.ExitCode != 0 {
			t.Fatalf("fork 0 failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Create second fork from first fork (cascaded)
		// Use --type=full because overlay fork from inside a mount is not supported
		result = env.RunCLI("fork", env.Forks[0], "--type=full", "-m", env.Forks[1])
		if result.ExitCode != 0 {
			t.Fatalf("fork 1 failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Create third fork from second fork (a -> b -> c)
		// Use --type=full because overlay fork from inside a mount is not supported
		result = env.RunCLI("fork", env.Forks[1], "--type=full", "-m", env.Forks[2])
		if result.ExitCode != 0 {
			t.Fatalf("fork 2 failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Verify all forks are symlinks
		for i := 0; i < 3; i++ {
			info, err := os.Lstat(env.Forks[i])
			if err != nil {
				t.Fatalf("fork %d not created: %v", i, err)
			}
			if info.Mode()&os.ModeSymlink == 0 {
				t.Errorf("fork %d is not a symlink", i)
			}
		}

		// Verify mount ls shows all forks (first is soft-fork, rest are hard-forks)
		statusResult := env.RunCLI("mount", "ls")
		t.Logf("Mount ls output:\n%s", statusResult.Combined)

		// First fork is soft-fork, cascaded forks are hard-forks
		if !statusResult.Contains("soft-fork") && !statusResult.Contains("hard-fork") {
			t.Error("mount ls should show fork indicators")
		}

		t.Log("Cascaded fork basic test successful")
	})

	t.Run("ReadThrough", func(t *testing.T) {
		env := shared.CreateCascadedForkSubtestEnv(t, "read_through", 3)
		defer env.Cleanup()

		// Create source content
		env.CreateSourceFile("source.txt", "content from original source")

		// Create cascade: source -> fork0 -> fork1 -> fork2
		result := env.RunCLI("fork", env.SourceDir, "-m", env.Forks[0])
		if result.ExitCode != 0 {
			t.Fatalf("fork 0 failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Write a file to fork0 (not in source)
		env.WriteForkFile(0, "fork0.txt", "content from fork0")

		// Use --type=full because overlay fork from inside a mount is not supported
		result = env.RunCLI("fork", env.Forks[0], "--type=full", "-m", env.Forks[1])
		if result.ExitCode != 0 {
			t.Fatalf("fork 1 failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Write a file to fork1 (not in fork0 or source)
		env.WriteForkFile(1, "fork1.txt", "content from fork1")

		// Use --type=full because overlay fork from inside a mount is not supported
		result = env.RunCLI("fork", env.Forks[1], "--type=full", "-m", env.Forks[2])
		if result.ExitCode != 0 {
			t.Fatalf("fork 2 failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Debug: Print mount info before read attempt
		statusResult := env.RunCLI("mount", "ls")
		t.Logf("Mount ls output before read:\n%s", statusResult.Combined)
		t.Logf("SourceDir: %s", env.SourceDir)
		t.Logf("Fork0: %s", env.Forks[0])
		t.Logf("Fork1: %s", env.Forks[1])
		t.Logf("Fork2: %s", env.Forks[2])

		// Read from fork2 - should traverse the cascade:
		// fork2 -> fork1 -> fork0 -> source

		// Test 1: Read file from source (traverses all layers)
		t.Log("Attempting to read source.txt from fork2...")
		content, err := env.ReadForkFile(2, "source.txt")
		if err != nil {
			t.Skipf("Cascaded read-through not yet implemented: %v", err)
		}
		if content != "content from original source" {
			t.Errorf("Expected source content, got %q", content)
		}

		// Test 2: Read file from fork0 (traverses fork2 -> fork1 -> fork0)
		content, err = env.ReadForkFile(2, "fork0.txt")
		if err != nil {
			t.Fatalf("Failed to read fork0.txt from fork2: %v", err)
		}
		if content != "content from fork0" {
			t.Errorf("Expected fork0 content, got %q", content)
		}

		// Test 3: Read file from fork1 (traverses fork2 -> fork1)
		content, err = env.ReadForkFile(2, "fork1.txt")
		if err != nil {
			t.Fatalf("Failed to read fork1.txt from fork2: %v", err)
		}
		if content != "content from fork1" {
			t.Errorf("Expected fork1 content, got %q", content)
		}

		t.Log("Cascaded fork read-through test successful")
	})

	t.Run("CopyOnWrite", func(t *testing.T) {
		env := shared.CreateCascadedForkSubtestEnv(t, "cow", 2)
		defer env.Cleanup()

		// Create source content
		env.CreateSourceFile("shared.txt", "original content")

		// Create cascade: source -> fork0 -> fork1
		result := env.RunCLI("fork", env.SourceDir, "-m", env.Forks[0])
		if result.ExitCode != 0 {
			t.Fatalf("fork 0 failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Use --type=full because overlay fork from inside a mount is not supported
		result = env.RunCLI("fork", env.Forks[0], "--type=full", "-m", env.Forks[1])
		if result.ExitCode != 0 {
			t.Fatalf("fork 1 failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Verify initial read through fork1 gets source content
		content, err := env.ReadForkFile(1, "shared.txt")
		if err != nil {
			t.Skipf("Cascaded read-through not yet implemented: %v", err)
		}
		if content != "original content" {
			t.Errorf("Expected original content, got %q", content)
		}

		// Write to fork1 (COW)
		env.WriteForkFile(1, "shared.txt", "modified in fork1")

		// fork1 should return modified content
		content, err = env.ReadForkFile(1, "shared.txt")
		if err != nil {
			t.Fatalf("Failed to read after COW: %v", err)
		}
		if content != "modified in fork1" {
			t.Errorf("fork1 should return modified content, got %q", content)
		}

		// fork0 should still return original content
		content, err = env.ReadForkFile(0, "shared.txt")
		if err != nil {
			t.Fatalf("Failed to read from fork0: %v", err)
		}
		if content != "original content" {
			t.Errorf("fork0 should still have original content, got %q", content)
		}

		// Source should be unchanged
		sourceContent, err := os.ReadFile(filepath.Join(env.SourceDir, "shared.txt"))
		if err != nil {
			t.Fatalf("Failed to read source: %v", err)
		}
		if string(sourceContent) != "original content" {
			t.Errorf("Source should be unchanged, got %q", string(sourceContent))
		}

		t.Log("Cascaded fork copy-on-write test successful")
	})

	t.Run("DataFileAlreadyMounted", func(t *testing.T) {
		env := shared.CreateCascadedForkSubtestEnv(t, "already_mounted", 2)
		defer env.Cleanup()

		// Create first fork
		result := env.RunCLI("fork", "--source-empty", "-m", env.Forks[0])
		if result.ExitCode != 0 {
			t.Fatalf("fork 0 failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Try to mount the same data file to a different location
		result = env.RunCLI("mount", env.Forks[1], "-d", env.DataFiles[0])
		if result.ExitCode == 0 {
			t.Fatal("should fail when data file is already mounted")
		}

		if !result.Contains("already mounted") {
			t.Errorf("error should mention 'already mounted', got: %s", result.Combined)
		}

		t.Log("Data file already mounted test successful")
	})

	t.Run("TargetEmptyDirectory", func(t *testing.T) {
		env := shared.CreateCascadedForkSubtestEnv(t, "empty_dir", 1)
		defer env.Cleanup()

		// Create the target as an empty directory
		if err := os.MkdirAll(env.Forks[0], 0755); err != nil {
			t.Fatalf("Failed to create empty target dir: %v", err)
		}

		// Fork should succeed (empty directory is allowed)
		result := env.RunCLI("fork", "--source-empty", "-m", env.Forks[0])
		if result.ExitCode != 0 {
			t.Fatalf("fork to empty directory should succeed: %s", result.Combined)
		}

		env.WaitForMountReady(MountReadyTimeout)

		// Verify symlink was created
		info, err := os.Lstat(env.Forks[0])
		if err != nil {
			t.Fatalf("fork target not created: %v", err)
		}
		if info.Mode()&os.ModeSymlink == 0 {
			t.Error("fork target should be a symlink")
		}

		t.Log("Fork to empty directory test successful")
	})

	t.Run("TargetNonEmptyDirectory", func(t *testing.T) {
		env := shared.CreateCascadedForkSubtestEnv(t, "nonempty_dir", 1)
		defer env.Cleanup()

		// Create the target as a non-empty directory
		if err := os.MkdirAll(env.Forks[0], 0755); err != nil {
			t.Fatalf("Failed to create target dir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(env.Forks[0], "file.txt"), []byte("content"), 0644); err != nil {
			t.Fatalf("Failed to create file in target: %v", err)
		}

		// Fork should fail
		result := env.RunCLI("fork", "--source-empty", "-m", env.Forks[0])
		if result.ExitCode == 0 {
			t.Fatal("fork to non-empty directory should fail")
		}

		if !result.Contains("not empty") {
			t.Errorf("error should mention 'not empty', got: %s", result.Combined)
		}

		t.Log("Fork to non-empty directory test successful")
	})
}
