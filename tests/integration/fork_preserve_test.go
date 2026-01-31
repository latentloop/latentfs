package integration

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestForkPreserve groups all fork preservation tests (symlinks, permissions, chmod, COW)
// under a single shared daemon to reduce startup overhead.
func TestForkPreserve(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "fork_preserve")
	defer shared.Cleanup()

	t.Run("HardForkPreservesSymlinks", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "hard_fork_symlinks")
		defer env.Cleanup()

		// Create a regular file and a symlink to it in the source directory
		env.CreateSourceFile("real_file.txt", "real file content")
		env.CreateSourceSymlink("link_to_file.txt", "real_file.txt")

		// Create a directory and a symlink to it
		env.CreateSourceFile("subdir/nested.txt", "nested content")
		env.CreateSourceSymlink("link_to_dir", "subdir")

		// Create a broken symlink (pointing to non-existent file)
		env.CreateSourceSymlink("broken_link.txt", "nonexistent.txt")

		// Create an absolute symlink
		env.CreateSourceSymlink("absolute_link.txt", "/tmp/some_absolute_path")

		// Create hard-fork
		result := env.RunCLI("fork", "--type=full", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("hard-fork failed: %s", result.Combined)
		}

		t.Logf("Hard-fork output: %s", result.Combined)

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Verify symlink was created
		if !env.IsSymlink() {
			t.Fatal("target is not a symlink")
		}

		// Test 1: Check link_to_file.txt is a symlink (not a regular file)
		linkPath := filepath.Join(env.TargetPath, "link_to_file.txt")
		linkInfo, err := os.Lstat(linkPath)
		if err != nil {
			t.Fatalf("Lstat link_to_file.txt failed: %v", err)
		}
		if linkInfo.Mode()&os.ModeSymlink == 0 {
			t.Error("link_to_file.txt should be a symlink, not a regular file")
		}

		// Test 2: Check the symlink target is preserved correctly
		target, err := os.Readlink(linkPath)
		if err != nil {
			t.Fatalf("Readlink link_to_file.txt failed: %v", err)
		}
		if target != "real_file.txt" {
			t.Errorf("link_to_file.txt target: got %q, want %q", target, "real_file.txt")
		}

		// Test 3: Check link_to_dir is a symlink to "subdir"
		dirLinkPath := filepath.Join(env.TargetPath, "link_to_dir")
		dirLinkInfo, err := os.Lstat(dirLinkPath)
		if err != nil {
			t.Fatalf("Lstat link_to_dir failed: %v", err)
		}
		if dirLinkInfo.Mode()&os.ModeSymlink == 0 {
			t.Error("link_to_dir should be a symlink")
		}

		dirTarget, err := os.Readlink(dirLinkPath)
		if err != nil {
			t.Fatalf("Readlink link_to_dir failed: %v", err)
		}
		if dirTarget != "subdir" {
			t.Errorf("link_to_dir target: got %q, want %q", dirTarget, "subdir")
		}

		// Test 4: Check broken symlink is preserved
		brokenLinkPath := filepath.Join(env.TargetPath, "broken_link.txt")
		brokenLinkInfo, err := os.Lstat(brokenLinkPath)
		if err != nil {
			t.Fatalf("Lstat broken_link.txt failed: %v", err)
		}
		if brokenLinkInfo.Mode()&os.ModeSymlink == 0 {
			t.Error("broken_link.txt should be a symlink")
		}

		brokenTarget, err := os.Readlink(brokenLinkPath)
		if err != nil {
			t.Fatalf("Readlink broken_link.txt failed: %v", err)
		}
		if brokenTarget != "nonexistent.txt" {
			t.Errorf("broken_link.txt target: got %q, want %q", brokenTarget, "nonexistent.txt")
		}

		// Test 5: Check absolute symlink is preserved
		absLinkPath := filepath.Join(env.TargetPath, "absolute_link.txt")
		absLinkInfo, err := os.Lstat(absLinkPath)
		if err != nil {
			t.Fatalf("Lstat absolute_link.txt failed: %v", err)
		}
		if absLinkInfo.Mode()&os.ModeSymlink == 0 {
			t.Error("absolute_link.txt should be a symlink")
		}

		absTarget, err := os.Readlink(absLinkPath)
		if err != nil {
			t.Fatalf("Readlink absolute_link.txt failed: %v", err)
		}
		if absTarget != "/tmp/some_absolute_path" {
			t.Errorf("absolute_link.txt target: got %q, want %q", absTarget, "/tmp/some_absolute_path")
		}

		// Test 6: Verify the real file content is correct (symlink was not followed)
		realContent := env.ReadTargetFile("real_file.txt")
		if realContent != "real file content" {
			t.Errorf("real_file.txt content: got %q, want %q", realContent, "real file content")
		}

		t.Log("Hard-fork preserves symlinks test successful")
	})

	t.Run("SoftForkPreservesSymlinks", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "soft_fork_symlinks")
		defer env.Cleanup()

		// Create a regular file and a symlink to it in the source directory
		env.CreateSourceFile("target_file.txt", "target file content")
		env.CreateSourceSymlink("symlink_file.txt", "target_file.txt")

		// Create a relative symlink with path
		env.CreateSourceFile("data/info.txt", "info content")
		env.CreateSourceSymlink("data/link_to_info.txt", "info.txt")

		// Create soft-fork (default behavior with source)
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("soft-fork failed: %s", result.Combined)
		}

		t.Logf("Soft-fork output: %s", result.Combined)

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Verify fork target symlink was created
		if !env.IsSymlink() {
			t.Fatal("target is not a symlink")
		}

		// Test 1: Check symlink_file.txt is a symlink in the fork (read-through from source)
		linkPath := filepath.Join(env.TargetPath, "symlink_file.txt")
		linkInfo, err := os.Lstat(linkPath)
		if err != nil {
			t.Fatalf("Lstat symlink_file.txt failed: %v", err)
		}
		if linkInfo.Mode()&os.ModeSymlink == 0 {
			t.Error("symlink_file.txt should be a symlink in the soft-fork")
		}

		// Test 2: Check the symlink target is preserved
		target, err := os.Readlink(linkPath)
		if err != nil {
			t.Fatalf("Readlink symlink_file.txt failed: %v", err)
		}
		if target != "target_file.txt" {
			t.Errorf("symlink_file.txt target: got %q, want %q", target, "target_file.txt")
		}

		// Test 3: Check nested symlink
		nestedLinkPath := filepath.Join(env.TargetPath, "data", "link_to_info.txt")
		nestedLinkInfo, err := os.Lstat(nestedLinkPath)
		if err != nil {
			t.Fatalf("Lstat data/link_to_info.txt failed: %v", err)
		}
		if nestedLinkInfo.Mode()&os.ModeSymlink == 0 {
			t.Error("data/link_to_info.txt should be a symlink")
		}

		nestedTarget, err := os.Readlink(nestedLinkPath)
		if err != nil {
			t.Fatalf("Readlink data/link_to_info.txt failed: %v", err)
		}
		if nestedTarget != "info.txt" {
			t.Errorf("data/link_to_info.txt target: got %q, want %q", nestedTarget, "info.txt")
		}

		// Test 4: Can read through the symlink (follow works)
		content, err := os.ReadFile(linkPath)
		if err != nil {
			t.Fatalf("ReadFile through symlink failed: %v", err)
		}
		if string(content) != "target file content" {
			t.Errorf("content through symlink: got %q, want %q", string(content), "target file content")
		}

		t.Log("Soft-fork preserves symlinks test successful")
	})

	t.Run("HardForkAllowPartial", func(t *testing.T) {
		// Skip if running as root (root can read all files)
		if os.Getuid() == 0 {
			t.Skip("skipping test when running as root")
		}

		env := shared.CreateForkSubtestEnv(t, "hard_fork_partial")
		defer env.Cleanup()

		// Create readable files
		env.CreateSourceFile("readable.txt", "readable content")
		env.CreateSourceFile("subdir/also_readable.txt", "also readable")

		// Create an unreadable file
		unreadablePath := filepath.Join(env.SourceDir, "unreadable.txt")
		if err := os.WriteFile(unreadablePath, []byte("secret content"), 0000); err != nil {
			t.Fatal(err)
		}
		// Ensure cleanup restores permissions so we can delete it
		defer os.Chmod(unreadablePath, 0644)

		// Test 1: Hard-fork WITHOUT --allow-partial should fail
		result := env.RunCLI("fork", "--type=full", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode == 0 {
			t.Error("hard-fork without --allow-partial should fail on unreadable file")
		}
		if !result.Contains("--allow-partial") {
			t.Logf("Note: error message should suggest --allow-partial, got: %s", result.Combined)
		}

		// Clean up failed fork attempt
		os.Remove(env.DataFile)
		os.Remove(env.TargetPath)

		// Test 2: Hard-fork WITH --allow-partial should succeed
		result = env.RunCLI("fork", "--type=full", "--allow-partial", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("hard-fork with --allow-partial failed: %s", result.Combined)
		}
		t.Logf("Hard-fork output: %s", result.Combined)

		// Verify warning about skipped files
		if !result.Contains("skipped") || !result.Contains("unreadable") {
			t.Logf("Note: output should mention skipped files, got: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Verify readable files exist
		readableContent := env.ReadTargetFile("readable.txt")
		if readableContent != "readable content" {
			t.Errorf("readable.txt content = %q, want %q", readableContent, "readable content")
		}

		subContent := env.ReadTargetFile("subdir/also_readable.txt")
		if subContent != "also readable" {
			t.Errorf("subdir/also_readable.txt content = %q, want %q", subContent, "also readable")
		}

		// Verify unreadable file was skipped (doesn't exist in fork)
		unreadableInFork := filepath.Join(env.TargetPath, "unreadable.txt")
		if _, err := os.Stat(unreadableInFork); err == nil {
			t.Error("unreadable.txt should have been skipped, but it exists in fork")
		}

		t.Log("Hard-fork --allow-partial test successful")
	})

	t.Run("DataSaveAllowPartial", func(t *testing.T) {
		// Skip if running as root (root can read all files)
		if os.Getuid() == 0 {
			t.Skip("skipping test when running as root")
		}

		env := shared.CreateForkSubtestEnv(t, "save_partial")
		defer env.Cleanup()

		// Create readable files in source
		env.CreateSourceFile("readable.txt", "readable content")
		env.CreateSourceFile("subdir/also_readable.txt", "also readable")

		// Create an unreadable file in source
		unreadablePath := filepath.Join(env.SourceDir, "unreadable.txt")
		if err := os.WriteFile(unreadablePath, []byte("secret content"), 0000); err != nil {
			t.Fatal(err)
		}
		// Ensure cleanup restores permissions so we can delete it
		defer os.Chmod(unreadablePath, 0644)

		// Create overlay fork (so we can test --full snapshot with source files)
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("overlay fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Write an overlay file to the fork
		env.WriteTargetFile("overlay.txt", "overlay content")

		// Test 1: data save --full WITHOUT --allow-partial should fail (due to unreadable source file)
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "--full", "-m", "Test snapshot")
		if result.ExitCode == 0 {
			t.Error("data save --full without --allow-partial should fail on unreadable source file")
		}
		if !result.Contains("--allow-partial") {
			t.Logf("Note: error message should suggest --allow-partial, got: %s", result.Combined)
		}

		// Test 2: data save --full WITH --allow-partial should succeed
		result = env.RunCLI("data", "save", "--data-file", env.DataFile, "--full", "--allow-partial", "-m", "Partial snapshot", "-t", "v1")
		if result.ExitCode != 0 {
			t.Fatalf("data save --full with --allow-partial failed: %s", result.Combined)
		}
		t.Logf("Save output: %s", result.Combined)

		// Verify warning about skipped files
		if !result.Contains("skipped") {
			t.Logf("Note: output should mention skipped files, got: %s", result.Combined)
		}

		// Verify snapshot was created
		if !result.Contains("Created snapshot") {
			t.Error("save should confirm snapshot creation")
		}

		// Verify the snapshot has readable files (via restore output)
		// Unmount first
		env.RunCLI("unmount", env.TargetPath)
		env.WaitForUnmount(2*time.Second, env.TargetPath)

		result = env.RunCLI("data", "restore", "v1", "--data-file", env.DataFile, "-y")
		if result.ExitCode != 0 {
			t.Fatalf("restore failed: %s", result.Combined)
		}
		t.Logf("Restore output: %s", result.Combined)

		// Verify readable files are in the snapshot
		if !result.Contains("readable.txt") {
			t.Error("restore should include readable.txt")
		}
		if !result.Contains("overlay.txt") {
			t.Error("restore should include overlay.txt")
		}
		// Note: unreadable.txt should NOT be in the snapshot (it was skipped)
		// We can't easily verify absence in the restore output, but the test passes
		// if we get here with the skipped warning above

		t.Log("data save --full --allow-partial test successful")
	})

	t.Run("HardForkPreservesPermissions", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "hard_fork_permissions")
		defer env.Cleanup()

		// Create files with various permissions in source
		testCases := []struct {
			name string
			mode os.FileMode
		}{
			{"executable.sh", 0755},
			{"readonly.txt", 0444},
			{"private.key", 0600},
			{"default.txt", 0644},
			{"world_writable.txt", 0666},
		}

		for _, tc := range testCases {
			fullPath := filepath.Join(env.SourceDir, tc.name)
			if err := os.WriteFile(fullPath, []byte("content"), tc.mode); err != nil {
				t.Fatalf("Failed to create %s: %v", tc.name, err)
			}
			// Explicitly set mode to override umask effect
			if err := os.Chmod(fullPath, tc.mode); err != nil {
				t.Fatalf("Failed to chmod %s: %v", tc.name, err)
			}
		}

		// Create a directory with non-default permissions
		restrictedDir := filepath.Join(env.SourceDir, "restricted_dir")
		if err := os.Mkdir(restrictedDir, 0700); err != nil {
			t.Fatalf("Failed to create restricted_dir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(restrictedDir, "secret.txt"), []byte("secret"), 0600); err != nil {
			t.Fatalf("Failed to create secret.txt: %v", err)
		}

		// Create hard-fork
		result := env.RunCLI("fork", "--type=full", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("hard-fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Verify file permissions are preserved
		for _, tc := range testCases {
			fullPath := filepath.Join(env.TargetPath, tc.name)
			info, err := os.Stat(fullPath)
			if err != nil {
				t.Errorf("Failed to stat %s: %v", tc.name, err)
				continue
			}

			gotMode := info.Mode().Perm()
			if gotMode != tc.mode {
				t.Errorf("%s: permissions = %o, want %o", tc.name, gotMode, tc.mode)
			}
		}

		// Verify directory permissions
		dirInfo, err := os.Stat(filepath.Join(env.TargetPath, "restricted_dir"))
		if err != nil {
			t.Fatalf("Failed to stat restricted_dir: %v", err)
		}
		if dirInfo.Mode().Perm() != 0700 {
			t.Errorf("restricted_dir: permissions = %o, want %o", dirInfo.Mode().Perm(), 0700)
		}

		t.Log("Hard-fork preserves permissions test successful")
	})

	t.Run("SoftForkPreservesPermissions", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "soft_fork_permissions")
		defer env.Cleanup()

		// Create files with various permissions in source
		executablePath := filepath.Join(env.SourceDir, "script.sh")
		if err := os.WriteFile(executablePath, []byte("#!/bin/bash\necho hello"), 0755); err != nil {
			t.Fatalf("Failed to create script.sh: %v", err)
		}

		privatePath := filepath.Join(env.SourceDir, "private.key")
		if err := os.WriteFile(privatePath, []byte("secret key"), 0600); err != nil {
			t.Fatalf("Failed to create private.key: %v", err)
		}

		// Create soft-fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("soft-fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Verify executable file permissions through soft-fork
		scriptInfo, err := os.Stat(filepath.Join(env.TargetPath, "script.sh"))
		if err != nil {
			t.Fatalf("Failed to stat script.sh: %v", err)
		}
		if scriptInfo.Mode().Perm() != 0755 {
			t.Errorf("script.sh: permissions = %o, want %o", scriptInfo.Mode().Perm(), 0755)
		}

		// Verify private file permissions through soft-fork
		privateInfo, err := os.Stat(filepath.Join(env.TargetPath, "private.key"))
		if err != nil {
			t.Fatalf("Failed to stat private.key: %v", err)
		}
		if privateInfo.Mode().Perm() != 0600 {
			t.Errorf("private.key: permissions = %o, want %o", privateInfo.Mode().Perm(), 0600)
		}

		t.Log("Soft-fork preserves permissions test successful")
	})

	t.Run("ChmodThroughMount", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "chmod_mount")
		defer env.Cleanup()

		// Create empty fork
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Create a file with default permissions
		testFile := filepath.Join(env.TargetPath, "test.sh")
		if err := os.WriteFile(testFile, []byte("#!/bin/bash\necho test"), 0644); err != nil {
			t.Fatalf("Failed to create test.sh: %v", err)
		}

		// Verify initial permissions
		info, err := os.Stat(testFile)
		if err != nil {
			t.Fatalf("Failed to stat test.sh: %v", err)
		}
		if info.Mode().Perm() != 0644 {
			t.Errorf("Initial permissions = %o, want %o", info.Mode().Perm(), 0644)
		}

		// Change permissions to executable
		if err := os.Chmod(testFile, 0755); err != nil {
			t.Fatalf("Failed to chmod test.sh: %v", err)
		}
		// No sleep needed: noac mount option disables NFS attribute caching

		// Verify new permissions
		info, err = os.Stat(testFile)
		if err != nil {
			t.Fatalf("Failed to stat test.sh after chmod: %v", err)
		}
		if info.Mode().Perm() != 0755 {
			t.Errorf("After chmod permissions = %o, want %o", info.Mode().Perm(), 0755)
		}

		// Change to private mode
		if err := os.Chmod(testFile, 0600); err != nil {
			t.Fatalf("Failed to chmod test.sh to 0600: %v", err)
		}
		// No sleep needed: noac mount option disables NFS attribute caching

		info, err = os.Stat(testFile)
		if err != nil {
			t.Fatalf("Failed to stat test.sh after second chmod: %v", err)
		}
		if info.Mode().Perm() != 0600 {
			t.Errorf("After second chmod permissions = %o, want %o", info.Mode().Perm(), 0600)
		}

		t.Log("Chmod through mount test successful")
	})

	t.Run("CopyOnWritePreservesPermissions", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "cow_permissions")
		defer env.Cleanup()

		// Create an executable script in source
		scriptPath := filepath.Join(env.SourceDir, "script.sh")
		if err := os.WriteFile(scriptPath, []byte("#!/bin/bash\necho original"), 0755); err != nil {
			t.Fatalf("Failed to create script.sh: %v", err)
		}

		// Create soft-fork
		result := env.RunCLI("fork", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("soft-fork failed: %s", result.Combined)
		}

		// Wait for mount to be ready
		env.WaitForMountReady(MountReadyTimeout)

		// Verify permissions before copy-on-write
		forkScriptPath := filepath.Join(env.TargetPath, "script.sh")
		info, err := os.Stat(forkScriptPath)
		if err != nil {
			t.Fatalf("Failed to stat script.sh: %v", err)
		}
		if info.Mode().Perm() != 0755 {
			t.Errorf("Before COW: permissions = %o, want %o", info.Mode().Perm(), 0755)
		}

		// Trigger copy-on-write by writing to the file
		if err := os.WriteFile(forkScriptPath, []byte("#!/bin/bash\necho modified"), 0755); err != nil {
			t.Fatalf("Failed to write to script.sh: %v", err)
		}
		// No sleep needed: noac mount option disables NFS attribute caching

		// Verify permissions are preserved after copy-on-write
		info, err = os.Stat(forkScriptPath)
		if err != nil {
			t.Fatalf("Failed to stat script.sh after COW: %v", err)
		}
		if info.Mode().Perm() != 0755 {
			t.Errorf("After COW: permissions = %o, want %o", info.Mode().Perm(), 0755)
		}

		// Verify source file is unchanged
		sourceInfo, err := os.Stat(scriptPath)
		if err != nil {
			t.Fatalf("Failed to stat source script.sh: %v", err)
		}
		if sourceInfo.Mode().Perm() != 0755 {
			t.Errorf("Source file permissions changed: %o, want %o", sourceInfo.Mode().Perm(), 0755)
		}

		t.Log("Copy-on-write preserves permissions test successful")
	})
}
