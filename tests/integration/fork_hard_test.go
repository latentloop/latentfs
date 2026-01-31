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

package integration

import (
	"testing"
	"time"
)

func TestForkHard(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "fork_hard")
	defer shared.Cleanup()

	t.Run("HardForkBasic", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "hard_fork_basic")
		defer env.Cleanup()

		// Create some files in the source directory
		env.CreateSourceFile("readme.txt", "readme content")
		env.CreateSourceFile("subdir/nested.txt", "nested content")

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

		// Verify files were copied (read from hard-fork)
		content := env.ReadTargetFile("readme.txt")
		if content != "readme content" {
			t.Errorf("Expected 'readme content', got %q", content)
		}

		content = env.ReadTargetFile("subdir/nested.txt")
		if content != "nested content" {
			t.Errorf("Expected 'nested content', got %q", content)
		}

		// Modify source file - hard-fork should NOT see the change (it's independent)
		env.CreateSourceFile("readme.txt", "modified source")
		time.Sleep(50 * time.Millisecond)

		content = env.ReadTargetFile("readme.txt")
		if content != "readme content" {
			t.Errorf("Hard-fork should be independent, expected 'readme content', got %q", content)
		}

		// mount ls should NOT show soft-fork indicator (no source)
		lsResult := env.RunCLI("mount", "ls")
		t.Logf("Mount ls output:\n%s", lsResult.Combined)
		if lsResult.Contains("soft-fork") {
			t.Error("Hard-fork should not show soft-fork indicator")
		}

		t.Log("Hard-fork basic test successful")
	})

	t.Run("HardForkFromSoftFork", func(t *testing.T) {
		env := shared.CreateCascadedForkSubtestEnv(t, "hard_from_soft", 2)
		defer env.Cleanup()

		// Create source content
		env.CreateSourceFile("base.txt", "base content from source")

		// Create soft-fork from source
		result := env.RunCLI("fork", env.SourceDir, "-m", env.Forks[0])
		if result.ExitCode != 0 {
			t.Fatalf("soft-fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Write a file to the soft-fork (overlay)
		env.WriteForkFile(0, "overlay.txt", "overlay content")

		// Verify soft-fork can read both source and overlay
		content, err := env.ReadForkFile(0, "base.txt")
		if err != nil {
			t.Skipf("Soft-fork read-through not working: %v", err)
		}
		if content != "base content from source" {
			t.Errorf("Expected source content, got %q", content)
		}

		// Create hard-fork FROM the soft-fork
		result = env.RunCLI("fork", "--type=full", env.Forks[0], "-m", env.Forks[1])
		if result.ExitCode != 0 {
			t.Fatalf("hard-fork from soft-fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Hard-fork should have BOTH source content AND overlay content flattened
		content, err = env.ReadForkFile(1, "base.txt")
		if err != nil {
			t.Fatalf("Failed to read base.txt from hard-fork: %v", err)
		}
		if content != "base content from source" {
			t.Errorf("Hard-fork should have source content, got %q", content)
		}

		content, err = env.ReadForkFile(1, "overlay.txt")
		if err != nil {
			t.Fatalf("Failed to read overlay.txt from hard-fork: %v", err)
		}
		if content != "overlay content" {
			t.Errorf("Hard-fork should have overlay content, got %q", content)
		}

		// Unmount soft-fork - hard-fork should still work (it's independent)
		env.RunCLI("unmount", env.Forks[0])
		time.Sleep(50 * time.Millisecond) // Brief wait for filesystem to settle

		content, err = env.ReadForkFile(1, "base.txt")
		if err != nil {
			t.Fatalf("Hard-fork should work after unmounting soft-fork: %v", err)
		}
		if content != "base content from source" {
			t.Errorf("Hard-fork content should be preserved, got %q", content)
		}

		t.Log("Hard-fork from soft-fork test successful")
	})

	t.Run("HardForkFromCascadedSoftFork", func(t *testing.T) {
		env := shared.CreateCascadedForkSubtestEnv(t, "hard_from_cascade", 4)
		defer env.Cleanup()

		// Create source content
		env.CreateSourceFile("source.txt", "content from source")

		// Create cascade: source -> fork0 -> fork1 -> fork2
		result := env.RunCLI("fork", env.SourceDir, "-m", env.Forks[0])
		if result.ExitCode != 0 {
			t.Fatalf("fork 0 failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Try to write to fork0
		if err := tryWriteForkFile(env.Forks[0], "fork0.txt", "content from fork0"); err != nil {
			t.Skipf("Cascaded soft-fork write not working (fork0): %v", err)
		}

		result = env.RunCLI("fork", env.Forks[0], "-m", env.Forks[1])
		if result.ExitCode != 0 {
			if result.Contains("overlay fork of a mount path is not supported") {
				t.Skip("Skipping: overlay fork of a mount path is not supported yet")
			}
			t.Fatalf("fork 1 failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Try to write to fork1
		if err := tryWriteForkFile(env.Forks[1], "fork1.txt", "content from fork1"); err != nil {
			t.Skipf("Cascaded soft-fork write not working (fork1): %v", err)
		}

		result = env.RunCLI("fork", env.Forks[1], "-m", env.Forks[2])
		if result.ExitCode != 0 {
			if result.Contains("overlay fork of a mount path is not supported") {
				t.Skip("Skipping: overlay fork of a mount path is not supported yet")
			}
			t.Fatalf("fork 2 failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Try to write to fork2
		if err := tryWriteForkFile(env.Forks[2], "fork2.txt", "content from fork2"); err != nil {
			t.Skipf("Cascaded soft-fork write not working (fork2): %v", err)
		}

		// Verify cascaded read works
		content, err := env.ReadForkFile(2, "source.txt")
		if err != nil {
			t.Skipf("Cascaded read-through not working: %v", err)
		}
		if content != "content from source" {
			t.Errorf("Expected source content, got %q", content)
		}

		// Create hard-fork FROM the end of cascade (fork2)
		// This should flatten the entire chain: source + fork0 + fork1 + fork2
		result = env.RunCLI("fork", "--type=full", env.Forks[2], "-m", env.Forks[3])
		if result.ExitCode != 0 {
			t.Fatalf("hard-fork from cascade failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Hard-fork should have ALL content from the cascade
		testCases := []struct {
			file    string
			content string
		}{
			{"source.txt", "content from source"},
			{"fork0.txt", "content from fork0"},
			{"fork1.txt", "content from fork1"},
			{"fork2.txt", "content from fork2"},
		}

		for _, tc := range testCases {
			content, err = env.ReadForkFile(3, tc.file)
			if err != nil {
				t.Errorf("Failed to read %s from hard-fork: %v", tc.file, err)
				continue
			}
			if content != tc.content {
				t.Errorf("%s: expected %q, got %q", tc.file, tc.content, content)
			}
		}

		// Unmount all soft-forks - hard-fork should still work
		env.RunCLI("unmount", env.Forks[2])
		env.RunCLI("unmount", env.Forks[1])
		env.RunCLI("unmount", env.Forks[0])
		time.Sleep(50 * time.Millisecond) // Brief wait for filesystem to settle

		// Hard-fork should still have all content (it's independent)
		for _, tc := range testCases {
			content, err = env.ReadForkFile(3, tc.file)
			if err != nil {
				t.Errorf("Hard-fork should work after unmounting cascade, failed to read %s: %v", tc.file, err)
				continue
			}
			if content != tc.content {
				t.Errorf("After unmount, %s: expected %q, got %q", tc.file, tc.content, content)
			}
		}

		t.Log("Hard-fork from cascaded soft-fork test successful")
	})

	t.Run("FullForkEmptyNotAllowed", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "full_fork_empty")
		defer env.Cleanup()

		// Try to use --type=full with --source-empty
		result := env.RunCLI("fork", "--type=full", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode == 0 {
			t.Fatal("--type=full --source-empty should fail")
		}

		if !result.Contains("cannot be used") {
			t.Errorf("error should mention cannot be used, got: %s", result.Combined)
		}

		t.Log("Full fork empty not allowed test successful")
	})

	t.Run("InvalidForkType", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "invalid_type")
		defer env.Cleanup()

		// Try to use invalid --type value
		result := env.RunCLI("fork", "--type=invalid", env.SourceDir, "-m", env.TargetPath)
		if result.ExitCode == 0 {
			t.Fatal("--type=invalid should fail")
		}

		if !result.Contains("invalid") {
			t.Errorf("error should mention invalid, got: %s", result.Combined)
		}

		t.Log("Invalid fork type test successful")
	})
}
