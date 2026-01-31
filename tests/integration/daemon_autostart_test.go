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
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

// TestDaemonAutoStart tests that the daemon is automatically started when running
// commands that require it. This tests the PersistentPreRunE auto-start path.
//
// IMPORTANT: These tests exercise isolated test daemons via LATENTFS_CONFIG_DIR
// to verify the actual user experience when running CLI commands.
func TestDaemonAutoStart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	// Create isolated config directory for this test
	testDir := filepath.Join(os.TempDir(), "latentfs_autostart_"+time.Now().Format("150405.000"))
	configDir := filepath.Join(testDir, "config")
	os.MkdirAll(configDir, 0755)

	defer func() {
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 5*time.Second)
		cleanupTestConfigDir(configDir)
		os.RemoveAll(testDir)
	}()

	// Verify daemon is not running
	result := RunCLIWithConfigDir(configDir, "daemon", "status")
	if !result.Contains("not running") {
		t.Fatalf("daemon should not be running before test, got: %s", result.Combined)
	}

	t.Run("fork auto-starts daemon", func(t *testing.T) {
		g := NewWithT(t)

		// Create test directory for fork target
		forkDir := filepath.Join(testDir, "fork_test")
		os.MkdirAll(forkDir, 0755)

		targetPath := filepath.Join(forkDir, "target")
		dataFile := targetPath + ".latentfs"

		// Run fork without explicitly starting daemon first
		// This should trigger autoStartDaemon() in PersistentPreRunE
		result := RunCLIWithConfigDir(configDir, "fork", "--source-empty", "-m", targetPath)

		// Check for the warning message that indicates autoStartDaemon failed
		if result.Contains("could not auto-start daemon") {
			t.Errorf("autoStartDaemon() failed - this indicates the bug where 'daemon run' is called instead of 'daemon start'. Output: %s", result.Combined)
		}

		// Fork should still succeed (either via auto-start or fork's own startDaemon call)
		if result.ExitCode != 0 {
			t.Fatalf("fork --source-empty failed: %s", result.Combined)
		}

		// Verify daemon is now running
		g.Eventually(func() bool {
			result := RunCLIWithConfigDir(configDir, "daemon", "status")
			return result.Contains("running") && !result.Contains("not running")
		}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue(),
			"daemon should be running after fork")

		// Verify the fork was created successfully
		_, err := os.ReadDir(targetPath)
		if err != nil {
			t.Errorf("target path should be accessible: %v", err)
		}

		// Cleanup
		RunCLIWithConfigDir(configDir, "unmount", targetPath)
		os.Remove(dataFile)
	})

	t.Run("unmount command auto-starts daemon", func(t *testing.T) {
		g := NewWithT(t)

		// Stop daemon
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 5*time.Second)

		// Verify daemon is not running
		result := RunCLIWithConfigDir(configDir, "daemon", "status")
		if !result.Contains("not running") {
			t.Fatalf("daemon should not be running, got: %s", result.Combined)
		}

		// Run 'unmount --all' without explicitly starting daemon first
		// This should trigger auto-start in PersistentPreRunE
		result = RunCLIWithConfigDir(configDir, "unmount", "--all")

		// Check for the warning message (should NOT appear with fix)
		if result.Contains("could not auto-start daemon") {
			t.Errorf("autoStartDaemon() failed - indicates 'daemon run' bug. Output: %s", result.Combined)
		}

		// Should see "Starting daemon..." notification
		if !result.Contains("Starting daemon") {
			t.Logf("Note: 'Starting daemon...' notification not found in output: %s", result.Combined)
		}

		// Verify daemon is running (auto-started by PersistentPreRunE)
		g.Eventually(func() bool {
			result := RunCLIWithConfigDir(configDir, "daemon", "status")
			return result.Contains("running") && !result.Contains("not running")
		}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue(),
			"daemon should be running after unmount --all")
	})

	// Regression test: autoStartDaemon() must call 'daemon start' not 'daemon run'
	t.Run("mount ls auto-starts daemon correctly", func(t *testing.T) {
		g := NewWithT(t)

		// Stop daemon
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 5*time.Second)

		// Run 'mount ls' which triggers PersistentPreRunE
		result := RunCLIWithConfigDir(configDir, "mount", "ls")

		// The warning "could not auto-start daemon" indicates the bug:
		// autoStartDaemon() calls 'daemon run' which doesn't exist
		if result.Contains("could not auto-start daemon") {
			t.Errorf("BUG DETECTED: autoStartDaemon() is calling a non-existent command. "+
				"It should call 'daemon start' not 'daemon run'. Output: %s", result.Combined)
		}

		// Daemon should be running now (auto-started by PersistentPreRunE)
		g.Eventually(func() bool {
			result := RunCLIWithConfigDir(configDir, "daemon", "status")
			return result.Contains("running") && !result.Contains("not running")
		}).WithTimeout(5 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue(),
			"daemon should be auto-started by mount ls command")
	})
}

// TestNoAutoStartForDaemonCommands verifies that daemon subcommands don't trigger
// auto-start (they manage daemon lifecycle themselves).
func TestNoAutoStartForDaemonCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	// Create isolated config directory for this test
	testDir := filepath.Join(os.TempDir(), "latentfs_noautostart_"+time.Now().Format("150405.000"))
	configDir := filepath.Join(testDir, "config")
	os.MkdirAll(configDir, 0755)

	defer func() {
		cleanupTestConfigDir(configDir)
		os.RemoveAll(testDir)
	}()

	// Run 'daemon status' - should NOT trigger auto-start
	result := RunCLIWithConfigDir(configDir, "daemon", "status")

	// Should report not running (no auto-start for daemon commands)
	if !result.Contains("not running") {
		t.Errorf("daemon status should report 'not running' without auto-starting daemon, got: %s", result.Combined)
	}

	// Verify daemon is still not running
	result = RunCLIWithConfigDir(configDir, "daemon", "status")
	if !result.Contains("not running") {
		t.Errorf("daemon should still not be running after 'daemon status', got: %s", result.Combined)
	}
}
