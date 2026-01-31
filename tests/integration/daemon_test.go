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
	"strings"
	"testing"
	"time"

	"latentfs/internal/daemon"

	. "github.com/onsi/gomega"
)

func TestDaemonStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	// Create isolated config directory for this test
	testDir := filepath.Join(os.TempDir(), "latentfs_status_"+time.Now().Format("150405.000"))
	configDir := filepath.Join(testDir, "config")
	os.MkdirAll(configDir, 0755)

	defer func() {
		cleanupTestConfigDir(configDir)
		os.RemoveAll(testDir)
	}()

	t.Run("status when daemon not running", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "status")
		if result.ExitCode != 0 {
			t.Errorf("daemon status should succeed, got exit code %d: %s", result.ExitCode, result.Combined)
		}
		if !result.Contains("not running") {
			t.Errorf("daemon status should show 'not running', got: %s", result.Combined)
		}
	})
}

func TestDaemonStartStop(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	// Create isolated config directory for this test
	testDir := filepath.Join(os.TempDir(), "latentfs_startstop_"+time.Now().Format("150405.000"))
	configDir := filepath.Join(testDir, "config")
	os.MkdirAll(configDir, 0755)

	defer func() {
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 10*time.Second)
		cleanupTestConfigDir(configDir)
		os.RemoveAll(testDir)
	}()

	t.Run("daemon start", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon start failed: %s", result.Combined)
		}
		if !waitForDaemonRunningWithConfigDir(configDir, 3*time.Second) {
			t.Fatal("daemon did not start in time")
		}
	})

	t.Run("daemon status shows running", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "status")
		if result.ExitCode != 0 {
			t.Errorf("daemon status failed: %s", result.Combined)
		}
		if !result.Contains("running") || result.Contains("not running") {
			t.Errorf("daemon should be running, got: %s", result.Combined)
		}
	})

	t.Run("daemon stop", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "stop")
		if result.ExitCode != 0 {
			t.Errorf("daemon stop failed: %s", result.Combined)
		}
	})

	t.Run("daemon is stopped", func(t *testing.T) {
		// waitForDaemonStoppedWithConfigDir is void; check status after waiting
		waitForDaemonStoppedWithConfigDir(configDir, 10*time.Second)
		result := RunCLIWithConfigDir(configDir, "daemon", "status")
		if !result.Contains("not running") {
			t.Errorf("daemon should be stopped, got: %s", result.Combined)
		}
	})
}

func TestDaemonConfigLoginStart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	// Not parallel: shared plist resource (LaunchAgent)

	// Skip on non-macOS
	if !daemon.LaunchAgentSupported() {
		t.Skip("LaunchAgent only supported on macOS")
	}

	// Create isolated config directory for this test
	testDir := filepath.Join(os.TempDir(), "latentfs_loginstart_"+time.Now().Format("150405.000"))
	configDir := filepath.Join(testDir, "config")
	os.MkdirAll(configDir, 0755)

	defer func() {
		cleanupTestConfigDir(configDir)
		os.RemoveAll(testDir)
	}()

	// Ensure login-start is disabled initially (cleanup from previous tests)
	RunCLIWithConfigDir(configDir, "daemon", "config", "--login-start", "off")

	t.Run("enable login-start via config", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "config", "--login-start", "on")
		if result.ExitCode != 0 {
			t.Errorf("config --login-start on failed: %s", result.Combined)
		}
		if !result.Contains("enabled") {
			t.Errorf("should report enabled, got: %s", result.Combined)
		}
	})

	t.Run("status shows auto-start enabled", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "status")
		if result.ExitCode != 0 {
			t.Errorf("daemon status failed: %s", result.Combined)
		}
		if !result.Contains("Auto-start") || !result.Contains("enabled") {
			t.Errorf("status should show auto-start enabled, got: %s", result.Combined)
		}
	})

	t.Run("LaunchAgent plist exists", func(t *testing.T) {
		plistPath := daemon.LaunchAgentPath()
		if _, err := os.Stat(plistPath); os.IsNotExist(err) {
			t.Errorf("LaunchAgent plist should exist at %s", plistPath)
		}
	})

	t.Run("disable login-start via config", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "config", "--login-start", "off")
		if result.ExitCode != 0 {
			t.Errorf("config --login-start off failed: %s", result.Combined)
		}
		if !result.Contains("disabled") {
			t.Errorf("should report disabled, got: %s", result.Combined)
		}
	})

	t.Run("status shows auto-start disabled", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "status")
		if result.ExitCode != 0 {
			t.Errorf("daemon status failed: %s", result.Combined)
		}
		if !result.Contains("Auto-start") || !result.Contains("disabled") {
			t.Errorf("status should show auto-start disabled, got: %s", result.Combined)
		}
	})

	t.Run("LaunchAgent plist removed", func(t *testing.T) {
		plistPath := daemon.LaunchAgentPath()
		if _, err := os.Stat(plistPath); err == nil {
			t.Errorf("LaunchAgent plist should be removed from %s", plistPath)
		}
	})

	t.Run("invalid login-start value fails", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "config", "--login-start", "invalid")
		if result.ExitCode == 0 {
			t.Error("should fail with invalid value")
		}
		if !result.Contains("must be 'on' or 'off'") {
			t.Errorf("should show error about valid values, got: %s", result.Combined)
		}
	})
}

func TestDaemonLogging(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	// Create isolated config directory for this test
	testDir := filepath.Join(os.TempDir(), "latentfs_logging_"+time.Now().Format("150405.000"))
	configDir := filepath.Join(testDir, "config")
	os.MkdirAll(configDir, 0755)
	logPath := filepath.Join(configDir, "daemon.log")

	defer func() {
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 10*time.Second)
		cleanupTestConfigDir(configDir)
		os.RemoveAll(testDir)
	}()

	// Remove existing log file
	os.Remove(logPath)

	t.Run("start daemon without logging", func(t *testing.T) {
		// Explicitly set logging to none before starting to ensure test isolation
		// (other tests may have modified the global settings.yaml)
		RunCLIWithConfigDir(configDir, "daemon", "config", "--logging", "none")

		result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon start failed: %s", result.Combined)
		}
		if !waitForDaemonRunningWithConfigDir(configDir, 3*time.Second) {
			t.Fatal("daemon did not start in time")
		}
	})

	t.Run("log file should not be created without logging configured", func(t *testing.T) {
		// Stop daemon
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 10*time.Second)

		// Check if log exists and has content
		info, err := os.Stat(logPath)
		if err == nil && info.Size() > 0 {
			content, _ := os.ReadFile(logPath)
			if len(content) > 100 && !strings.Contains(string(content), "Daemon started") {
				t.Logf("Log file exists but may be from previous run")
			} else if strings.Contains(string(content), "Daemon started") {
				t.Errorf("Log file should not have daemon logs without logging configured")
			}
		}
	})

	// Clean up log for next test
	os.Remove(logPath)

	t.Run("daemon config --logging sets log level", func(t *testing.T) {
		// Configure logging via daemon config
		result := RunCLIWithConfigDir(configDir, "daemon", "config", "--logging", "debug")
		if result.ExitCode != 0 {
			t.Fatalf("daemon config --logging debug failed: %s", result.Combined)
		}
		if !strings.Contains(result.Combined, "Log level set to: debug") {
			t.Errorf("Expected confirmation message, got: %s", result.Combined)
		}
	})

	t.Run("start daemon with configured logging level", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon start failed: %s", result.Combined)
		}
		if !waitForDaemonRunningWithConfigDir(configDir, 3*time.Second) {
			t.Fatal("daemon did not start in time")
		}
	})

	t.Run("log file should be created with logging configured", func(t *testing.T) {
		// Stop daemon to ensure logs are flushed
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 10*time.Second)

		info, err := os.Stat(logPath)
		if os.IsNotExist(err) {
			t.Errorf("Log file should exist with logging configured")
		} else if info.Size() == 0 {
			t.Errorf("Log file should have content with logging configured")
		} else {
			content, _ := os.ReadFile(logPath)
			if !strings.Contains(string(content), "Daemon") {
				t.Errorf("Log file should contain daemon logs, got: %s", string(content[:min(200, len(content))]))
			}
		}
	})

	t.Run("daemon config shows current settings", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "config")
		if result.ExitCode != 0 {
			t.Fatalf("daemon config failed: %s", result.Combined)
		}
		if !strings.Contains(result.Combined, "Log level: debug") {
			t.Errorf("Expected current log level in output, got: %s", result.Combined)
		}
	})

	t.Run("daemon config --logging none offs logging", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "config", "--logging", "none")
		if result.ExitCode != 0 {
			t.Fatalf("daemon config --logging none failed: %s", result.Combined)
		}
		if !strings.Contains(result.Combined, "Log level set to: none") {
			t.Errorf("Expected confirmation message, got: %s", result.Combined)
		}
	})
}

func TestUnmountAll(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	env := NewForkTestEnv(t, "unmountall")
	defer env.Cleanup()

	env.StartDaemon()

	t.Run("create fork", func(t *testing.T) {
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)
	})

	t.Run("unmount --all clears mounts", func(t *testing.T) {
		result := env.RunCLI("unmount", "--all")
		if result.ExitCode != 0 {
			t.Errorf("unmount --all failed: %s", result.Combined)
		}
	})

	t.Run("daemon still running after unmount --all", func(t *testing.T) {
		g := NewWithT(t)
		g.Eventually(func() bool {
			result := env.RunCLI("daemon", "status")
			return result.Contains("running") && !result.Contains("not running")
		}).WithTimeout(2 * time.Second).WithPolling(50 * time.Millisecond).Should(BeTrue(),
			"daemon should still be running after unmount --all")
	})

	t.Run("can create new fork after unmount --all", func(t *testing.T) {
		// Use a new target path since the previous data file still exists (unmount doesn't delete data files)
		newTarget := filepath.Join(env.TestDir, "target2")
		result := env.RunCLI("fork", newTarget, "--source-empty")
		if result.ExitCode != 0 {
			t.Errorf("fork after unmount --all failed: %s", result.Combined)
		}
	})
}

func TestMultipleDaemonInstances(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	// Create two separate isolated config directories
	testDirA := filepath.Join(os.TempDir(), "latentfs_multi_a_"+time.Now().Format("150405.000"))
	configDirA := filepath.Join(testDirA, "config")
	os.MkdirAll(configDirA, 0755)

	testDirB := filepath.Join(os.TempDir(), "latentfs_multi_b_"+time.Now().Format("150405.000"))
	configDirB := filepath.Join(testDirB, "config")
	os.MkdirAll(configDirB, 0755)

	defer func() {
		// Cleanup both daemons using isolated RunCLIWithConfigDir
		RunCLIWithConfigDir(configDirA, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDirA, 3*time.Second)
		cleanupTestConfigDir(configDirA)
		os.RemoveAll(testDirA)

		RunCLIWithConfigDir(configDirB, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDirB, 3*time.Second)
		cleanupTestConfigDir(configDirB)
		os.RemoveAll(testDirB)
	}()

	t.Run("start daemon A", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDirA, "daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon A start failed: %s", result.Combined)
		}
		g := NewWithT(t)
		g.Eventually(func() bool {
			result := RunCLIWithConfigDir(configDirA, "daemon", "status")
			return result.Contains("running") && !result.Contains("not running")
		}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue())

		// Verify daemon A is running
		result = RunCLIWithConfigDir(configDirA, "daemon", "status")
		if !result.Contains("running") || result.Contains("not running") {
			t.Errorf("daemon A should be running, got: %s", result.Combined)
		}
	})

	t.Run("start daemon B with different LATENTFS_CONFIG_DIR", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDirB, "daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon B start failed: %s", result.Combined)
		}
		g := NewWithT(t)
		g.Eventually(func() bool {
			result := RunCLIWithConfigDir(configDirB, "daemon", "status")
			return result.Contains("running") && !result.Contains("not running")
		}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue())

		// Verify daemon B is running
		result = RunCLIWithConfigDir(configDirB, "daemon", "status")
		if !result.Contains("running") || result.Contains("not running") {
			t.Errorf("daemon B should be running, got: %s", result.Combined)
		}
	})

	t.Run("both daemons have separate files", func(t *testing.T) {
		// Daemon A files (fixed name "daemon" in its config dir)
		_, err := os.Stat(filepath.Join(configDirA, "daemon.pid"))
		if err != nil {
			t.Errorf("daemon.pid should exist in configDirA: %v", err)
		}
		_, err = os.Stat(filepath.Join(configDirA, "daemon.sock"))
		if err != nil {
			t.Errorf("daemon.sock should exist in configDirA: %v", err)
		}

		// Daemon B files (fixed name "daemon" in its config dir)
		_, err = os.Stat(filepath.Join(configDirB, "daemon.pid"))
		if err != nil {
			t.Errorf("daemon.pid should exist in configDirB: %v", err)
		}
		_, err = os.Stat(filepath.Join(configDirB, "daemon.sock"))
		if err != nil {
			t.Errorf("daemon.sock should exist in configDirB: %v", err)
		}
	})

	t.Run("stop daemon A does not affect daemon B", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDirA, "daemon", "stop")
		if result.ExitCode != 0 {
			t.Errorf("daemon A stop failed: %s", result.Combined)
		}
		waitForDaemonStoppedWithConfigDir(configDirA, 3*time.Second)

		// Daemon A should be stopped
		result = RunCLIWithConfigDir(configDirA, "daemon", "status")
		if !result.Contains("not running") {
			t.Errorf("daemon A should be stopped, got: %s", result.Combined)
		}

		// Daemon B should still be running
		result = RunCLIWithConfigDir(configDirB, "daemon", "status")
		if !result.Contains("running") || result.Contains("not running") {
			t.Errorf("daemon B should still be running, got: %s", result.Combined)
		}
	})

	t.Run("stop daemon B", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDirB, "daemon", "stop")
		if result.ExitCode != 0 {
			t.Errorf("daemon B stop failed: %s", result.Combined)
		}
		waitForDaemonStoppedWithConfigDir(configDirB, 3*time.Second)

		result = RunCLIWithConfigDir(configDirB, "daemon", "status")
		if !result.Contains("not running") {
			t.Errorf("daemon B should be stopped, got: %s", result.Combined)
		}
	})
}

func TestDaemonMountOnStart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	env := NewForkTestEnv(t, "mount_on_start")
	defer env.Cleanup()

	// Enable mount_on_start via settings
	t.Run("enable mount_on_start", func(t *testing.T) {
		// Write settings.yaml directly to the isolated config directory
		// (can't use daemon.SaveGlobalSettings because it uses process environment, not test's configDir)
		settingsPath := filepath.Join(env.configDir, "settings.yaml")
		settingsContent := `# LatentFS daemon settings
mount_on_start: true
`
		if err := os.WriteFile(settingsPath, []byte(settingsContent), 0600); err != nil {
			t.Fatalf("Failed to write settings: %v", err)
		}
	})

	// Start daemon and create a fork
	t.Run("create mount", func(t *testing.T) {
		env.StartDaemon()

		// Create a fork (this creates a data file and mounts it)
		result := env.RunCLI("fork", "--source-empty", "-m", env.TargetPath)
		if result.ExitCode != 0 {
			t.Fatalf("fork failed: %s", result.Combined)
		}
		env.WaitForMountReady(MountReadyTimeout)

		// Write a test file
		testFile := filepath.Join(env.TargetPath, "test.txt")
		if err := os.WriteFile(testFile, []byte("mount_on_start test"), 0644); err != nil {
			t.Fatalf("Failed to write test file: %v", err)
		}
	})

	// Get the data file path before stopping
	dataFilePath := env.DataFile

	t.Run("stop daemon preserves metadata", func(t *testing.T) {
		// Stop the daemon
		result := env.RunCLI("daemon", "stop")
		if result.ExitCode != 0 {
			t.Fatalf("daemon stop failed: %s", result.Combined)
		}
		waitForDaemonStoppedWithConfigDir(env.configDir, 5*time.Second)

		// Verify symlink is gone (cleanup during stop)
		if _, err := os.Lstat(env.TargetPath); !os.IsNotExist(err) {
			t.Errorf("symlink should be removed after daemon stop, err: %v", err)
		}

		// Verify data file still exists
		if _, err := os.Stat(dataFilePath); os.IsNotExist(err) {
			t.Fatalf("data file should still exist after daemon stop")
		}
	})

	t.Run("restart daemon restores mount", func(t *testing.T) {
		// Start daemon again
		result := env.RunCLI("daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon start failed: %s", result.Combined)
		}
		if !waitForDaemonRunningWithConfigDir(env.configDir, 5*time.Second) {
			t.Fatal("daemon did not start in time")
		}

		// Wait for mount to be restored
		g := NewWithT(t)
		g.Eventually(func() error {
			_, err := os.ReadDir(env.TargetPath)
			return err
		}).WithTimeout(MountReadyTimeout).WithPolling(50 * time.Millisecond).Should(Succeed(),
			"mount should be restored after daemon restart with mount_on_start=true")

		// Verify symlink points to correct location
		info, err := os.Lstat(env.TargetPath)
		if err != nil {
			t.Fatalf("Failed to stat restored mount: %v", err)
		}
		if info.Mode()&os.ModeSymlink == 0 {
			t.Errorf("restored mount should be a symlink")
		}

		// Verify test file content is preserved
		testFile := filepath.Join(env.TargetPath, "test.txt")
		content, err := os.ReadFile(testFile)
		if err != nil {
			t.Fatalf("Failed to read test file after restore: %v", err)
		}
		if string(content) != "mount_on_start test" {
			t.Errorf("file content mismatch: got %q, want %q", string(content), "mount_on_start test")
		}
	})

	t.Run("disable mount_on_start", func(t *testing.T) {
		// Write settings.yaml directly to the isolated config directory
		settingsPath := filepath.Join(env.configDir, "settings.yaml")
		settingsContent := `# LatentFS daemon settings
mount_on_start: false
`
		if err := os.WriteFile(settingsPath, []byte(settingsContent), 0600); err != nil {
			t.Fatalf("Failed to write settings: %v", err)
		}
	})

	t.Run("stop daemon removes metadata when mount_on_start=false", func(t *testing.T) {
		// Stop the daemon
		result := env.RunCLI("daemon", "stop")
		if result.ExitCode != 0 {
			t.Fatalf("daemon stop failed: %s", result.Combined)
		}
		waitForDaemonStoppedWithConfigDir(env.configDir, 5*time.Second)
	})

	t.Run("restart daemon does not restore mount when mount_on_start=false", func(t *testing.T) {
		// Start daemon again
		result := env.RunCLI("daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon start failed: %s", result.Combined)
		}
		if !waitForDaemonRunningWithConfigDir(env.configDir, 5*time.Second) {
			t.Fatal("daemon did not start in time")
		}

		// Brief wait to ensure any auto-restore would have happened
		time.Sleep(200 * time.Millisecond)

		// Verify symlink is NOT restored
		if _, err := os.Lstat(env.TargetPath); !os.IsNotExist(err) {
			t.Errorf("symlink should NOT be restored when mount_on_start=false")
		}

		// Verify daemon status shows no mounts
		result = env.RunCLI("daemon", "status")
		if result.Contains(env.TargetPath) {
			t.Errorf("daemon status should not show the mount when mount_on_start=false")
		}
	})
}

func TestDaemonConfigLiveReload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	// Create isolated config directory for this test
	testDir := filepath.Join(os.TempDir(), "latentfs_livereload_"+time.Now().Format("150405.000"))
	configDir := filepath.Join(testDir, "config")
	os.MkdirAll(configDir, 0755)
	logPath := filepath.Join(configDir, "daemon.log")

	defer func() {
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 10*time.Second)
		cleanupTestConfigDir(configDir)
		os.RemoveAll(testDir)
	}()

	// Remove existing log file
	os.Remove(logPath)

	t.Run("start daemon without logging", func(t *testing.T) {
		// Set logging to none first
		RunCLIWithConfigDir(configDir, "daemon", "config", "--logging", "none")

		result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon start failed: %s", result.Combined)
		}
		if !waitForDaemonRunningWithConfigDir(configDir, 3*time.Second) {
			t.Fatal("daemon did not start in time")
		}
	})

	t.Run("log file should not exist initially", func(t *testing.T) {
		// Small delay to allow any potential log writes
		time.Sleep(100 * time.Millisecond)

		info, err := os.Stat(logPath)
		if err == nil && info.Size() > 0 {
			content, _ := os.ReadFile(logPath)
			if strings.Contains(string(content), "Daemon started") {
				t.Errorf("Log file should not have daemon logs, got: %s", string(content[:min(200, len(content))]))
			}
		}
	})

	t.Run("daemon config --logging while running ons logging", func(t *testing.T) {
		// Configure logging while daemon is running
		result := RunCLIWithConfigDir(configDir, "daemon", "config", "--logging", "debug")
		if result.ExitCode != 0 {
			t.Fatalf("daemon config --logging debug failed: %s", result.Combined)
		}
		if !strings.Contains(result.Combined, "Daemon notified to reload configuration") {
			t.Errorf("Expected reload notification, got: %s", result.Combined)
		}
	})

	t.Run("log file should be created after live config reload", func(t *testing.T) {
		// Give daemon time to process the reload and write logs
		time.Sleep(200 * time.Millisecond)

		info, err := os.Stat(logPath)
		if os.IsNotExist(err) {
			t.Errorf("Log file should exist after live config reload")
		} else if info.Size() == 0 {
			t.Errorf("Log file should have content after live config reload")
		} else {
			content, _ := os.ReadFile(logPath)
			if !strings.Contains(string(content), "handleReloadConfig") {
				t.Errorf("Log file should contain reload log, got: %s", string(content[:min(200, len(content))]))
			}
		}
	})

	t.Run("daemon config --logging none while running offs logging", func(t *testing.T) {
		// Truncate log file to start fresh
		os.Truncate(logPath, 0)

		result := RunCLIWithConfigDir(configDir, "daemon", "config", "--logging", "none")
		if result.ExitCode != 0 {
			t.Fatalf("daemon config --logging none failed: %s", result.Combined)
		}

		// Small delay and then check log wasn't written to (except the reload message)
		time.Sleep(200 * time.Millisecond)

		// Run a command that would normally generate logs
		RunCLIWithConfigDir(configDir, "daemon", "status")

		// After disabling, no new logs should be written
		time.Sleep(100 * time.Millisecond)
		info, err := os.Stat(logPath)
		if err == nil && info.Size() > 500 {
			// Only a small amount of logging for the reload itself is acceptable
			content, _ := os.ReadFile(logPath)
			if strings.Contains(string(content), "handleStatus") {
				t.Errorf("Should not log handleStatus after disabling, got: %s", string(content))
			}
		}
	})
}
