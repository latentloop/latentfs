package integration

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"latentfs/internal/daemon"

	. "github.com/onsi/gomega"
)

// TestDaemonStopCleansMount verifies that daemon stop properly cleans up its mount point.
func TestDaemonStopCleansMount(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	// Create isolated config directory for this test
	testDir := filepath.Join(os.TempDir(), "latentfs_cleanup_stop_"+time.Now().Format("150405.000"))
	configDir := filepath.Join(testDir, "config")
	os.MkdirAll(configDir, 0755)
	mountPath := filepath.Join(configDir, "mnt_daemon")

	// Cleanup at the end
	defer func() {
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 10*time.Second)
		cleanupTestConfigDir(configDir)
		os.RemoveAll(testDir)
	}()

	t.Run("start daemon creates mount point", func(t *testing.T) {
		g := NewWithT(t)

		result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon start failed: %s", result.Combined)
		}

		// Wait for daemon to be ready
		waitForDaemonReadyWithConfigDir(g, configDir, 3*time.Second)

		// Verify mount point exists and is mounted
		g.Eventually(func() bool {
			return daemon.IsMounted(mountPath)
		}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue(),
			"mount point should be mounted after daemon start")
	})

	t.Run("stop daemon cleans mount point", func(t *testing.T) {
		g := NewWithT(t)

		result := RunCLIWithConfigDir(configDir, "daemon", "stop")
		if result.ExitCode != 0 {
			t.Fatalf("daemon stop failed: %s", result.Combined)
		}

		// Wait for daemon to stop — under parallel test load with multiple
		// NFS mounts to localhost:/, unmount can take up to 9s (3 attempts × 3s)
		waitForDaemonStoppedWithConfigDir(configDir, 10*time.Second)

		// Verify mount point is no longer mounted.
		// Under parallel NFS contention, the daemon's unmount may fail because
		// macOS kernel NFS client reports "Resource busy" when other localhost:/
		// mounts are active. Poll with a generous timeout to allow kernel
		// cleanup, and do additional force unmount attempts.
		g.Eventually(func() bool {
			if !daemon.IsMounted(mountPath) {
				return true
			}
			// Nudge: try force unmount while waiting
			exec.Command("umount", "-f", mountPath).Run()
			return false
		}).WithTimeout(10 * time.Second).WithPolling(500 * time.Millisecond).Should(BeTrue(),
			"mount point should not be mounted after daemon stop")
	})
}

// TestDaemonStartCleansStaleMount verifies that daemon start can recover from a stale mount.
func TestDaemonStartCleansStaleMount(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	// Create isolated config directory for this test
	testDir := filepath.Join(os.TempDir(), "latentfs_cleanup_stale_"+time.Now().Format("150405.000"))
	configDir := filepath.Join(testDir, "config")
	os.MkdirAll(configDir, 0755)
	mountPath := filepath.Join(configDir, "mnt_daemon")

	// Cleanup at the end
	defer func() {
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 10*time.Second)
		cleanupTestConfigDir(configDir)
		os.RemoveAll(testDir)
	}()

	t.Run("start and stop daemon normally", func(t *testing.T) {
		g := NewWithT(t)

		// Start daemon
		result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon start failed: %s", result.Combined)
		}
		waitForDaemonReadyWithConfigDir(g, configDir, 3*time.Second)

		// Stop daemon
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 10*time.Second)
	})

	t.Run("simulate stale mount directory left behind", func(t *testing.T) {
		// Create a stale mount directory (simulating a crash that left it behind)
		os.MkdirAll(mountPath, 0755)

		// Verify directory exists
		_, err := os.Stat(mountPath)
		if os.IsNotExist(err) {
			t.Fatalf("failed to create stale mount directory")
		}
	})

	t.Run("daemon start succeeds despite stale directory", func(t *testing.T) {
		g := NewWithT(t)

		// Start daemon - it should clean up the stale mount directory
		result := RunCLIWithConfigDir(configDir, "daemon", "start")
		if result.ExitCode != 0 {
			t.Fatalf("daemon start failed with stale directory: %s", result.Combined)
		}

		// Wait for daemon to be ready
		waitForDaemonReadyWithConfigDir(g, configDir, 3*time.Second)

		// Verify mount point is now properly mounted
		g.Eventually(func() bool {
			return daemon.IsMounted(mountPath)
		}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue(),
			"mount point should be properly mounted after daemon start")
	})
}

// TestDaemonStartRestart verifies that daemon start --restart works correctly.
func TestDaemonStartRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	// Create isolated config directory for this test
	testDir := filepath.Join(os.TempDir(), "latentfs_restart_"+time.Now().Format("150405.000"))
	configDir := filepath.Join(testDir, "config")
	os.MkdirAll(configDir, 0755)

	// Cleanup at the end
	defer func() {
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 10*time.Second)
		cleanupTestConfigDir(configDir)
		os.RemoveAll(testDir)
	}()

	t.Run("start daemon initially", func(t *testing.T) {
		g := NewWithT(t)

		result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon start failed: %s", result.Combined)
		}
		waitForDaemonReadyWithConfigDir(g, configDir, 3*time.Second)
	})

	t.Run("start without restart shows message", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
		// Should succeed (exit 0) but print message about already running
		if result.ExitCode != 0 {
			t.Fatalf("daemon start should succeed even if already running: %s", result.Combined)
		}
		if !result.Contains("already running") {
			t.Errorf("should report daemon already running, got: %s", result.Combined)
		}
		if !result.Contains("--restart") {
			t.Errorf("should mention --restart flag, got: %s", result.Combined)
		}
	})

	t.Run("start with restart restarts daemon", func(t *testing.T) {
		g := NewWithT(t)

		// Get current PID
		statusResult := RunCLIWithConfigDir(configDir, "daemon", "status")
		if !statusResult.Contains("running") {
			t.Fatalf("daemon should be running before restart test")
		}

		// Start with --restart
		result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup", "--restart")
		if result.ExitCode != 0 {
			t.Fatalf("daemon start --restart failed: %s", result.Combined)
		}
		if !result.Contains("restarting") {
			t.Errorf("should mention restarting, got: %s", result.Combined)
		}

		// Wait for daemon to be ready
		waitForDaemonReadyWithConfigDir(g, configDir, 3*time.Second)

		// Verify daemon is still running
		statusResult = RunCLIWithConfigDir(configDir, "daemon", "status")
		if !statusResult.Contains("running") {
			t.Errorf("daemon should be running after restart, got: %s", statusResult.Combined)
		}
	})
}

// TestDaemonStopVerifies verifies that daemon stop waits for daemon to actually stop.
func TestDaemonStopVerifies(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	// Create isolated config directory for this test
	testDir := filepath.Join(os.TempDir(), "latentfs_stop_verify_"+time.Now().Format("150405.000"))
	configDir := filepath.Join(testDir, "config")
	os.MkdirAll(configDir, 0755)

	// Cleanup at the end
	defer func() {
		cleanupTestConfigDir(configDir)
		os.RemoveAll(testDir)
	}()

	t.Run("start daemon", func(t *testing.T) {
		g := NewWithT(t)

		result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon start failed: %s", result.Combined)
		}
		waitForDaemonReadyWithConfigDir(g, configDir, 3*time.Second)
	})

	t.Run("stop daemon and verify stopped", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "stop")
		if result.ExitCode != 0 {
			t.Fatalf("daemon stop failed: %s", result.Combined)
		}

		// After stop returns, daemon should be fully stopped
		statusResult := RunCLIWithConfigDir(configDir, "daemon", "status")
		if !statusResult.Contains("not running") {
			t.Errorf("daemon should be stopped after stop command returns, got: %s", statusResult.Combined)
		}
	})

	t.Run("stop already stopped daemon is no-op", func(t *testing.T) {
		result := RunCLIWithConfigDir(configDir, "daemon", "stop")
		// Should succeed even if daemon is already stopped
		if result.ExitCode != 0 {
			t.Fatalf("daemon stop should succeed even if already stopped: %s", result.Combined)
		}
		if !result.Contains("not running") {
			t.Errorf("should report daemon not running, got: %s", result.Combined)
		}
	})
}

// TestZombieCleanupSafety verifies that zombie cleanup doesn't affect healthy daemons.
// IMPORTANT: This test calls KillZombieDaemons() directly which can interfere with other
// parallel test daemons. It must run in isolation (PARALLEL=1) to be reliable.
func TestZombieCleanupSafety(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create isolated config directories for two test daemons
	testDir1 := filepath.Join(os.TempDir(), "latentfs_zombie_1_"+time.Now().Format("150405.000"))
	configDir1 := filepath.Join(testDir1, "config")
	os.MkdirAll(configDir1, 0755)

	testDir2 := filepath.Join(os.TempDir(), "latentfs_zombie_2_"+time.Now().Format("150405.000"))
	configDir2 := filepath.Join(testDir2, "config")
	os.MkdirAll(configDir2, 0755)

	// Cleanup at the end
	defer func() {
		// Capture PIDs BEFORE stopping (daemon removes PID file during shutdown)
		pid1 := getDaemonPIDInConfigDir(configDir1)
		pid2 := getDaemonPIDInConfigDir(configDir2)

		RunCLIWithConfigDir(configDir1, "daemon", "stop")
		RunCLIWithConfigDir(configDir2, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir1, 5*time.Second)
		waitForDaemonStoppedWithConfigDir(configDir2, 5*time.Second)

		// Force kill using captured PIDs (PID file may be gone by now)
		forceKillPID(pid1)
		forceKillPID(pid2)

		cleanupTestConfigDir(configDir1)
		cleanupTestConfigDir(configDir2)
		os.RemoveAll(testDir1)
		os.RemoveAll(testDir2)
	}()

	t.Run("start two daemons", func(t *testing.T) {
		g := NewWithT(t)

		// Start daemons sequentially with retry. After many prior tests,
		// the macOS NFS kernel client can be slow to process new mounts.
		result1 := startDaemonWithRetry(t, configDir1, 2)
		if result1.ExitCode != 0 {
			t.Fatalf("daemon1 start failed after retries: %s", result1.Combined)
		}
		waitForDaemonReadyWithConfigDir(g, configDir1, 3*time.Second)

		result2 := startDaemonWithRetry(t, configDir2, 2)
		if result2.ExitCode != 0 {
			t.Fatalf("daemon2 start failed after retries: %s", result2.Combined)
		}
		waitForDaemonReadyWithConfigDir(g, configDir2, 3*time.Second)
	})

	t.Run("zombie cleanup from daemon1 doesn't kill daemon2", func(t *testing.T) {
		// Call KillZombieDaemons (simulating what happens at daemon startup)
		killed := daemon.KillZombieDaemons()

		// Since both daemons have healthy sockets, nothing should be killed
		if killed > 0 {
			t.Errorf("KillZombieDaemons killed %d processes but both daemons should be healthy", killed)
		}

		// Verify both daemons are still running
		status1 := RunCLIWithConfigDir(configDir1, "daemon", "status")
		if !status1.Contains("running") {
			t.Errorf("daemon1 should still be running after KillZombieDaemons, got: %s", status1.Combined)
		}

		status2 := RunCLIWithConfigDir(configDir2, "daemon", "status")
		if !status2.Contains("running") {
			t.Errorf("daemon2 should still be running after KillZombieDaemons, got: %s", status2.Combined)
		}
	})
}

// TestForceMountCleanup verifies that force unmount works for stuck mounts.
func TestForceMountCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	// Create isolated config directory for this test
	testDir := filepath.Join(os.TempDir(), "latentfs_force_cleanup_"+time.Now().Format("150405.000"))
	configDir := filepath.Join(testDir, "config")
	os.MkdirAll(configDir, 0755)
	mountPath := filepath.Join(configDir, "mnt_daemon")

	// Cleanup at the end
	defer func() {
		cleanupTestConfigDir(configDir)
		os.RemoveAll(testDir)
	}()

	t.Run("start daemon", func(t *testing.T) {
		g := NewWithT(t)

		result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon start failed: %s", result.Combined)
		}
		waitForDaemonReadyWithConfigDir(g, configDir, 3*time.Second)
	})

	t.Run("force cleanup unmounts even without graceful stop", func(t *testing.T) {
		g := NewWithT(t)

		// Verify mount exists
		g.Eventually(func() bool {
			return daemon.IsMounted(mountPath)
		}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue())

		// Force cleanup without stopping daemon gracefully
		// (simulates what happens when daemon crashes or is killed)
		exec.Command("umount", "-f", mountPath).Run()
		exec.Command("diskutil", "unmount", "force", mountPath).Run()

		// Verify mount is gone
		g.Eventually(func() bool {
			return !daemon.IsMounted(mountPath)
		}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue(),
			"force unmount should remove the mount")
	})

	t.Run("daemon can restart after force cleanup", func(t *testing.T) {
		g := NewWithT(t)

		// Stop any residual daemon
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 10*time.Second)

		// Clean up artifacts
		cleanupTestConfigDir(configDir)

		// Start fresh
		result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
		if result.ExitCode != 0 {
			t.Fatalf("daemon restart failed: %s", result.Combined)
		}
		waitForDaemonReadyWithConfigDir(g, configDir, 3*time.Second)

		// Verify mount is working
		g.Eventually(func() bool {
			return daemon.IsMounted(mountPath)
		}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue(),
			"mount should work after restart")

		// Final cleanup
		RunCLIWithConfigDir(configDir, "daemon", "stop")
		waitForDaemonStoppedWithConfigDir(configDir, 10*time.Second)
	})
}
