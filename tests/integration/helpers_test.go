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

// Package integration provides test helpers for latentfs integration tests.
//
// # Architecture Overview
//
// This file contains shared test infrastructure organized into these categories:
//
// ## Test Environments
//
//   - TestEnv: Basic test environment with isolated daemon, data file, and mount
//   - ForkTestEnv: Environment for testing fork operations (soft-fork, hard-fork)
//   - CascadedForkTestEnv: Environment for testing cascaded fork chains
//   - SharedDaemonTestEnv: Shares a single daemon across multiple subtests (efficiency)
//   - SubtestEnv: Isolated environment within SharedDaemonTestEnv
//
// ## CLI Execution
//
// All CLI execution flows through RunCLIWithConfigDir() for test isolation:
//
//	RunCLIWithConfigDir("", ...)       → Default daemon (for daemon lifecycle tests)
//	env.RunCLI() → RunCLIWithConfigDir(env.configDir, ...)  → Isolated daemon
//
// IMPORTANT: Never use os.Setenv("LATENTFS_DAEMON", ...) in tests.
// This causes race conditions in parallel tests.
//
// ## Wait Helpers (Inner/Outer Loop Design)
//
// All wait functions use a unified inner/outer loop pattern for consistency:
//
//	Outer loop: Controls total timeout
//	Inner loop: Polls at short intervals (50ms default)
//	Logging:    Logs status every 1s during wait for troubleshooting
//	Timeout:    Logs final status when timeout occurs
//
// Available wait functions:
//
//	waitFor()                     - Core unified wait helper with logging
//	waitForDaemonRunning()        - Wait for daemon to report "running"
//	waitForDaemonStopped()        - Wait for daemon to report "not running"
//	waitForPathReady()            - Wait for filesystem path to be accessible
//	waitForCondition()            - Wait for arbitrary condition with logging
//
// ## Design Principles
//
//  1. Isolation: Each test gets its own daemon via unique LATENTFS_DAEMON name
//  2. Determinism: All waits use polling, not fixed sleeps
//  3. Debuggability: Wait helpers log progress for troubleshooting timeouts
//  4. Cleanup: All environments have Cleanup() that stops daemons and removes files
package integration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

var (
	cliBinary   string
	buildOnce   sync.Once
	buildErr    error
	projectRoot string

	// NFS log monitor
	nfsLogMonitor *NFSLogMonitor
)

// TestMain builds the CLI binary once before running all tests
func TestMain(m *testing.M) {
	// Find project root (go up from tests/integration)
	wd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get working directory: %v\n", err)
		os.Exit(1)
	}

	// Navigate to project root
	projectRoot = filepath.Join(wd, "..", "..")
	cliBinary = filepath.Join(projectRoot, "bin", "latentfs")

	// NOTE: No global os.Setenv("LATENTFS_DAEMON", ...) here!
	// Each test creates its own isolated daemon via RunCLIWithConfigDir().
	// This enables true parallel test isolation without env var races.

	// Ensure bin directory exists
	if err := os.MkdirAll(filepath.Join(projectRoot, "bin"), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create bin directory: %v\n", err)
		os.Exit(1)
	}

	// === GLOBAL BEFORE: clean slate ===
	// Snapshot pre-existing state, then kill orphans and unmount stale mounts.
	preExistingDaemons, preExistingMounts := snapshotOrphans()
	globalBeforeTests(preExistingDaemons, preExistingMounts)

	// Build the binary with NFS support (default)
	fmt.Println("Building latentfs binary with NFS support...")
	cmd := exec.Command("go", "build", "-o", cliBinary, "./cmd/latentfs")
	cmd.Dir = projectRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build binary: %v\n", err)
		os.Exit(1)
	}

	// Start NFS log monitor (macOS only, will fail gracefully on other platforms)
	nfsLogMonitor = NewNFSLogMonitor()
	if err := nfsLogMonitor.Start(); err != nil {
		fmt.Printf("Note: NFS log monitor not available: %v\n", err)
	}

	code := m.Run()

	// === GLOBAL AFTER: verify tests cleaned up after themselves ===
	leakedCode := globalAfterTests(preExistingDaemons, preExistingMounts)
	if leakedCode != 0 && code == 0 {
		code = leakedCode
	}

	// Report any NFS errors captured during tests
	if nfsLogMonitor != nil {
		errors := nfsLogMonitor.GetErrors()
		if len(errors) > 0 {
			fmt.Printf("\n=== NFS Log Monitor Summary ===\n")
			fmt.Printf("Detected %d potential NFS issues during tests:\n", len(errors))
			for i, err := range errors {
				if i >= 20 {
					fmt.Printf("... and %d more\n", len(errors)-20)
					break
				}
				fmt.Printf("  %s\n", err)
			}
		}
		nfsLogMonitor.Stop()
	}

	os.Exit(code)
}

// ---------------------------------------------------------------------------
// Global before/after helpers
// ---------------------------------------------------------------------------

// snapshotOrphans captures the current set of latentfs daemon PIDs, NFS
// mount paths, and test artifact files. Items that existed *before* the test
// run are not the tests' fault and will be excluded from the after-check.
func snapshotOrphans() (daemonPIDs map[string]bool, mountPaths map[string]bool) {
	daemonPIDs = make(map[string]bool)
	mountPaths = make(map[string]bool)

	// Snapshot daemon PIDs
	if out, err := exec.Command("pgrep", "-f", "latentfs.*daemon").Output(); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if line != "" {
				daemonPIDs[line] = true
			}
		}
	}

	// Snapshot NFS mounts
	for _, mp := range listLatentfsMounts() {
		mountPaths[mp] = true
	}

	return
}

// globalBeforeTests performs aggressive cleanup so the test suite starts from a
// clean slate: stop the default daemon, kill all orphan test daemons, force-
// unmount all stale NFS mounts, and remove leftover mount directories.
func globalBeforeTests(preExistingDaemons map[string]bool, preExistingMounts map[string]bool) {
	fmt.Println("=== Global Before: cleaning up orphan daemons/mounts ===")

	nDaemons := len(preExistingDaemons)
	nMounts := len(preExistingMounts)

	// 1. Force unmount ALL latentfs NFS mounts first (prevents Finder hang
	//    when we subsequently kill daemon processes).
	forceUnmountAllTestMounts()

	// 2. Kill ALL latentfs daemon processes (test + default).
	exec.Command("pkill", "-9", "-f", "latentfs.*daemon.*test_").Run()
	exec.Command("pkill", "-9", "-f", "latentfs2.*daemon").Run()
	// Also stop the default daemon gracefully if it happens to be running.
	exec.Command("pkill", "-9", "-f", "latentfs.*daemon.*--foreground").Run()

	// 3. Brief sleep to let the kernel release NFS client state after servers die.
	time.Sleep(200 * time.Millisecond)

	// 4. Second pass unmount — now that servers are dead, previously "busy"
	//    mounts may succeed.
	forceUnmountAllTestMounts()

	// 5. Remove stale mount directories.
	home, _ := os.UserHomeDir()
	configDir := filepath.Join(home, ".latentfs")
	cleanupAllStaleMountDirs(configDir)

	// 6. Remove stale test artifact files (sock, pid, lock, log, meta) from
	//    previous runs. Only touch test_* prefixed files.
	nArtifacts := removeStaleTestArtifacts(configDir)

	fmt.Printf("  Before: %d orphan daemons, %d stale mounts, %d stale artifacts (cleaned)\n",
		nDaemons, nMounts, nArtifacts)
}

// removeStaleTestArtifacts removes test_* artifact files (.sock, .pid, .lock,
// .log, _meta.latentfs, _meta.latentfs-wal, _meta.latentfs-shm) left behind
// by previous test runs.
func removeStaleTestArtifacts(configDir string) int {
	entries, err := os.ReadDir(configDir)
	if err != nil {
		return 0
	}
	removed := 0
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "test_") {
			continue
		}
		os.Remove(filepath.Join(configDir, name))
		removed++
	}
	return removed
}

// globalAfterTests checks for any orphan daemons or NFS mounts that were
// created during the test run and not cleaned up. If any are found, it prints
// a diagnostic and returns exit code 1 so CI catches the leak.
// Items that already existed before the test run (preExisting*) are excluded.
//
// When LATENTFS_PRESERVE_DEBUG=1 is set, .log and _meta.latentfs files are
// excluded from the leak check since they are intentionally preserved.
func globalAfterTests(preExistingDaemons map[string]bool, preExistingMounts map[string]bool) int {
	fmt.Println("\n=== Global After: checking for leaked daemons/mounts ===")
	preserveDebug := preserveDebugEnv()
	if preserveDebug {
		fmt.Println("  [PRESERVE_DEBUG] Debug artifacts (.log, _meta.latentfs) excluded from leak check")
	}

	// Brief delay to let any exiting daemon processes fully terminate.
	// Daemon cleanup involves unmounting NFS which can take a moment on macOS.
	time.Sleep(500 * time.Millisecond)

	leaked := 0

	// Check for orphan daemon processes
	if out, err := exec.Command("pgrep", "-fl", "latentfs.*daemon").Output(); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if line == "" {
				continue
			}
			// Extract PID (first field)
			pid := strings.Fields(line)[0]
			if preExistingDaemons[pid] {
				continue // existed before tests
			}
			fmt.Printf("  LEAKED daemon: %s\n", line)
			leaked++
		}
	}

	// Check for orphan NFS mounts
	for _, mp := range listLatentfsMounts() {
		if preExistingMounts[mp] {
			continue // existed before tests
		}
		fmt.Printf("  LEAKED mount: %s\n", mp)
		leaked++
	}

	// Check for orphan test artifact files in ~/.latentfs/
	home, _ := os.UserHomeDir()
	configDir := filepath.Join(home, ".latentfs")
	if entries, err := os.ReadDir(configDir); err == nil {
		for _, entry := range entries {
			name := entry.Name()
			// Only flag test-related artifacts (test_* pattern)
			if !strings.HasPrefix(name, "test_") {
				continue
			}
			// When PRESERVE_DEBUG is enabled, skip .log and _meta.latentfs files
			if preserveDebug {
				if strings.HasSuffix(name, ".log") ||
					strings.HasSuffix(name, "_meta.latentfs") ||
					strings.HasSuffix(name, "_meta.latentfs-wal") ||
					strings.HasSuffix(name, "_meta.latentfs-shm") {
					continue
				}
			}
			fmt.Printf("  LEAKED artifact: %s/%s\n", configDir, name)
			leaked++
		}
	}

	if leaked > 0 {
		fmt.Printf("  FAIL: %d leaked resources (tests did not clean up properly)\n", leaked)
		// Best-effort cleanup so we don't leave a mess for the next run
		forceUnmountAllTestMounts()
		exec.Command("pkill", "-9", "-f", "latentfs.*daemon.*test_").Run()
		return 1
	}

	fmt.Println("  OK: no leaked daemons or mounts")
	return 0
}

// listLatentfsMounts returns all NFS mount paths related to latentfs tests.
// This includes mounts under ~/.latentfs/ (legacy) and /tmp/lfs_* or /var/folders/.../lfs_* (new isolation pattern).
func listLatentfsMounts() []string {
	home, _ := os.UserHomeDir()
	configDir := filepath.Join(home, ".latentfs")
	var result []string

	mountOut, err := exec.Command("mount", "-t", "nfs").Output()
	if err != nil {
		return nil
	}
	for _, line := range strings.Split(string(mountOut), "\n") {
		if !strings.Contains(line, "localhost:/") && !strings.Contains(line, "127.0.0.1:/") {
			continue
		}
		parts := strings.Split(line, " on ")
		if len(parts) < 2 {
			continue
		}
		mountPart := parts[1]
		idx := strings.Index(mountPart, " (")
		if idx == -1 {
			continue
		}
		mountPoint := mountPart[:idx]
		// Match latentfs mounts:
		// - ~/.latentfs/ (legacy default config dir)
		// - /tmp/lfs_* or /private/tmp/lfs_* (new test isolation pattern)
		// - /var/folders/.../lfs_* (macOS temp dirs)
		// - Any path containing mnt_daemon (central mount dir)
		if strings.Contains(mountPoint, configDir) ||
			strings.Contains(mountPoint, "/.latentfs/") ||
			strings.Contains(mountPoint, "/lfs_") ||
			strings.Contains(mountPoint, "/mnt_daemon") {
			result = append(result, mountPoint)
		}
	}
	return result
}

// TestEnv holds the test environment configuration
type TestEnv struct {
	t         *testing.T
	g         Gomega // Gomega instance for Eventually/Consistently
	TestDir   string
	DataFile  string
	Mount     string
	configDir string // LATENTFS_CONFIG_DIR for this test (isolated daemon config)
	mounted   bool
}

// NewTestEnv creates a new test environment with isolated daemon via LATENTFS_CONFIG_DIR
func NewTestEnv(t *testing.T, name string) *TestEnv {
	t.Helper()

	// Create isolated test directory
	// Use short prefix to avoid exceeding Unix socket path limit (104 bytes on macOS).
	shortName := name
	if len(shortName) > 10 {
		shortName = shortName[:10]
	}
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("lfs_%s_%d", shortName, time.Now().UnixMilli()%1000000))
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create config directory inside test directory for daemon isolation
	// Each test gets its own config dir with fixed daemon name "daemon"
	configDir := filepath.Join(testDir, "config")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("Failed to create config directory: %v", err)
	}

	// Note: mount path is NOT created here - fork command will create a symlink at this path
	mountPath := filepath.Join(testDir, "mount")

	dataFile := filepath.Join(testDir, "test.latentfs")

	return &TestEnv{
		t:         t,
		g:         NewWithT(t),
		TestDir:   testDir,
		DataFile:  dataFile,
		Mount:     mountPath,
		configDir: configDir,
	}
}

// Cleanup removes the test environment and stops the daemon.
// When LATENTFS_PRESERVE_DEBUG=1 is set, test directories are preserved
// for post-mortem analysis.
func (e *TestEnv) Cleanup() {
	// Report any NFS errors before cleanup
	if nfsLogMonitor != nil {
		nfsLogMonitor.ReportErrors(e.t)
	}

	// Stop daemon for this test (using isolated env)
	e.RunCLI("daemon", "stop")
	waitForDaemonStoppedWithConfigDir(e.configDir, 5*time.Second)

	// Remove symlink/mount point if exists
	if e.mounted {
		e.Unmount()
	}
	runWithTimeout(2*time.Second, func() { os.Remove(e.Mount) })

	// Force unmount and clean up config directory artifacts
	cleanupTestConfigDir(e.configDir)

	// Remove test directory (use timeout to avoid hanging on stale NFS)
	// Skip when preserving debug artifacts
	if preserveDebugEnv() {
		e.t.Logf("[PRESERVE_DEBUG] Keeping test directory: %s", e.TestDir)
	} else {
		runWithTimeout(2*time.Second, func() { os.RemoveAll(e.TestDir) })
	}
}

// InitDataFile creates a new .latentfs data file and mounts it
// This uses fork --source-empty to create an empty data file and mount it
func (e *TestEnv) InitDataFile() {
	e.t.Helper()

	// Start daemon first
	e.StartDaemon()

	// Use fork --source-empty to create and mount in one step
	result := e.RunCLI("fork", "--source-empty", "-m", e.Mount, "--data-file", e.DataFile)
	if result.ExitCode != 0 {
		e.t.Fatalf("Failed to init data file: %s", result.Combined)
	}
	e.mounted = true

	// Wait for mount to be ready
	e.g.Eventually(func() error {
		_, err := os.ReadDir(e.Mount)
		return err
	}).WithTimeout(MountReadyTimeout).WithPolling(50 * time.Millisecond).Should(Succeed())
}

// StartDaemon starts the daemon if not running
func (e *TestEnv) StartDaemon() {
	e.t.Helper()
	result := e.RunCLI("daemon", "start", "--skip-cleanup")
	if result.ExitCode != 0 && !result.Contains("already running") {
		e.t.Fatalf("Failed to start daemon: %s", result.Combined)
	}
	// Poll for daemon readiness using Gomega's Eventually
	e.g.Eventually(func() bool {
		result := e.RunCLI("mount", "ls")
		// Daemon is running if output doesn't contain "daemon not running"
		return !result.Contains("daemon not running")
	}).WithTimeout(MountReadyTimeout).WithPolling(25 * time.Millisecond).Should(BeTrue())
}

// StartMount mounts the data file (no-op if already mounted by InitDataFile)
func (e *TestEnv) StartMount() {
	e.t.Helper()

	// If already mounted by InitDataFile, nothing to do
	if e.mounted {
		return
	}

	e.StartDaemon()

	// Mount existing data file
	result := e.RunCLI("mount", e.Mount, "-d", e.DataFile)
	if result.ExitCode != 0 {
		e.t.Fatalf("Failed to mount: %s", result.Combined)
	}
	e.mounted = true

	// Poll for mount readiness using Gomega's Eventually
	e.g.Eventually(func() error {
		_, err := os.ReadDir(e.Mount)
		return err
	}).WithTimeout(MountReadyTimeout).WithPolling(50 * time.Millisecond).Should(Succeed())
}

// Unmount unmounts the filesystem
func (e *TestEnv) Unmount() {
	e.t.Helper()

	e.RunCLI("unmount", e.Mount)

	// Poll for unmount completion using `mount check -q` (efficient single-path check)
	e.g.Eventually(func() bool {
		result := e.RunCLI("mount", "check", "-q", e.Mount)
		return result.ExitCode != 0 // Non-zero exit = not mounted
	}).WithTimeout(100 * time.Millisecond).WithPolling(20 * time.Millisecond).Should(BeTrue())

	// Fallback: force unmount if still mounted
	exec.Command("umount", "-f", e.Mount).Run()
	e.mounted = false
}


// CLIResult holds the result of a CLI command
type CLIResult struct {
	Stdout   string
	Stderr   string
	Combined string
	ExitCode int
}

// RunCLI executes the latentfs CLI with isolated daemon environment
func (e *TestEnv) RunCLI(args ...string) CLIResult {
	return RunCLIWithConfigDir(e.configDir, args...)
}


// filterEnvExcluding returns os.Environ() with the specified env var removed
func filterEnvExcluding(exclude string) []string {
	env := make([]string, 0, len(os.Environ()))
	prefix := exclude + "="
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, prefix) {
			env = append(env, e)
		}
	}
	return env
}

// CLITimeout is the maximum time a CLI command can run before being killed.
// This prevents tests from hanging indefinitely on stale NFS mounts.
// Set to 15s to accommodate `daemon start` under heavy parallelism (16 daemons
// starting simultaneously, each doing cleanup + mount_nfs). Most commands
// complete in <1s; daemon start can take 5-10s under contention.
const CLITimeout = 15 * time.Second

// RunCLIWithConfigDir executes CLI with isolated daemon environment via LATENTFS_CONFIG_DIR.
// This passes LATENTFS_CONFIG_DIR via cmd.Env instead of process-wide env,
// enabling true test isolation for parallel tests.
func RunCLIWithConfigDir(configDir string, args ...string) CLIResult {
	ctx, cancel := context.WithTimeout(context.Background(), CLITimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, cliBinary, args...)
	// After context cancels and process is killed, wait up to 2s for I/O
	// pipes to drain, then forcefully close them. Without this, cmd.Run()
	// hangs forever if the CLI spawns a background daemon that inherits pipes.
	cmd.WaitDelay = 2 * time.Second

	// Isolated environment - no LATENTFS_CONFIG_DIR inheritance from process
	env := filterEnvExcluding("LATENTFS_CONFIG_DIR")
	if configDir != "" {
		env = append(env, "LATENTFS_CONFIG_DIR="+configDir)
	}
	// Pass test process PID so daemon can self-terminate when test dies.
	// Chain: test (PID A) → CLI (PID B) → daemon (PID C, Setsid).
	// CLI inherits env via os.Environ(), so daemon gets LATENTFS_PARENT_PID=A.
	// When test timeout causes os.Exit(2), defers are bypassed, but daemon
	// detects parent PID death and shuts down gracefully.
	env = append(env, fmt.Sprintf("LATENTFS_PARENT_PID=%d", os.Getpid()))
	cmd.Env = env

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	exitCode := 0
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			// Command timed out - likely stuck on stale NFS mount
			exitCode = 124 // Standard timeout exit code
			stderr.WriteString(fmt.Sprintf("\n[CLI TIMEOUT] Command timed out after %v: %v\n", CLITimeout, args))
		} else if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = 1
		}
	}

	combined := stdout.String() + stderr.String()

	return CLIResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		Combined: combined,
		ExitCode: exitCode,
	}
}

// WriteFile writes content to a file in the mount
func (e *TestEnv) WriteFile(relPath, content string) {
	e.t.Helper()
	fullPath := filepath.Join(e.Mount, relPath)

	// Ensure parent directory exists
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		e.t.Fatalf("Failed to create directory %s: %v", dir, err)
	}

	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		e.t.Fatalf("Failed to write file %s: %v", relPath, err)
	}
}

// ReadFile reads content from a file in the mount
func (e *TestEnv) ReadFile(relPath string) string {
	e.t.Helper()
	fullPath := filepath.Join(e.Mount, relPath)

	data, err := os.ReadFile(fullPath)
	if err != nil {
		e.t.Fatalf("Failed to read file %s: %v", relPath, err)
	}
	return string(data)
}

// FileExists checks if a file exists in the mount
func (e *TestEnv) FileExists(relPath string) bool {
	fullPath := filepath.Join(e.Mount, relPath)
	_, err := os.Stat(fullPath)
	return err == nil
}

// MkdirAll creates directories in the mount
func (e *TestEnv) MkdirAll(relPath string) {
	e.t.Helper()
	fullPath := filepath.Join(e.Mount, relPath)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		e.t.Fatalf("Failed to create directory %s: %v", relPath, err)
	}
}

// DeleteFile removes a file from the mount
func (e *TestEnv) DeleteFile(relPath string) {
	e.t.Helper()
	fullPath := filepath.Join(e.Mount, relPath)
	if err := os.Remove(fullPath); err != nil {
		e.t.Fatalf("Failed to delete file %s: %v", relPath, err)
	}
}

// Contains checks if output contains a substring
func (r CLIResult) Contains(s string) bool {
	return bytes.Contains([]byte(r.Combined), []byte(s))
}

// runWithTimeout runs a function with a timeout. If the function doesn't complete
// within the timeout, it returns (the goroutine may still be running but we don't wait).
// This is used for cleanup operations that might hang on stale NFS mounts.
func runWithTimeout(timeout time.Duration, fn func()) {
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
		// Completed
	case <-time.After(timeout):
		// Timed out - we abandon the goroutine
	}
}

// startDaemonWithRetry starts a daemon with retries. After many prior tests,
// the macOS NFS kernel client can be slow to process new mounts, causing
// the daemon readiness check to time out. Retrying after a brief pause
// lets the kernel recover.
func startDaemonWithRetry(t *testing.T, configDir string, maxRetries int) CLIResult {
	t.Helper()
	for attempt := 0; attempt <= maxRetries; attempt++ {
		result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
		if result.ExitCode == 0 {
			return result
		}
		if attempt < maxRetries {
			t.Logf("daemon in %s start attempt %d failed, retrying...", configDir, attempt+1)
			// Clean up failed attempt artifacts before retry
			RunCLIWithConfigDir(configDir, "daemon", "stop")
			cleanupTestConfigDir(configDir)
			time.Sleep(2 * time.Second)
		}
	}
	// Return last failed result
	return RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
}

// preserveDebugEnv returns true if LATENTFS_PRESERVE_DEBUG=1 is set.
// When enabled, test cleanup will preserve log files and other debug artifacts
// to help troubleshoot test failures. Set this when running failing tests locally.
func preserveDebugEnv() bool {
	return os.Getenv("LATENTFS_PRESERVE_DEBUG") == "1"
}

// cleanupTestConfigDir cleans up all artifacts in a test config directory.
// This includes force-killing the daemon process, unmounting, and removing
// all daemon files. With LATENTFS_CONFIG_DIR isolation, each test has its own
// config directory with a fixed daemon name "daemon".
//
// When LATENTFS_PRESERVE_DEBUG=1 is set, log files and meta databases are
// preserved for post-mortem analysis. Use this when debugging flaky tests.
func cleanupTestConfigDir(configDir string) {
	preserveDebug := preserveDebugEnv()

	// Read PID before removing files so we can force-kill if needed.
	pidFile := filepath.Join(configDir, "daemon.pid")
	if pidData, err := os.ReadFile(pidFile); err == nil {
		var pid int
		if _, err := fmt.Sscanf(strings.TrimSpace(string(pidData)), "%d", &pid); err == nil && pid > 0 {
			// Use kill command for more reliable SIGKILL on macOS
			exec.Command("kill", "-9", fmt.Sprintf("%d", pid)).Run()
			// Wait for process to fully exit so globalAfterTests
			// doesn't see it as leaked.
			if proc, err := os.FindProcess(pid); err == nil {
				for i := 0; i < 50; i++ {
					if err := proc.Signal(syscall.Signal(0)); err != nil {
						break // process gone
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
		}
	}

	// Force unmount the central mount point with tight timeouts.
	// The daemon stop command already attempted graceful unmount — these are
	// just safety nets for when the daemon couldn't unmount cleanly.
	mountPath := filepath.Join(configDir, "mnt_daemon")
	runWithTimeout(1*time.Second, func() { exec.Command("umount", "-f", mountPath).Run() })
	runWithTimeout(1*time.Second, func() { exec.Command("diskutil", "unmount", "force", mountPath).Run() })
	os.RemoveAll(mountPath)

	// Clean up daemon files (fixed name "daemon")
	// Always remove: .sock, .pid, .lock (runtime files)
	os.Remove(filepath.Join(configDir, "daemon.sock"))
	os.Remove(pidFile)
	os.Remove(filepath.Join(configDir, "daemon.lock"))

	// Conditionally remove debug artifacts based on LATENTFS_PRESERVE_DEBUG
	if preserveDebug {
		// Keep log and meta files for debugging
		fmt.Printf("  [PRESERVE_DEBUG] Keeping debug artifacts in %s\n", configDir)
	} else {
		// Normal cleanup: remove everything
		os.Remove(filepath.Join(configDir, "daemon.log"))
		os.Remove(filepath.Join(configDir, "daemon_meta.latentfs"))
		os.Remove(filepath.Join(configDir, "daemon_meta.latentfs-wal"))
		os.Remove(filepath.Join(configDir, "daemon_meta.latentfs-shm"))
	}
}

// getDaemonPIDInConfigDir reads the daemon PID from its PID file in a config directory.
// Returns 0 if the PID file doesn't exist or can't be read.
func getDaemonPIDInConfigDir(configDir string) int {
	pidFile := filepath.Join(configDir, "daemon.pid")
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return 0
	}
	var pid int
	fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid)
	return pid
}

// forceKillPID sends SIGKILL to a process and waits for it to exit.
// Safe to call with pid=0 (no-op).
func forceKillPID(pid int) {
	if pid <= 0 {
		return
	}
	exec.Command("kill", "-9", fmt.Sprintf("%d", pid)).Run()
	// Wait for process to fully exit
	if proc, err := os.FindProcess(pid); err == nil {
		for i := 0; i < 50; i++ {
			if err := proc.Signal(syscall.Signal(0)); err != nil {
				break // process gone
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// cleanupAllStaleMountDirs removes all empty, unmounted mnt_* directories.
// This handles directories left behind from old test runs.
func cleanupAllStaleMountDirs(configDir string) {
	entries, err := os.ReadDir(configDir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		// Only process directories starting with "mnt_"
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "mnt_") {
			continue
		}

		dirPath := filepath.Join(configDir, entry.Name())

		// Skip if currently mounted (check mount table)
		mountOut, _ := exec.Command("mount").Output()
		if strings.Contains(string(mountOut), " on "+dirPath+" ") {
			continue
		}

		// Check if directory is empty
		dirEntries, err := os.ReadDir(dirPath)
		if err != nil {
			continue
		}

		// Only remove if empty
		if len(dirEntries) == 0 {
			os.Remove(dirPath)
		}
	}
}

// forceUnmountAllTestMounts force-unmounts all latentfs NFS mounts.
// This includes mounts under ~/.latentfs/ (legacy) and /tmp/lfs_* or /var/folders/.../lfs_* (new isolation pattern).
// This MUST be called before killing daemon processes to prevent Finder hangs.
// Unmounts run in parallel with a global timeout to avoid slow startup.
func forceUnmountAllTestMounts() {
	home, _ := os.UserHomeDir()
	configDir := filepath.Join(home, ".latentfs")

	// Get all NFS mounts
	mountOut, err := exec.Command("mount", "-t", "nfs").Output()
	if err != nil {
		return
	}

	// Collect mount points to unmount
	var mountPoints []string
	for _, line := range strings.Split(string(mountOut), "\n") {
		if !strings.Contains(line, "localhost:/") && !strings.Contains(line, "127.0.0.1:/") {
			continue
		}
		parts := strings.Split(line, " on ")
		if len(parts) < 2 {
			continue
		}
		mountPart := parts[1]
		idx := strings.Index(mountPart, " (")
		if idx == -1 {
			continue
		}
		mountPoint := mountPart[:idx]
		// Match latentfs mounts:
		// - ~/.latentfs/ (legacy default config dir)
		// - /tmp/lfs_* or /private/tmp/lfs_* (new test isolation pattern)
		// - /var/folders/.../lfs_* (macOS temp dirs)
		// - Any path containing mnt_daemon (central mount dir)
		if !strings.Contains(mountPoint, configDir) &&
			!strings.Contains(mountPoint, "/.latentfs/") &&
			!strings.Contains(mountPoint, "/lfs_") &&
			!strings.Contains(mountPoint, "/mnt_daemon") {
			continue
		}
		mountPoints = append(mountPoints, mountPoint)
	}

	if len(mountPoints) == 0 {
		return
	}

	fmt.Printf("Force unmounting %d stale test mounts...\n", len(mountPoints))

	// Unmount all in parallel with a global 3s timeout
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for _, mp := range mountPoints {
			wg.Add(1)
			go func(mountPoint string) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				exec.CommandContext(ctx, "diskutil", "unmount", "force", mountPoint).Run()
			}(mp)
		}
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		fmt.Println("Warning: stale mount cleanup timed out (zombie NFS mounts may require reboot)")
	}
}

// NFSLogMonitor monitors macOS unified logs for NFS-related events
type NFSLogMonitor struct {
	cmd       *exec.Cmd
	output    bytes.Buffer
	mu        sync.Mutex
	running   bool
	stopCh    chan struct{}
	doneCh    chan struct{}
}

// NewNFSLogMonitor creates a new NFS log monitor
func NewNFSLogMonitor() *NFSLogMonitor {
	return &NFSLogMonitor{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start begins monitoring NFS logs
func (m *NFSLogMonitor) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return nil
	}

	// Start log stream with NFS-related predicates
	// Capture errors, warnings, and important events
	m.cmd = exec.Command("log", "stream",
		"--predicate", "(eventMessage CONTAINS \"NFS\" OR eventMessage CONTAINS \"nfs\" OR eventMessage CONTAINS \"mount\" OR eventMessage CONTAINS \"VQ_DEAD\") AND (messageType == error OR messageType == fault OR eventMessage CONTAINS \"error\" OR eventMessage CONTAINS \"failed\" OR eventMessage CONTAINS \"disconnect\")",
		"--style", "compact",
		"--level", "debug",
	)

	m.cmd.Stdout = &m.output
	m.cmd.Stderr = &m.output

	if err := m.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start log monitor: %w", err)
	}

	m.running = true

	// Monitor in background
	go func() {
		defer close(m.doneCh)
		m.cmd.Wait()
	}()

	return nil
}

// Stop stops the log monitor
func (m *NFSLogMonitor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return
	}

	if m.cmd != nil && m.cmd.Process != nil {
		m.cmd.Process.Kill()
	}

	m.running = false

	// Wait for process to finish
	select {
	case <-m.doneCh:
	case <-time.After(2 * time.Second):
	}
}

// GetOutput returns the captured log output
func (m *NFSLogMonitor) GetOutput() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.output.String()
}

// GetErrors returns NFS-related errors from the log
func (m *NFSLogMonitor) GetErrors() []string {
	output := m.GetOutput()
	if output == "" {
		return nil
	}

	var errors []string
	lines := bytes.Split([]byte(output), []byte("\n"))
	for _, line := range lines {
		lineStr := string(line)
		// Filter for actual error indicators
		if bytes.Contains(line, []byte("error")) ||
			bytes.Contains(line, []byte("failed")) ||
			bytes.Contains(line, []byte("Error")) ||
			bytes.Contains(line, []byte("VQ_DEAD")) ||
			bytes.Contains(line, []byte("disconnect")) ||
			bytes.Contains(line, []byte("not responding")) {
			if len(lineStr) > 0 {
				errors = append(errors, lineStr)
			}
		}
	}
	return errors
}

// ReportErrors logs any NFS errors found (call at end of test)
func (m *NFSLogMonitor) ReportErrors(t *testing.T) {
	errors := m.GetErrors()
	if len(errors) > 0 {
		t.Logf("=== NFS Log Monitor detected %d potential issues ===", len(errors))
		for i, err := range errors {
			if i >= 10 {
				t.Logf("... and %d more", len(errors)-10)
				break
			}
			t.Logf("  [NFS] %s", err)
		}
	}
}

// Clear clears the captured output
func (m *NFSLogMonitor) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.output.Reset()
}

// =============================================================================
// WAIT HELPERS (Inner/Outer Loop Design)
// =============================================================================
//
// All wait functions use a unified pattern:
//   - Outer loop: Total timeout control
//   - Inner loop: Poll at 50ms intervals
//   - Logging: Log status every 1s during wait (for troubleshooting timeouts)
//   - Return: bool indicating success (true) or timeout (false)
//
// The logging helps debug timeout issues by showing what state the system
// was in during the wait, rather than just "timeout after Xs".

// WaitConfig holds configuration for wait operations
type WaitConfig struct {
	Name        string        // Description for logging (e.g., "daemon running")
	Timeout     time.Duration // Total timeout
	Poll        time.Duration // Polling interval (default: 50ms)
	LogInterval time.Duration // How often to log during wait (default: 1s)
}

// DefaultWaitConfig returns sensible defaults for wait operations
func DefaultWaitConfig(name string, timeout time.Duration) WaitConfig {
	return WaitConfig{
		Name:        name,
		Timeout:     timeout,
		Poll:        50 * time.Millisecond,
		LogInterval: 1 * time.Second,
	}
}

// waitFor is the unified wait helper with inner/outer loop design.
// It polls the check function until it returns (true, _) or timeout.
// The check function returns (done bool, status string) where:
//   - done: true if condition is met
//   - status: human-readable status for logging (shown during wait and on timeout)
//
// Returns true if condition met, false on timeout.
func waitFor(t *testing.T, cfg WaitConfig, check func() (done bool, status string)) bool {
	if t != nil {
		t.Helper()
	}

	// Apply defaults
	if cfg.Poll == 0 {
		cfg.Poll = 50 * time.Millisecond
	}
	if cfg.LogInterval == 0 {
		cfg.LogInterval = 1 * time.Second
	}

	deadline := time.Now().Add(cfg.Timeout)
	lastLog := time.Now()

	// Outer loop: timeout control
	for time.Now().Before(deadline) {
		// Inner loop check
		done, status := check()
		if done {
			return true
		}

		// Periodic logging for debugging timeouts
		if t != nil && time.Since(lastLog) >= cfg.LogInterval {
			t.Logf("[waitFor:%s] still waiting... (status: %s, elapsed: %v)",
				cfg.Name, status, time.Since(deadline.Add(-cfg.Timeout)))
			lastLog = time.Now()
		}

		time.Sleep(cfg.Poll)
	}

	// Timeout - log final status
	_, finalStatus := check()
	if t != nil {
		t.Logf("[waitFor:%s] TIMEOUT after %v (final status: %s)",
			cfg.Name, cfg.Timeout, finalStatus)
	}
	return false
}

// waitForCondition is a simplified version that takes just a condition function.
// Use this when you don't need custom status messages.
func waitForCondition(t *testing.T, name string, timeout time.Duration, condition func() bool) bool {
	return waitFor(t, DefaultWaitConfig(name, timeout), func() (bool, string) {
		if condition() {
			return true, "done"
		}
		return false, "waiting"
	})
}

// --- Daemon Wait Helpers ---

// waitForDaemonReadyWithConfigDir polls until daemon reports running status (isolated env)
// Uses inner/outer loop design with logging for troubleshooting.
func waitForDaemonReadyWithConfigDir(g Gomega, configDir string, timeout time.Duration) {
	g.Eventually(func() bool {
		result := RunCLIWithConfigDir(configDir, "daemon", "status")
		return result.Contains("running") && !result.Contains("not running")
	}).WithTimeout(timeout).WithPolling(50 * time.Millisecond).Should(BeTrue())
}

// waitForDaemonRunningWithConfigDir polls until daemon reports running status (isolated env).
// Returns true if daemon is running, false on timeout.
func waitForDaemonRunningWithConfigDir(configDir string, timeout time.Duration) bool {
	return waitFor(nil, DefaultWaitConfig("daemon running in "+filepath.Base(configDir), timeout), func() (bool, string) {
		result := RunCLIWithConfigDir(configDir, "daemon", "status")
		if result.Contains("running") && !result.Contains("not running") {
			return true, "running"
		}
		status := strings.TrimSpace(result.Combined)
		if len(status) > 50 {
			status = status[:50] + "..."
		}
		return false, status
	})
}

// waitForDaemonStoppedWithConfigDir polls until daemon is no longer running (isolated env)
// Uses inner/outer loop design with logging for troubleshooting.
func waitForDaemonStoppedWithConfigDir(configDir string, timeout time.Duration) {
	waitFor(nil, DefaultWaitConfig("daemon stopped in "+filepath.Base(configDir), timeout), func() (bool, string) {
		result := RunCLIWithConfigDir(configDir, "daemon", "status")
		if result.Contains("not running") {
			return true, "not running"
		}
		// Extract status from output for debugging
		status := strings.TrimSpace(result.Combined)
		if len(status) > 50 {
			status = status[:50] + "..."
		}
		return false, status
	})
}

// waitForDaemonStoppedWithLogging is like waitForDaemonStoppedWithConfigDir but logs progress.
// Use this when debugging daemon stop issues.
func waitForDaemonStoppedWithLogging(t *testing.T, configDir string, timeout time.Duration) bool {
	return waitFor(t, DefaultWaitConfig("daemon stopped in "+filepath.Base(configDir), timeout), func() (bool, string) {
		result := RunCLIWithConfigDir(configDir, "daemon", "status")
		if result.Contains("not running") {
			return true, "not running"
		}
		status := strings.TrimSpace(result.Combined)
		if len(status) > 50 {
			status = status[:50] + "..."
		}
		return false, status
	})
}

// --- Path/Mount Wait Helpers ---

// waitForPathReady polls until a filesystem path is accessible.
// Uses inner/outer loop design with logging for troubleshooting.
func waitForPathReady(t *testing.T, path string, timeout time.Duration) bool {
	return waitFor(t, DefaultWaitConfig("path ready: "+filepath.Base(path), timeout), func() (bool, string) {
		_, err := os.ReadDir(path)
		if err == nil {
			return true, "accessible"
		}
		return false, err.Error()
	})
}

// waitForPathGone polls until a filesystem path is no longer accessible.
// Uses inner/outer loop design with logging for troubleshooting.
func waitForPathGone(t *testing.T, path string, timeout time.Duration) bool {
	return waitFor(t, DefaultWaitConfig("path gone: "+filepath.Base(path), timeout), func() (bool, string) {
		_, err := os.Stat(path)
		if os.IsNotExist(err) {
			return true, "not found"
		}
		if err != nil {
			return false, err.Error()
		}
		return false, "still exists"
	})
}

// WaitForUnmount waits until ALL specified paths are no longer mounted.
// Uses `mount check -q path1 path2 ...` for efficient batch checking.
// Returns true if all paths are unmounted within timeout, false otherwise.
func WaitForUnmount(t *testing.T, configDir string, timeout time.Duration, mountPaths ...string) bool {
	t.Helper()
	if len(mountPaths) == 0 {
		return true
	}

	// Build description for logging
	desc := filepath.Base(mountPaths[0])
	if len(mountPaths) > 1 {
		desc = fmt.Sprintf("%d paths", len(mountPaths))
	}

	// Build command args: mount check -q path1 path2 ...
	args := append([]string{"mount", "check", "-q"}, mountPaths...)

	return waitFor(t, DefaultWaitConfig("unmount: "+desc, timeout), func() (bool, string) {
		result := RunCLIWithConfigDir(configDir, args...)
		// Exit code non-zero means not all paths are mounted (what we want for unmount wait)
		return result.ExitCode != 0, "checking"
	})
}

// waitForMountGone is an alias for single-path unmount wait (backward compatibility).
func waitForMountGone(t *testing.T, configDir, mountPath string, timeout time.Duration) bool {
	return WaitForUnmount(t, configDir, timeout, mountPath)
}

// --- SharedDaemonTestEnv for sharing daemon across subtests ---

// SharedDaemonTestEnv allows multiple subtests to share a single daemon
// This reduces test time by avoiding daemon start/stop overhead per subtest
type SharedDaemonTestEnv struct {
	t         *testing.T
	g         Gomega
	TestDir   string
	configDir string // LATENTFS_CONFIG_DIR for this shared daemon
}

// NewSharedDaemonTestEnv creates a shared daemon environment for subtests
func NewSharedDaemonTestEnv(t *testing.T, name string) *SharedDaemonTestEnv {
	t.Helper()

	// Use short prefix to avoid exceeding Unix socket path limit (104 bytes on macOS).
	// macOS temp dir is ~57 chars, so we limit test dir name to ~30 chars.
	shortName := name
	if len(shortName) > 10 {
		shortName = shortName[:10]
	}
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("lfs_%s_%d", shortName, time.Now().UnixMilli()%1000000))
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create config directory inside test directory for daemon isolation
	configDir := filepath.Join(testDir, "config")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("Failed to create config directory: %v", err)
	}

	env := &SharedDaemonTestEnv{
		t:         t,
		g:         NewWithT(t),
		TestDir:   testDir,
		configDir: configDir,
	}

	// Configure logging before starting daemon
	// Use daemon config to set trace logging for detailed debug output
	RunCLIWithConfigDir(configDir, "daemon", "config", "--logging", "trace")

	// Start daemon once for all subtests (using isolated env)
	result := RunCLIWithConfigDir(configDir, "daemon", "start", "--skip-cleanup")
	if result.ExitCode != 0 && !result.Contains("already running") {
		t.Fatalf("Failed to start daemon: %s", result.Combined)
	}
	waitForDaemonReadyWithConfigDir(env.g, configDir, MountReadyTimeout)

	// Also verify the NFS central mount is actually accessible.
	// The IPC readiness check above only confirms the daemon process is running,
	// but the NFS mount may have failed to start (port contention, mount_nfs failure, etc).
	centralMount := filepath.Join(configDir, "mnt_daemon")
	env.g.Eventually(func() error {
		_, err := os.ReadDir(centralMount)
		return err
	}).WithTimeout(MountReadyTimeout).WithPolling(50 * time.Millisecond).Should(Succeed(),
		"Central NFS mount at %s not accessible after daemon start", centralMount)

	return env
}

// CreateSubtestEnv creates an isolated environment for a subtest with its own mount/datafile
func (e *SharedDaemonTestEnv) CreateSubtestEnv(t *testing.T, name string) *SubtestEnv {
	t.Helper()
	subDir := filepath.Join(e.TestDir, name)
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatalf("Failed to create subtest directory: %v", err)
	}

	return &SubtestEnv{
		t:         t,
		g:         NewGomegaWithT(t),
		TestDir:   subDir,
		DataFile:  filepath.Join(subDir, "test.latentfs"),
		Mount:     filepath.Join(subDir, "mount"),
		configDir: e.configDir, // Inherit config dir from shared env
	}
}

// Cleanup stops the shared daemon and removes test directory.
// The total cleanup budget is ~8s to stay well within the 30s test timeout.
// When LATENTFS_PRESERVE_DEBUG=1 is set, test directories are preserved.
func (e *SharedDaemonTestEnv) Cleanup() {
	// daemon stop CLI already: sends IPC stop, waits up to 10s, force-kills.
	// No additional waitForDaemonStopped needed.
	RunCLIWithConfigDir(e.configDir, "daemon", "stop")

	// Force cleanup of config directory artifacts (quick timeouts since daemon stop
	// already did the heavy lifting — these are just safety nets)
	cleanupTestConfigDir(e.configDir)

	if preserveDebugEnv() {
		e.t.Logf("[PRESERVE_DEBUG] Keeping shared test directory: %s", e.TestDir)
	} else {
		os.RemoveAll(e.TestDir)
	}
}

// RunCLI executes the CLI with isolated daemon environment
func (e *SharedDaemonTestEnv) RunCLI(args ...string) CLIResult {
	return RunCLIWithConfigDir(e.configDir, args...)
}

// SubtestEnv is an isolated environment for a single subtest within a SharedDaemonTestEnv
type SubtestEnv struct {
	t         *testing.T
	g         Gomega
	TestDir   string
	DataFile  string
	Mount     string
	configDir string // Inherited from SharedDaemonTestEnv
	mounted   bool
}

// InitDataFile creates a new .latentfs data file and mounts it
func (e *SubtestEnv) InitDataFile() {
	e.t.Helper()

	// Use fork --source-empty to create and mount (using isolated env)
	result := e.RunCLI("fork", "--source-empty", "-m", e.Mount, "--data-file", e.DataFile)
	if result.ExitCode != 0 {
		e.t.Fatalf("Failed to init data file: %s", result.Combined)
	}
	e.mounted = true

	// Wait for mount to be ready
	e.g.Eventually(func() error {
		_, err := os.ReadDir(e.Mount)
		return err
	}).WithTimeout(MountReadyTimeout).WithPolling(50 * time.Millisecond).Should(Succeed())
}

// WriteFile writes content to a file in the mount
func (e *SubtestEnv) WriteFile(relPath, content string) {
	e.t.Helper()
	fullPath := filepath.Join(e.Mount, relPath)

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		e.t.Fatalf("Failed to create directory %s: %v", dir, err)
	}

	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		e.t.Fatalf("Failed to write file %s: %v", relPath, err)
	}
}

// ReadFile reads content from a file in the mount
func (e *SubtestEnv) ReadFile(relPath string) string {
	e.t.Helper()
	fullPath := filepath.Join(e.Mount, relPath)

	data, err := os.ReadFile(fullPath)
	if err != nil {
		e.t.Fatalf("Failed to read file %s: %v", relPath, err)
	}
	return string(data)
}

// RunCLI executes the CLI with isolated daemon environment
func (e *SubtestEnv) RunCLI(args ...string) CLIResult {
	return RunCLIWithConfigDir(e.configDir, args...)
}

// FileExists checks if a file exists in the mount
func (e *SubtestEnv) FileExists(relPath string) bool {
	fullPath := filepath.Join(e.Mount, relPath)
	_, err := os.Stat(fullPath)
	return err == nil
}

// MkdirAll creates directories in the mount
func (e *SubtestEnv) MkdirAll(relPath string) {
	e.t.Helper()
	fullPath := filepath.Join(e.Mount, relPath)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		e.t.Fatalf("Failed to create directory %s: %v", relPath, err)
	}
}

// DeleteFile removes a file from the mount
func (e *SubtestEnv) DeleteFile(relPath string) {
	e.t.Helper()
	fullPath := filepath.Join(e.Mount, relPath)
	if err := os.Remove(fullPath); err != nil {
		e.t.Fatalf("Failed to delete file %s: %v", relPath, err)
	}
}

// Unmount unmounts this subtest's mount point
func (e *SubtestEnv) Unmount() {
	e.t.Helper()
	e.RunCLI("unmount", e.Mount)
	e.g.Eventually(func() bool {
		result := e.RunCLI("mount", "check", "-q", e.Mount)
		return result.ExitCode != 0
	}).WithTimeout(100 * time.Millisecond).WithPolling(20 * time.Millisecond).Should(BeTrue())
	exec.Command("umount", "-f", e.Mount).Run()
	e.mounted = false
}

// Cleanup unmounts and removes the subtest directory.
// When LATENTFS_PRESERVE_DEBUG=1 is set, test directories are preserved.
func (e *SubtestEnv) Cleanup() {
	if e.mounted {
		e.RunCLI("unmount", e.Mount)
		// After IPC unmount removes the symlink, umount -f is a no-op safety net
		runWithTimeout(1*time.Second, func() { exec.Command("umount", "-f", e.Mount).Run() })
	}
	if preserveDebugEnv() {
		e.t.Logf("[PRESERVE_DEBUG] Keeping subtest directory: %s", e.TestDir)
	} else {
		os.RemoveAll(e.TestDir)
	}
}

// --- ForkSubtestEnv for fork-based tests within a shared daemon ---

// ForkSubtestEnv is an isolated fork environment for a single subtest within a SharedDaemonTestEnv
type ForkSubtestEnv struct {
	t          *testing.T
	g          Gomega
	TestDir    string
	SourceDir  string // Source folder for fork
	TargetPath string // Symlink target path
	DataFile   string // Data file path
	configDir  string // Inherited from SharedDaemonTestEnv
}

// CreateForkSubtestEnv creates an isolated fork environment for a subtest
func (e *SharedDaemonTestEnv) CreateForkSubtestEnv(t *testing.T, name string) *ForkSubtestEnv {
	t.Helper()
	subDir := filepath.Join(e.TestDir, name)
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatalf("Failed to create subtest directory: %v", err)
	}

	sourceDir := filepath.Join(subDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("Failed to create source directory: %v", err)
	}

	targetPath := filepath.Join(subDir, "target")

	return &ForkSubtestEnv{
		t:          t,
		g:          NewGomegaWithT(t),
		TestDir:    subDir,
		SourceDir:  sourceDir,
		TargetPath: targetPath,
		DataFile:   targetPath + ".latentfs",
		configDir:  e.configDir,
	}
}

// RunCLI executes the CLI with isolated daemon environment
func (e *ForkSubtestEnv) RunCLI(args ...string) CLIResult {
	return RunCLIWithConfigDir(e.configDir, args...)
}

// ForkEmpty creates an empty fork at TargetPath
func (e *ForkSubtestEnv) ForkEmpty() {
	e.t.Helper()
	result := e.RunCLI("fork", "--source-empty", "-m", e.TargetPath)
	if result.ExitCode != 0 {
		e.t.Fatalf("fork --source-empty failed: %s", result.Combined)
	}
	e.WaitForMountReady(MountReadyTimeout)
}

// ForkWithSource creates a fork with SourceDir as source
func (e *ForkSubtestEnv) ForkWithSource() {
	e.t.Helper()
	result := e.RunCLI("fork", e.SourceDir, "-m", e.TargetPath)
	if result.ExitCode != 0 {
		e.t.Fatalf("fork with source failed: %s", result.Combined)
	}
	e.WaitForMountReady(MountReadyTimeout)
}

// WaitForMountReady polls until the target path is accessible
func (e *ForkSubtestEnv) WaitForMountReady(timeout time.Duration) {
	e.t.Helper()
	e.g.Eventually(func() error {
		_, err := os.ReadDir(e.TargetPath)
		return err
	}).WithTimeout(timeout).WithPolling(50 * time.Millisecond).Should(Succeed())
}

// CreateSourceFile creates a file in the source directory
func (e *ForkSubtestEnv) CreateSourceFile(relPath, content string) {
	e.t.Helper()
	fullPath := filepath.Join(e.SourceDir, relPath)
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		e.t.Fatalf("Failed to create directory %s: %v", dir, err)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		e.t.Fatalf("Failed to write source file %s: %v", relPath, err)
	}
}

// CreateSourceSymlink creates a symbolic link in the source directory
func (e *ForkSubtestEnv) CreateSourceSymlink(name, target string) {
	e.t.Helper()
	linkPath := filepath.Join(e.SourceDir, name)
	dir := filepath.Dir(linkPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		e.t.Fatalf("Failed to create directory %s: %v", dir, err)
	}
	if err := os.Symlink(target, linkPath); err != nil {
		e.t.Fatalf("Failed to create symlink %s -> %s: %v", name, target, err)
	}
}

// WriteTargetFile writes a file to the fork target
func (e *ForkSubtestEnv) WriteTargetFile(relPath, content string) {
	e.t.Helper()
	fullPath := filepath.Join(e.TargetPath, relPath)
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		e.t.Fatalf("Failed to create directory %s: %v", dir, err)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		e.t.Fatalf("Failed to write target file %s: %v", relPath, err)
	}
}

// ReadTargetFile reads a file from the fork target
func (e *ForkSubtestEnv) ReadTargetFile(relPath string) string {
	e.t.Helper()
	fullPath := filepath.Join(e.TargetPath, relPath)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		e.t.Fatalf("Failed to read target file %s: %v", relPath, err)
	}
	return string(data)
}

// TargetFileExists checks if a file exists in the fork target
func (e *ForkSubtestEnv) TargetFileExists(relPath string) bool {
	fullPath := filepath.Join(e.TargetPath, relPath)
	_, err := os.Stat(fullPath)
	return err == nil
}

// IsSymlink checks if the target path is a symlink
func (e *ForkSubtestEnv) IsSymlink() bool {
	info, err := os.Lstat(e.TargetPath)
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeSymlink != 0
}

// CentralMountPath returns the expected central mount path for this daemon
func (e *ForkSubtestEnv) CentralMountPath() string {
	return filepath.Join(e.configDir, "mnt_daemon")
}

// WaitForUnmount waits until the mount path(s) are no longer mounted
func (e *ForkSubtestEnv) WaitForUnmount(timeout time.Duration, mountPaths ...string) {
	e.t.Helper()
	if len(mountPaths) == 0 {
		return
	}
	args := append([]string{"mount", "check", "-q"}, mountPaths...)
	e.g.Eventually(func() bool {
		result := e.RunCLI(args...)
		return result.ExitCode != 0
	}).WithTimeout(timeout).WithPolling(50 * time.Millisecond).Should(BeTrue())
}

// Cleanup unmounts and removes the fork subtest directory.
// When LATENTFS_PRESERVE_DEBUG=1 is set, test directories are preserved.
func (e *ForkSubtestEnv) Cleanup() {
	e.RunCLI("unmount", e.TargetPath)
	runWithTimeout(3*time.Second, func() { exec.Command("umount", "-f", e.TargetPath).Run() })
	os.Remove(e.TargetPath) // Remove symlink
	if preserveDebugEnv() {
		e.t.Logf("[PRESERVE_DEBUG] Keeping fork subtest directory: %s", e.TestDir)
	} else {
		runWithTimeout(2*time.Second, func() { os.RemoveAll(e.TestDir) })
	}
}

// --- CascadedForkSubtestEnv for cascaded fork tests within a shared daemon ---

// CascadedForkSubtestEnv is an isolated cascaded fork environment for a single subtest
type CascadedForkSubtestEnv struct {
	t         *testing.T
	g         Gomega
	TestDir   string
	SourceDir string
	Forks     []string
	DataFiles []string
	configDir string
}

// CreateCascadedForkSubtestEnv creates an isolated cascaded fork environment for a subtest
func (e *SharedDaemonTestEnv) CreateCascadedForkSubtestEnv(t *testing.T, name string, numForks int) *CascadedForkSubtestEnv {
	t.Helper()
	subDir := filepath.Join(e.TestDir, name)
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatalf("Failed to create subtest directory: %v", err)
	}

	sourceDir := filepath.Join(subDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("Failed to create source directory: %v", err)
	}

	forks := make([]string, numForks)
	dataFiles := make([]string, numForks)
	for i := 0; i < numForks; i++ {
		forks[i] = filepath.Join(subDir, fmt.Sprintf("fork_%d", i))
		dataFiles[i] = forks[i] + ".latentfs"
	}

	return &CascadedForkSubtestEnv{
		t:         t,
		g:         NewGomegaWithT(t),
		TestDir:   subDir,
		SourceDir: sourceDir,
		Forks:     forks,
		DataFiles: dataFiles,
		configDir: e.configDir,
	}
}

// RunCLI executes the CLI with isolated daemon environment
func (e *CascadedForkSubtestEnv) RunCLI(args ...string) CLIResult {
	return RunCLIWithConfigDir(e.configDir, args...)
}

// CreateSourceFile creates a file in the source directory
func (e *CascadedForkSubtestEnv) CreateSourceFile(relPath, content string) {
	e.t.Helper()
	fullPath := filepath.Join(e.SourceDir, relPath)
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		e.t.Fatalf("Failed to create directory %s: %v", dir, err)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		e.t.Fatalf("Failed to write source file %s: %v", relPath, err)
	}
}

// WriteForkFile writes a file to a specific fork
func (e *CascadedForkSubtestEnv) WriteForkFile(forkIndex int, relPath, content string) {
	e.t.Helper()
	fullPath := filepath.Join(e.Forks[forkIndex], relPath)
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		e.t.Fatalf("Failed to create directory %s: %v", dir, err)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		e.t.Fatalf("Failed to write fork file %s: %v", relPath, err)
	}
}

// ReadForkFile reads a file from a specific fork
func (e *CascadedForkSubtestEnv) ReadForkFile(forkIndex int, relPath string) (string, error) {
	fullPath := filepath.Join(e.Forks[forkIndex], relPath)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// WaitForForkReady polls until a specific fork path is accessible
func (e *CascadedForkSubtestEnv) WaitForForkReady(forkIndex int, timeout time.Duration) {
	e.t.Helper()
	if forkIndex >= len(e.Forks) {
		e.t.Fatalf("Fork index %d out of range (have %d forks)", forkIndex, len(e.Forks))
	}
	e.g.Eventually(func() error {
		_, err := os.ReadDir(e.Forks[forkIndex])
		return err
	}).WithTimeout(timeout).WithPolling(50 * time.Millisecond).Should(Succeed())
}

// WaitForMountReady polls until the first fork path is accessible
func (e *CascadedForkSubtestEnv) WaitForMountReady(timeout time.Duration) {
	e.t.Helper()
	if len(e.Forks) > 0 {
		e.WaitForForkReady(0, timeout)
	}
}

// WaitForUnmount waits until the specified fork(s) are no longer mounted
func (e *CascadedForkSubtestEnv) WaitForUnmount(timeout time.Duration, forkIndices ...int) {
	e.t.Helper()
	paths := make([]string, 0, len(forkIndices))
	for _, idx := range forkIndices {
		if idx >= len(e.Forks) {
			e.t.Fatalf("Fork index %d out of range (have %d forks)", idx, len(e.Forks))
		}
		paths = append(paths, e.Forks[idx])
	}
	e.WaitForUnmountPaths(timeout, paths...)
}

// WaitForUnmountPaths waits until the specified path(s) are no longer mounted
func (e *CascadedForkSubtestEnv) WaitForUnmountPaths(timeout time.Duration, mountPaths ...string) {
	e.t.Helper()
	if len(mountPaths) == 0 {
		return
	}
	args := append([]string{"mount", "check", "-q"}, mountPaths...)
	e.g.Eventually(func() bool {
		result := e.RunCLI(args...)
		return result.ExitCode != 0
	}).WithTimeout(timeout).WithPolling(50 * time.Millisecond).Should(BeTrue())
}

// Cleanup unmounts all forks and removes the subtest directory.
// When LATENTFS_PRESERVE_DEBUG=1 is set, test directories are preserved.
func (e *CascadedForkSubtestEnv) Cleanup() {
	for _, fork := range e.Forks {
		e.RunCLI("unmount", fork)
		runWithTimeout(3*time.Second, func() { exec.Command("umount", "-f", fork).Run() })
		os.Remove(fork)
	}
	if preserveDebugEnv() {
		e.t.Logf("[PRESERVE_DEBUG] Keeping cascaded fork subtest directory: %s", e.TestDir)
	} else {
		runWithTimeout(2*time.Second, func() { os.RemoveAll(e.TestDir) })
	}
}
