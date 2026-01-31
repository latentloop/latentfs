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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

// MountReadyTimeout is the timeout for waiting for mounts to become ready.
// Set higher than individual test needs to handle parallel test execution.
const MountReadyTimeout = 10 * time.Second

// ForkTestEnv extends TestEnv for fork-specific testing
type ForkTestEnv struct {
	t          *testing.T
	TestDir    string
	SourceDir  string // Source folder for fork
	TargetPath string // Symlink target path
	DataFile   string // Data file path
	configDir  string // LATENTFS_CONFIG_DIR for isolated daemon
}

// NewForkTestEnv creates a new fork test environment with isolated daemon
func NewForkTestEnv(t *testing.T, name string) *ForkTestEnv {
	t.Helper()

	// Use short prefix to avoid exceeding Unix socket path limit (104 bytes on macOS).
	shortName := name
	if len(shortName) > 10 {
		shortName = shortName[:10]
	}
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("lfs_f_%s_%d", shortName, time.Now().UnixMilli()%1000000))

	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create isolated config directory for daemon (LATENTFS_CONFIG_DIR pattern)
	configDir := filepath.Join(testDir, "config")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("Failed to create config directory: %v", err)
	}

	sourceDir := filepath.Join(testDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("Failed to create source directory: %v", err)
	}

	targetPath := filepath.Join(testDir, "target")

	return &ForkTestEnv{
		t:          t,
		TestDir:    testDir,
		SourceDir:  sourceDir,
		TargetPath: targetPath,
		DataFile:   targetPath + ".latentfs",
		configDir:  configDir,
	}
}

// Cleanup removes the test environment and stops the daemon
func (e *ForkTestEnv) Cleanup() {
	// Stop daemon (using isolated env)
	e.RunCLI("daemon", "stop")
	waitForDaemonStoppedWithConfigDir(e.configDir, 500*time.Millisecond)

	// Remove symlink if exists
	os.Remove(e.TargetPath)

	// Force unmount and clean up all daemon artifacts (.sock, .pid, .lock, _meta.latentfs, etc.)
	cleanupTestConfigDir(e.configDir)

	// Remove test directory
	os.RemoveAll(e.TestDir)
}

// WaitForMountReady polls until the target path is accessible
func (e *ForkTestEnv) WaitForMountReady(timeout time.Duration) {
	e.t.Helper()
	g := NewWithT(e.t)
	g.Eventually(func() error {
		_, err := os.ReadDir(e.TargetPath)
		return err
	}).WithTimeout(timeout).WithPolling(50 * time.Millisecond).Should(Succeed())
}

// CentralMountPath returns the expected central mount path for this daemon
func (e *ForkTestEnv) CentralMountPath() string {
	return filepath.Join(e.configDir, "mnt_daemon")
}

// StartDaemon starts the isolated daemon
func (e *ForkTestEnv) StartDaemon() {
	e.t.Helper()
	result := e.RunCLI("daemon", "start", "--skip-cleanup")
	if result.ExitCode != 0 && !result.Contains("already running") {
		e.t.Fatalf("Failed to start daemon: %s", result.Combined)
	}
	// Poll for daemon to be ready (using isolated env)
	g := NewWithT(e.t)
	waitForDaemonReadyWithConfigDir(g, e.configDir, MountReadyTimeout)

	// Debug: verify central mount is accessible
	centralMount := e.CentralMountPath()
	e.t.Logf("Central mount path: %s", centralMount)
	if entries, err := os.ReadDir(centralMount); err != nil {
		e.t.Logf("Warning: cannot read central mount: %v", err)
	} else {
		e.t.Logf("Central mount entries: %d", len(entries))
	}
}

// RunCLI executes the CLI with isolated daemon environment
func (e *ForkTestEnv) RunCLI(args ...string) CLIResult {
	return RunCLIWithConfigDir(e.configDir, args...)
}

// CreateSourceFile creates a file in the source directory
func (e *ForkTestEnv) CreateSourceFile(relPath, content string) {
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

// WriteTargetFile writes a file to the fork target
func (e *ForkTestEnv) WriteTargetFile(relPath, content string) {
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
func (e *ForkTestEnv) ReadTargetFile(relPath string) string {
	e.t.Helper()
	fullPath := filepath.Join(e.TargetPath, relPath)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		e.t.Fatalf("Failed to read target file %s: %v", relPath, err)
	}
	return string(data)
}

// TargetFileExists checks if a file exists in the fork target
func (e *ForkTestEnv) TargetFileExists(relPath string) bool {
	fullPath := filepath.Join(e.TargetPath, relPath)
	_, err := os.Stat(fullPath)
	return err == nil
}

// IsSymlink checks if the target path is a symlink
func (e *ForkTestEnv) IsSymlink() bool {
	info, err := os.Lstat(e.TargetPath)
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeSymlink != 0
}

// CreateSourceSymlink creates a symbolic link in the source directory
func (e *ForkTestEnv) CreateSourceSymlink(name, target string) {
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

// WaitForUnmount waits until the mount path(s) are no longer mounted.
// Uses `mount check -q` with batch support for efficient multi-path checking.
func (e *ForkTestEnv) WaitForUnmount(timeout time.Duration, mountPaths ...string) {
	e.t.Helper()
	if len(mountPaths) == 0 {
		return
	}
	args := append([]string{"mount", "check", "-q"}, mountPaths...)
	g := NewWithT(e.t)
	g.Eventually(func() bool {
		result := e.RunCLI(args...)
		return result.ExitCode != 0 // Non-zero exit = not all mounted (what we want)
	}).WithTimeout(timeout).WithPolling(50 * time.Millisecond).Should(BeTrue())
}

// CascadedForkTestEnv extends TestEnv for cascaded fork testing
type CascadedForkTestEnv struct {
	t         *testing.T
	TestDir   string
	SourceDir string   // Original source folder
	Forks     []string // Fork target paths (index 0 = first fork, 1 = second fork, etc.)
	DataFiles []string // Data file paths
	configDir string   // LATENTFS_CONFIG_DIR for isolated daemon
}

// NewCascadedForkTestEnv creates a test environment for cascaded forks
func NewCascadedForkTestEnv(t *testing.T, name string, numForks int) *CascadedForkTestEnv {
	t.Helper()

	// Clear NFS log monitor to only capture logs for this test
	if nfsLogMonitor != nil {
		nfsLogMonitor.Clear()
	}

	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("latentfs_cascade_test_%s_%d", name, time.Now().UnixNano()))
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create isolated config directory for daemon (LATENTFS_CONFIG_DIR pattern)
	configDir := filepath.Join(testDir, "config")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("Failed to create config directory: %v", err)
	}

	sourceDir := filepath.Join(testDir, "source")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatalf("Failed to create source directory: %v", err)
	}

	forks := make([]string, numForks)
	dataFiles := make([]string, numForks)
	for i := 0; i < numForks; i++ {
		forks[i] = filepath.Join(testDir, fmt.Sprintf("fork_%d", i))
		dataFiles[i] = forks[i] + ".latentfs"
	}

	return &CascadedForkTestEnv{
		t:         t,
		TestDir:   testDir,
		SourceDir: sourceDir,
		Forks:     forks,
		DataFiles: dataFiles,
		configDir: configDir,
	}
}

// Cleanup removes the test environment
func (e *CascadedForkTestEnv) Cleanup() {
	// Report any NFS errors before cleanup
	if nfsLogMonitor != nil {
		nfsLogMonitor.ReportErrors(e.t)
	}

	e.RunCLI("daemon", "stop")
	waitForDaemonStoppedWithConfigDir(e.configDir, 500*time.Millisecond)
	for _, fork := range e.Forks {
		os.Remove(fork)
	}

	// Force unmount and clean up all daemon artifacts (.sock, .pid, .lock, _meta.latentfs, etc.)
	cleanupTestConfigDir(e.configDir)

	os.RemoveAll(e.TestDir)
}

// RunCLI executes the CLI with isolated daemon environment
func (e *CascadedForkTestEnv) RunCLI(args ...string) CLIResult {
	return RunCLIWithConfigDir(e.configDir, args...)
}

// StartDaemon starts the isolated daemon
func (e *CascadedForkTestEnv) StartDaemon() {
	e.t.Helper()
	result := e.RunCLI("daemon", "start", "--skip-cleanup")
	if result.ExitCode != 0 && !result.Contains("already running") {
		e.t.Fatalf("Failed to start daemon: %s", result.Combined)
	}
	g := NewWithT(e.t)
	waitForDaemonReadyWithConfigDir(g, e.configDir, MountReadyTimeout)
}

// WaitForMountReady polls until the given path is accessible
func (e *CascadedForkTestEnv) WaitForMountReady(timeout time.Duration) {
	e.t.Helper()
	// Wait for the first fork (most common case)
	if len(e.Forks) > 0 {
		e.WaitForForkReady(0, timeout)
	}
}

// WaitForForkReady polls until a specific fork path is accessible
func (e *CascadedForkTestEnv) WaitForForkReady(forkIndex int, timeout time.Duration) {
	e.t.Helper()
	if forkIndex >= len(e.Forks) {
		e.t.Fatalf("Fork index %d out of range (have %d forks)", forkIndex, len(e.Forks))
	}
	g := NewWithT(e.t)
	g.Eventually(func() error {
		_, err := os.ReadDir(e.Forks[forkIndex])
		return err
	}).WithTimeout(timeout).WithPolling(50 * time.Millisecond).Should(Succeed())
}

// CreateSourceFile creates a file in the source directory
func (e *CascadedForkTestEnv) CreateSourceFile(relPath, content string) {
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
func (e *CascadedForkTestEnv) WriteForkFile(forkIndex int, relPath, content string) {
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
func (e *CascadedForkTestEnv) ReadForkFile(forkIndex int, relPath string) (string, error) {
	fullPath := filepath.Join(e.Forks[forkIndex], relPath)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// WaitForUnmount waits until the specified fork(s) are no longer mounted.
// Uses `mount check -q` with batch support for efficient multi-path checking.
func (e *CascadedForkTestEnv) WaitForUnmount(timeout time.Duration, forkIndices ...int) {
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

// WaitForUnmountPaths waits until the specified path(s) are no longer mounted.
// Uses `mount check -q` with batch support for efficient multi-path checking.
func (e *CascadedForkTestEnv) WaitForUnmountPaths(timeout time.Duration, mountPaths ...string) {
	e.t.Helper()
	if len(mountPaths) == 0 {
		return
	}
	args := append([]string{"mount", "check", "-q"}, mountPaths...)
	g := NewWithT(e.t)
	g.Eventually(func() bool {
		result := e.RunCLI(args...)
		return result.ExitCode != 0 // Non-zero exit = not all mounted (what we want)
	}).WithTimeout(timeout).WithPolling(50 * time.Millisecond).Should(BeTrue())
}

// tryWriteForkFile attempts to write a file to a fork path with timeout (non-fatal version)
func tryWriteForkFile(forkPath, relPath, content string) error {
	fullPath := filepath.Join(forkPath, relPath)
	dir := filepath.Dir(fullPath)

	// Use timeout to avoid hanging on unresponsive mounts
	done := make(chan error, 1)
	go func() {
		if err := os.MkdirAll(dir, 0755); err != nil {
			done <- err
			return
		}
		done <- os.WriteFile(fullPath, []byte(content), 0644)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout writing to %s", fullPath)
	}
}
