//go:build darwin

package daemon

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// CleanupResult contains the result of a cleanup operation
type CleanupResult struct {
	StaleMounts     []string // Mount points that were unmounted
	CleanedPidFile  bool     // Whether PID file was cleaned
	CleanedSocket   bool     // Whether socket file was cleaned
	Errors          []error  // Any errors encountered
}

// CleanupStaleMounts finds and unmounts stale latentfs mounts (both NFS and SMB)
// A stale mount is one that points to localhost but the daemon isn't running
func CleanupStaleMounts() (*CleanupResult, error) {
	result := &CleanupResult{}

	// Don't clean up if daemon is running
	if IsDaemonRunning() {
		return result, nil
	}

	// Find all stale mounts (NFS and SMB)
	var staleMounts []string

	// Find NFS mounts (default on macOS)
	nfsMounts, err := findStaleNFSMounts()
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("failed to find stale NFS mounts: %w", err))
	} else {
		staleMounts = append(staleMounts, nfsMounts...)
	}

	// Find SMB mounts (fallback/alternative)
	smbMounts, err := findStaleSMBMounts()
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("failed to find stale SMB mounts: %w", err))
	} else {
		staleMounts = append(staleMounts, smbMounts...)
	}

	// Unmount each stale mount
	for _, mountPoint := range staleMounts {
		if err := Unmount(mountPoint); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("failed to unmount %s: %w", mountPoint, err))
		} else {
			result.StaleMounts = append(result.StaleMounts, mountPoint)
		}
	}

	// Clean up stale PID file if process is dead
	if cleanedPid := cleanupStalePidFile(); cleanedPid {
		result.CleanedPidFile = true
	}

	// Clean up stale socket file
	if cleanedSocket := cleanupStaleSocket(); cleanedSocket {
		result.CleanedSocket = true
	}

	return result, nil
}

// findStaleNFSMounts finds NFS mounts pointing to localhost (latentfs mounts)
// Pattern: localhost:/latentfs on /path/to/mount (nfs, ...)
func findStaleNFSMounts() ([]string, error) {
	// Get current NFS mounts using mount command
	cmd := exec.Command("mount", "-t", "nfs")
	output, err := cmd.Output()
	if err != nil {
		// If command fails (e.g., no nfs mounts), that's OK
		return nil, nil
	}

	var staleMounts []string
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		// Format: localhost:/sharename on /path/to/mount (nfs, ...)
		// We look for localhost NFS mounts in the latentfs config directory
		if strings.Contains(line, "localhost:/") || strings.Contains(line, "127.0.0.1:/") {
			// Extract mount point (after " on " and before " (")
			parts := strings.Split(line, " on ")
			if len(parts) >= 2 {
				mountPart := parts[1]
				// Find the mount point (before the options in parentheses)
				if idx := strings.Index(mountPart, " ("); idx != -1 {
					mountPoint := mountPart[:idx]
					// Only include mounts in ~/.latentfs/ directory (our mounts)
					if strings.Contains(mountPoint, "/.latentfs/") || strings.Contains(mountPoint, getConfigDir()) {
						staleMounts = append(staleMounts, mountPoint)
					}
				}
			}
		}
	}

	return staleMounts, scanner.Err()
}

// findStaleSMBMounts finds SMB mounts pointing to localhost (latentfs mounts)
func findStaleSMBMounts() ([]string, error) {
	// Get current mounts using mount command
	cmd := exec.Command("mount", "-t", "smbfs")
	output, err := cmd.Output()
	if err != nil {
		// If command fails (e.g., no smbfs mounts), that's OK
		return nil, nil
	}

	var staleMounts []string
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		// Format: //Guest@127.0.0.1:PORT/latentfs on /path/to/mount (smbfs, ...)
		// We look for localhost SMB mounts
		if strings.Contains(line, "//Guest@127.0.0.1:") ||
			strings.Contains(line, "//guest@127.0.0.1:") ||
			strings.Contains(line, "//Guest@localhost:") ||
			strings.Contains(line, "//guest@localhost:") {
			// Extract mount point (after " on " and before " (")
			parts := strings.Split(line, " on ")
			if len(parts) >= 2 {
				mountPart := parts[1]
				// Find the mount point (before the options in parentheses)
				if idx := strings.Index(mountPart, " ("); idx != -1 {
					mountPoint := mountPart[:idx]
					staleMounts = append(staleMounts, mountPoint)
				}
			}
		}
	}

	return staleMounts, scanner.Err()
}

// cleanupStalePidFile removes PID file if the process is not running
func cleanupStalePidFile() bool {
	pidPath := PidPath()

	// Check if PID file exists
	pid, err := GetPID()
	if err != nil {
		// No PID file or can't read it
		return false
	}

	// Check if process is running
	proc, err := os.FindProcess(pid)
	if err != nil {
		// Process not found, clean up
		os.Remove(pidPath)
		return true
	}

	// On Unix, FindProcess always succeeds, so send signal 0 to check if alive
	err = proc.Signal(os.Signal(nil))
	if err != nil {
		// Process is dead, clean up
		os.Remove(pidPath)
		return true
	}

	return false
}

// cleanupStaleSocket removes socket file if daemon isn't running
func cleanupStaleSocket() bool {
	socketPath := SocketPath()

	// Check if socket exists
	if _, err := os.Stat(socketPath); os.IsNotExist(err) {
		return false
	}

	// If we can't connect to the daemon, the socket is stale
	if !IsDaemonRunning() {
		os.Remove(socketPath)
		return true
	}

	return false
}

// CleanupOwnMount unmounts and cleans up this daemon's own central mount point.
// This should be called before starting the daemon to ensure a clean slate.
// It handles stale mounts from previous crashed daemon instances.
func CleanupOwnMount() error {
	mountPath := CentralMountPath()

	// Check if the mount point is currently mounted
	if IsMounted(mountPath) {
		// Try graceful unmount first
		if err := Unmount(mountPath); err != nil {
			// Force unmount as fallback
			exec.Command("umount", "-f", mountPath).Run()
			exec.Command("diskutil", "unmount", "force", mountPath).Run()
		}
	}

	// Remove the mount directory if it exists
	if info, err := os.Stat(mountPath); err == nil && info.IsDir() {
		// Only remove if empty (to avoid accidental data loss)
		entries, err := os.ReadDir(mountPath)
		if err == nil && len(entries) == 0 {
			os.Remove(mountPath)
		}
	}

	return nil
}

// KillZombieDaemons kills latentfs daemon processes that are truly orphaned.
// A process is considered a zombie if:
// 1. It's from an old binary (latentfs2) - always kill these
// 2. It's a test daemon AND its socket doesn't respond
//
// IMPORTANT: This function first unmounts all stale NFS/SMB mounts BEFORE killing
// processes. This prevents Finder from hanging on stale mounts when the NFS server
// dies. See: https://discussions.apple.com/thread/6672455
//
// SAFETY: This function will NOT kill:
// - The current process
// - Test daemons whose sockets are still responding (healthy test daemons)
// - The default "daemon" instance (only test daemons and old binaries)
func KillZombieDaemons() int {
	// CRITICAL: First unmount stale mounts before killing any processes
	// This prevents Finder from hanging on stale NFS mounts when servers die
	forceUnmountAllStaleMounts()

	// Get all latentfs daemon processes
	cmd := exec.Command("pgrep", "-f", "latentfs.*daemon")
	output, err := cmd.Output()
	if err != nil {
		return 0
	}

	killed := 0
	pids := strings.Fields(string(output))
	myPid := os.Getpid()

	for _, pidStr := range pids {
		pid, err := strconv.Atoi(pidStr)
		if err != nil || pid == myPid {
			continue
		}

		proc, err := os.FindProcess(pid)
		if err != nil {
			continue
		}

		// Get process command line
		psCmd := exec.Command("ps", "-p", pidStr, "-o", "command=")
		psOut, err := psCmd.Output()
		if err != nil {
			continue
		}
		cmdline := string(psOut)

		// Always kill old binary processes (latentfs2)
		if strings.Contains(cmdline, "latentfs2") {
			proc.Signal(syscall.SIGKILL)
			killed++
			continue
		}

		// For test daemons (those using LATENTFS_CONFIG_DIR), only kill if their socket is not responding
		// This prevents killing healthy test daemons that are currently in use
		if configDir := extractConfigDirFromCmdline(cmdline); configDir != "" {
			// Check if this test daemon is still healthy (socket responds)
			if isDaemonRunningInConfigDir(configDir) {
				// Socket responds - this is a healthy daemon, don't kill
				continue
			}

			// Socket doesn't respond - this is a zombie, kill it
			proc.Signal(syscall.SIGKILL)
			killed++
		}
	}

	return killed
}

// extractConfigDirFromCmdline extracts the config directory from a process command line
// Looks for LATENTFS_CONFIG_DIR=xxx pattern. Returns empty string if not found (default daemon).
func extractConfigDirFromCmdline(cmdline string) string {
	// Look for LATENTFS_CONFIG_DIR=xxx pattern
	if idx := strings.Index(cmdline, "LATENTFS_CONFIG_DIR="); idx != -1 {
		rest := cmdline[idx+len("LATENTFS_CONFIG_DIR="):]
		// Find end of value (space or end of string)
		if endIdx := strings.IndexAny(rest, " \t\n"); endIdx != -1 {
			return rest[:endIdx]
		}
		return strings.TrimSpace(rest)
	}
	return ""
}

// forceUnmountAllStaleMounts force-unmounts only STALE NFS and SMB mounts in ~/.latentfs/
// A mount is considered stale if its daemon is not running (socket check fails).
// This MUST be called before killing daemon processes to prevent Finder hangs.
// When an NFS server dies while Finder is accessing its mount, Finder freezes.
//
// IMPORTANT: This function only unmounts mounts whose daemons are NOT running,
// to avoid interfering with healthy daemon instances.
func forceUnmountAllStaleMounts() {
	// Find all localhost mounts in ~/.latentfs/
	nfsMounts, _ := findStaleNFSMounts()
	smbMounts, _ := findStaleSMBMounts()
	allMounts := append(nfsMounts, smbMounts...)

	for _, mountPoint := range allMounts {
		// Extract daemon name from mount path (e.g., "mnt_test_foo" -> "test_foo")
		daemonName := extractDaemonNameFromMount(mountPoint)
		if daemonName == "" {
			continue
		}

		// Check if this daemon is running by checking its socket
		if isDaemonInstanceRunning(daemonName) {
			// Skip - this mount belongs to a healthy running daemon
			continue
		}

		// Daemon is not running, this is a stale mount - force unmount
		Unmount(mountPoint)
		exec.Command("umount", "-f", mountPoint).Run()
		exec.Command("diskutil", "unmount", "force", mountPoint).Run()
	}
}

// extractDaemonNameFromMount extracts the daemon name from a mount path
// e.g., "/Users/foo/.latentfs/mnt_daemon" -> "daemon"
// Note: With LATENTFS_CONFIG_DIR isolation, all daemons use "daemon" as their name.
func extractDaemonNameFromMount(mountPath string) string {
	base := filepath.Base(mountPath)
	if name, found := strings.CutPrefix(base, "mnt_"); found {
		return name
	}
	return ""
}

// isDaemonInstanceRunning checks if a specific daemon instance is running
// by trying to connect to its socket (in the default config dir)
func isDaemonInstanceRunning(name string) bool {
	socketPath := filepath.Join(getConfigDir(), name+".sock")
	// Use a longer timeout (2s) to handle system load during parallel tests.
	// Under high parallelism, daemons may be busy handling NFS requests and
	// take longer to accept new socket connections.
	conn, err := net.DialTimeout("unix", socketPath, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// isDaemonRunningInConfigDir checks if a daemon is running in a specific config directory
// by trying to connect to its socket at {configDir}/daemon.sock
func isDaemonRunningInConfigDir(configDir string) bool {
	socketPath := filepath.Join(configDir, "daemon.sock")
	conn, err := net.DialTimeout("unix", socketPath, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// CleanupStaleMountDirectories removes all empty, unmounted mnt_* directories
// in ~/.latentfs/. This handles directories left behind from crashed daemons
// or old test runs with different naming patterns.
func CleanupStaleMountDirectories() (int, error) {
	entries, err := os.ReadDir(getConfigDir())
	if err != nil {
		return 0, err
	}

	cleaned := 0
	for _, entry := range entries {
		// Only process directories starting with "mnt_"
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "mnt_") {
			continue
		}

		dirPath := filepath.Join(getConfigDir(), entry.Name())

		// Skip if currently mounted
		if IsMounted(dirPath) {
			continue
		}

		// Check if directory is empty
		dirEntries, err := os.ReadDir(dirPath)
		if err != nil {
			continue
		}

		// Only remove if empty
		if len(dirEntries) == 0 {
			if err := os.Remove(dirPath); err == nil {
				cleaned++
			}
		}
	}

	return cleaned, nil
}


// FormatCleanupResult formats a cleanup result for display
func FormatCleanupResult(result *CleanupResult) string {
	var parts []string

	if len(result.StaleMounts) > 0 {
		parts = append(parts, fmt.Sprintf("Unmounted %d stale mount(s):", len(result.StaleMounts)))
		for _, m := range result.StaleMounts {
			parts = append(parts, fmt.Sprintf("  - %s", m))
		}
	}

	if result.CleanedPidFile {
		parts = append(parts, "Cleaned up stale PID file")
	}

	if result.CleanedSocket {
		parts = append(parts, "Cleaned up stale socket file")
	}

	if len(result.Errors) > 0 {
		parts = append(parts, fmt.Sprintf("Encountered %d error(s):", len(result.Errors)))
		for _, e := range result.Errors {
			parts = append(parts, fmt.Sprintf("  - %s", e.Error()))
		}
	}

	if len(parts) == 0 {
		return "No cleanup needed"
	}

	return strings.Join(parts, "\n")
}
