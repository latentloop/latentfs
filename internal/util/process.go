package util

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// ProcessConfig configures process management behavior.
type ProcessConfig struct {
	GracefulTimeout time.Duration // Time to wait for graceful shutdown (default: 10s)
	PollInterval    time.Duration // Polling interval for process state (default: 100ms)
}


// StartBackgroundProcess starts a detached background process.
// The process will continue running after the parent exits.
func StartBackgroundProcess(executable string, args []string, env []string) (*os.Process, error) {
	cmd := exec.Command(executable, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	if env != nil {
		cmd.Env = env
	} else {
		cmd.Env = os.Environ()
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true, // Create new session (detach from terminal)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start process: %w", err)
	}

	return cmd.Process, nil
}


// StopProcess attempts graceful shutdown, then force kills if needed.
// The gracefulStop function should request the process to stop (e.g., via IPC).
// The isRunning function should check if the process is still running.
func StopProcess(ctx context.Context, pid int, cfg ProcessConfig, gracefulStop func() error, isRunning func() bool) error {
	if cfg.GracefulTimeout == 0 {
		cfg.GracefulTimeout = 10 * time.Second
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}

	// Request graceful stop
	if gracefulStop != nil {
		if err := gracefulStop(); err != nil {
			// Continue anyway - we'll force kill if needed
		}
	}

	// Wait for graceful shutdown
	deadline := time.Now().Add(cfg.GracefulTimeout)
	for time.Now().Before(deadline) {
		if !isRunning() {
			return nil
		}
		time.Sleep(cfg.PollInterval)
	}

	// Process didn't stop gracefully, force kill
	if proc, err := os.FindProcess(pid); err == nil {
		_ = proc.Signal(syscall.SIGKILL)
	}

	// Wait a bit more for process to die
	time.Sleep(500 * time.Millisecond)

	if isRunning() {
		return fmt.Errorf("failed to stop process (PID %d)", pid)
	}

	return nil
}

// IsProcessRunning checks if a process with the given PID is running.
func IsProcessRunning(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// On Unix, sending signal 0 checks if process exists
	err = proc.Signal(syscall.Signal(0))
	return err == nil
}

// GetExecutablePath returns the path to the current executable.
func GetExecutablePath() (string, error) {
	return os.Executable()
}
