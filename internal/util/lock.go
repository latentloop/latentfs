package util

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/flock"
)

// FileLock provides exclusive file locking with timeout support.
// Uses OS-level flock() which is automatically released when the process
// terminates (including crashes, SIGKILL, etc.).
type FileLock struct {
	lock *flock.Flock
	path string
}

// LockConfig configures file locking behavior.
type LockConfig struct {
	Timeout       time.Duration // Total timeout (default: 30s)
	RetryInterval time.Duration // Retry interval (default: 100ms)
}

// DefaultLockConfig returns sensible defaults for file locking.
func DefaultLockConfig() LockConfig {
	return LockConfig{
		Timeout:       30 * time.Second,
		RetryInterval: 100 * time.Millisecond,
	}
}

// AcquireFileLock acquires an exclusive lock on the given path.
// The lock file is created if it doesn't exist.
// Returns a FileLock that must be released with Unlock().
func AcquireFileLock(ctx context.Context, lockPath string, cfg LockConfig) (*FileLock, error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = 100 * time.Millisecond
	}

	fileLock := flock.New(lockPath)
	deadline := time.Now().Add(cfg.Timeout)

	for {
		locked, err := fileLock.TryLock()
		if err != nil {
			return nil, fmt.Errorf("failed to acquire lock on %s: %w", lockPath, err)
		}
		if locked {
			return &FileLock{lock: fileLock, path: lockPath}, nil
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timeout waiting for lock on %s", lockPath)
		}

		time.Sleep(cfg.RetryInterval)
	}
}


// Unlock releases the file lock.
// Safe to call multiple times or on a nil lock.
func (l *FileLock) Unlock() error {
	if l == nil || l.lock == nil {
		return nil
	}
	return l.lock.Unlock()
}

// Path returns the lock file path.
func (l *FileLock) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}
