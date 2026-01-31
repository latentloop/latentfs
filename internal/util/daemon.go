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

package util

import (
	"context"
	"fmt"
	"os"
)

// DaemonStartConfig configures daemon start behavior.
type DaemonStartConfig struct {
	Notify   bool      // Print status messages to stderr
	PollConfig PollConfig // Polling config for waiting
}

// DefaultDaemonStartConfig returns sensible defaults.
func DefaultDaemonStartConfig() DaemonStartConfig {
	return DaemonStartConfig{
		Notify: true,
		PollConfig: FastPollConfig(),
	}
}

// StartDaemonIfNeeded starts the daemon in the background if not running.
// Uses the provided checker to determine if daemon is already running.
// Uses the provided startCmd slice (e.g., []string{"daemon", "start"}) to start.
// Returns nil if daemon is already running or successfully started.
func StartDaemonIfNeeded(ctx context.Context, cfg DaemonStartConfig, isRunning func() bool, startCmd []string) error {
	if isRunning() {
		return nil
	}

	if cfg.Notify {
		fmt.Fprint(os.Stderr, "Starting daemon...")
	}

	exe, err := GetExecutablePath()
	if err != nil {
		if cfg.Notify {
			fmt.Fprintln(os.Stderr, " failed")
		}
		return err
	}

	// Start daemon process
	_, err = StartBackgroundProcess(exe, startCmd, nil)
	if err != nil {
		if cfg.Notify {
			fmt.Fprintln(os.Stderr, " failed")
		}
		return err
	}

	// Wait for daemon to be ready
	err = PollUntil(ctx, cfg.PollConfig, isRunning)
	if err != nil {
		if cfg.Notify {
			fmt.Fprintln(os.Stderr, " timeout")
		}
		return fmt.Errorf("daemon did not start in time")
	}

	if cfg.Notify {
		fmt.Fprintln(os.Stderr, " done")
	}
	return nil
}
