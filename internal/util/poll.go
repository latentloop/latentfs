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
	"time"
)

// PollConfig configures polling/wait behavior.
type PollConfig struct {
	Timeout  time.Duration // Total timeout (default: 5s)
	Interval time.Duration // Polling interval (default: 50ms)
}

// DefaultPollConfig returns sensible defaults for polling operations.
func DefaultPollConfig() PollConfig {
	return PollConfig{
		Timeout:  5 * time.Second,
		Interval: 50 * time.Millisecond,
	}
}

// FastPollConfig returns config for fast polling (e.g., daemon startup).
func FastPollConfig() PollConfig {
	return PollConfig{
		Timeout:  5 * time.Second,
		Interval: 25 * time.Millisecond,
	}
}


// PollUntil polls until condition returns true or timeout.
// Returns nil on success, context.DeadlineExceeded on timeout.
func PollUntil(ctx context.Context, cfg PollConfig, condition func() bool) error {
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.Interval == 0 {
		cfg.Interval = 50 * time.Millisecond
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	// Check immediately before first tick
	if condition() {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if condition() {
				return nil
			}
		}
	}
}


// WaitWithDeadline waits for a condition with a hard deadline.
// Unlike PollUntil, this uses a simple polling loop without context overhead.
// Returns true if condition was met, false on timeout.
func WaitWithDeadline(deadline time.Time, interval time.Duration, condition func() bool) bool {
	if interval == 0 {
		interval = 50 * time.Millisecond
	}

	// Check immediately
	if condition() {
		return true
	}

	for time.Now().Before(deadline) {
		time.Sleep(interval)
		if condition() {
			return true
		}
	}
	return false
}

// WaitFixed waits with fixed number of iterations and interval.
// Returns true if condition was met, false after all iterations.
func WaitFixed(iterations int, interval time.Duration, condition func() bool) bool {
	for i := range iterations {
		if condition() {
			return true
		}
		if i < iterations-1 {
			time.Sleep(interval)
		}
	}
	return false
}
