// Package util provides shared utility functions for latentfs.
package util

import (
	"context"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
)

// DatabaseRetryOptions returns retry options optimized for database operations.
// Uses linear backoff (100ms, 200ms, 300ms) suitable for transient lock errors.
func DatabaseRetryOptions(ctx context.Context) []retry.Option {
	return []retry.Option{
		retry.Attempts(3),
		retry.Delay(100 * time.Millisecond),
		retry.MaxDelay(300 * time.Millisecond),
		retry.DelayType(retry.BackOffDelay),
		retry.RetryIf(IsDatabaseLocked),
		retry.Context(ctx),
	}
}

// DefaultRetryOptions returns sensible defaults for retry operations.
func DefaultRetryOptions(ctx context.Context) []retry.Option {
	return []retry.Option{
		retry.Attempts(3),
		retry.Delay(100 * time.Millisecond),
		retry.MaxDelay(1 * time.Second),
		retry.DelayType(retry.BackOffDelay),
		retry.Context(ctx),
	}
}

// Retry executes fn with retry logic.
// Returns the last error if all attempts fail.
func Retry(ctx context.Context, fn func() error, opts ...retry.Option) error {
	if len(opts) == 0 {
		opts = DefaultRetryOptions(ctx)
	}
	return retry.Do(fn, opts...)
}

// RetryWithResult executes fn with retry logic and returns the result.
func RetryWithResult[T any](ctx context.Context, fn func() (T, error), opts ...retry.Option) (T, error) {
	if len(opts) == 0 {
		opts = DefaultRetryOptions(ctx)
	}
	return retry.DoWithData(fn, opts...)
}

// Common retry predicates

// IsDatabaseLocked returns true if the error indicates a database lock.
func IsDatabaseLocked(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "database is locked")
}

