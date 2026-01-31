package common

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorDefinitions(t *testing.T) {
	t.Parallel()

	// Verify all errors are defined and unique
	errs := []error{
		ErrNotFound,
		ErrExists,
		ErrNotDir,
		ErrIsDir,
		ErrNotEmpty,
		ErrInvalidPath,
		ErrInvalidHandle,
		ErrReadOnly,
		ErrIO,
	}

	t.Run("all errors are non-nil", func(t *testing.T) {
		t.Parallel()
		for i, err := range errs {
			require.NotNil(t, err, "error at index %d should not be nil", i)
		}
	})

	t.Run("all error messages are unique", func(t *testing.T) {
		t.Parallel()
		seen := make(map[string]bool)
		for _, err := range errs {
			msg := err.Error()
			assert.False(t, seen[msg], "duplicate error message: %s", msg)
			seen[msg] = true
		}
	})
}

func TestErrorMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{"ErrNotFound", ErrNotFound, "not found"},
		{"ErrExists", ErrExists, "already exists"},
		{"ErrNotDir", ErrNotDir, "not a directory"},
		{"ErrIsDir", ErrIsDir, "is a directory"},
		{"ErrNotEmpty", ErrNotEmpty, "directory not empty"},
		{"ErrInvalidPath", ErrInvalidPath, "invalid path"},
		{"ErrInvalidHandle", ErrInvalidHandle, "invalid handle"},
		{"ErrReadOnly", ErrReadOnly, "read-only filesystem"},
		{"ErrIO", ErrIO, "I/O error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.err.Error())
		})
	}
}

func TestErrorIs(t *testing.T) {
	t.Parallel()

	t.Run("wrapped error does not match without proper wrapping", func(t *testing.T) {
		t.Parallel()
		wrappedErr := errors.New("wrapped: " + ErrNotFound.Error())
		assert.False(t, errors.Is(wrappedErr, ErrNotFound),
			"wrapped error should not match with errors.Is (no wrapping used)")
	})

	t.Run("same error equals itself", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, ErrNotFound, ErrNotFound)
	})
}
