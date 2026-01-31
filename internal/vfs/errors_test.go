package vfs

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorMappings(t *testing.T) {
	t.Parallel()

	// Verify all errors are mapped to correct syscall errors
	tests := []struct {
		name string
		err  error
		want syscall.Errno
	}{
		{"ENOENT", ENOENT, syscall.ENOENT},
		{"EEXIST", EEXIST, syscall.EEXIST},
		{"ENOTDIR", ENOTDIR, syscall.ENOTDIR},
		{"EISDIR", EISDIR, syscall.EISDIR},
		{"EBADF", EBADF, syscall.EBADF},
		{"EINVAL", EINVAL, syscall.EINVAL},
		{"ENOTSUP", ENOTSUP, syscall.ENOTSUP},
		{"ENOSPC", ENOSPC, syscall.ENOSPC},
		{"EIO", EIO, syscall.EIO},
		{"EACCES", EACCES, syscall.EACCES},
		{"EPERM", EPERM, syscall.EPERM},
		{"EROFS", EROFS, syscall.EROFS},
		{"ENOATTR", ENOATTR, syscall.ENOATTR},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.err, "%s should map to syscall.%s", tt.name, tt.name)
		})
	}
}

func TestErrorsAreNotNil(t *testing.T) {
	t.Parallel()

	errs := []struct {
		name string
		err  error
	}{
		{"ENOENT", ENOENT},
		{"EEXIST", EEXIST},
		{"ENOTDIR", ENOTDIR},
		{"EISDIR", EISDIR},
		{"EBADF", EBADF},
		{"EINVAL", EINVAL},
		{"ENOTSUP", ENOTSUP},
		{"ENOSPC", ENOSPC},
		{"EIO", EIO},
		{"EACCES", EACCES},
		{"EPERM", EPERM},
		{"EROFS", EROFS},
		{"ENOATTR", ENOATTR},
	}

	for _, tt := range errs {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.NotNil(t, tt.err, "%s should not be nil", tt.name)
		})
	}
}

func TestErrorMessages(t *testing.T) {
	t.Parallel()

	// Verify error messages are non-empty
	tests := []struct {
		name string
		err  error
	}{
		{"ENOENT", ENOENT},
		{"EEXIST", EEXIST},
		{"ENOTDIR", ENOTDIR},
		{"EISDIR", EISDIR},
		{"EBADF", EBADF},
		{"EINVAL", EINVAL},
		{"EIO", EIO},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msg := tt.err.Error()
			assert.NotEmpty(t, msg, "%s should have a non-empty error message", tt.name)
		})
	}
}
