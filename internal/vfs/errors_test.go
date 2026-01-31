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
