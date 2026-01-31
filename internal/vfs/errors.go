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

import "syscall"

// VFS error codes mapped to syscall errors
var (
	ENOENT    = syscall.ENOENT    // No such file or directory
	EEXIST    = syscall.EEXIST    // File exists
	ENOTDIR   = syscall.ENOTDIR   // Not a directory
	EISDIR    = syscall.EISDIR    // Is a directory
	EBADF     = syscall.EBADF     // Bad file descriptor
	EINVAL    = syscall.EINVAL    // Invalid argument
	ENOTSUP   = syscall.ENOTSUP   // Operation not supported
	ENOSPC    = syscall.ENOSPC    // No space left on device
	EIO       = syscall.EIO       // I/O error
	EACCES    = syscall.EACCES    // Permission denied
	EPERM     = syscall.EPERM     // Operation not permitted
	EROFS     = syscall.EROFS     // Read-only file system
	ENOATTR   = syscall.ENOATTR   // Attribute not found (xattr)
	ENOTEMPTY = syscall.ENOTEMPTY // Directory not empty
)
