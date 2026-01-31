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
