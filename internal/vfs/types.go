package vfs

import "time"

// FileType represents the type of a filesystem entry
type FileType int

const (
	// FileTypeRegularFile is a regular file
	FileTypeRegularFile FileType = iota
	// FileTypeDirectory is a directory
	FileTypeDirectory
	// FileTypeSymlink is a symbolic link
	FileTypeSymlink
)

// FileAttrs is the interface for file attributes
// This abstracts away the underlying VFS implementation (SMB/NFS)
type FileAttrs interface {
	GetFileType() FileType
	GetSizeBytes() (uint64, bool)
	GetInodeNumber() uint64
	GetLastDataModificationTime() (time.Time, bool)
	GetUnixMode() (uint32, bool)
}

// DirEntry is the interface for directory entries
// This abstracts away the underlying VFS implementation (SMB/NFS)
type DirEntry interface {
	GetFileType() FileType
	GetSizeBytes() (uint64, bool)
	GetInodeNumber() uint64
	GetUnixMode() (uint32, bool)
}
