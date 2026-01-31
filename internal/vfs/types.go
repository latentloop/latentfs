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
