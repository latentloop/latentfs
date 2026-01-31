package storage

import "time"

// Inode represents file/directory metadata
type Inode struct {
	Ino   int64
	Mode  uint32
	Uid   uint32
	Gid   uint32
	Size  int64
	Atime time.Time
	Mtime time.Time
	Ctime time.Time
	Nlink int32
}

// IsDir returns true if the inode is a directory
func (i *Inode) IsDir() bool {
	return i.Mode&ModeMask == ModeDir
}

// IsFile returns true if the inode is a regular file
func (i *Inode) IsFile() bool {
	return i.Mode&ModeMask == ModeFile
}

// IsSymlink returns true if the inode is a symbolic link
func (i *Inode) IsSymlink() bool {
	return i.Mode&ModeMask == ModeSymlink
}

// IsDeleted returns true if the inode is a tombstone (deleted source file marker)
func (i *Inode) IsDeleted() bool {
	return i.Mode == ModeDeleted
}

// Permissions returns the permission bits
func (i *Inode) Permissions() uint32 {
	return i.Mode & 0777
}

// Dentry represents a directory entry
type Dentry struct {
	ParentIno int64
	Name      string
	Ino       int64
}

// DirEntry represents a directory entry with full info for listing
type DirEntry struct {
	Name  string
	Ino   int64
	Mode  uint32
	Size  int64
	Mtime time.Time
}

// InodeUpdate represents fields to update on an inode
type InodeUpdate struct {
	Mode  *uint32
	Uid   *uint32
	Gid   *uint32
	Size  *int64
	Atime *time.Time
	Mtime *time.Time
}
