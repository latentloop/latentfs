//go:build !smb

package vfs

import (
	"time"

	smbvfs "github.com/macos-fuse-t/go-smb2/vfs"
)

// NfsVfsHandle is the handle type used by the NFS adapter
// This provides access to the underlying VFS handle type without
// requiring NFS code to import SMB packages directly
type NfsVfsHandle = smbvfs.VfsHandle

// attrsWrapper wraps *smbvfs.Attributes to implement FileAttrs
type attrsWrapper struct {
	attrs *smbvfs.Attributes
}

func (w *attrsWrapper) GetFileType() FileType {
	return FileType(w.attrs.GetFileType())
}

func (w *attrsWrapper) GetSizeBytes() (uint64, bool) {
	return w.attrs.GetSizeBytes()
}

func (w *attrsWrapper) GetInodeNumber() uint64 {
	return w.attrs.GetInodeNumber()
}

func (w *attrsWrapper) GetLastDataModificationTime() (time.Time, bool) {
	return w.attrs.GetLastDataModificationTime()
}

func (w *attrsWrapper) GetUnixMode() (uint32, bool) {
	return w.attrs.GetUnixMode()
}

// dirInfoWrapper wraps *smbvfs.DirInfo to implement DirEntry
type dirInfoWrapper struct {
	info *smbvfs.DirInfo
}

func (w *dirInfoWrapper) GetFileType() FileType {
	return FileType(w.info.GetFileType())
}

func (w *dirInfoWrapper) GetSizeBytes() (uint64, bool) {
	return w.info.GetSizeBytes()
}

func (w *dirInfoWrapper) GetInodeNumber() uint64 {
	return w.info.GetInodeNumber()
}

func (w *dirInfoWrapper) GetUnixMode() (uint32, bool) {
	return w.info.GetUnixMode()
}

// WrapAttrs wraps an interface{} that may be *smbvfs.Attributes into FileAttrs
// Returns nil if the type is not recognized
func WrapAttrs(v any) FileAttrs {
	if v == nil {
		return nil
	}
	if a, ok := v.(*smbvfs.Attributes); ok {
		return &attrsWrapper{attrs: a}
	}
	return nil
}

// WrapDirInfo wraps an interface{} that may be *smbvfs.DirInfo into DirEntry
// Returns nil if the type is not recognized
func WrapDirInfo(v any) DirEntry {
	if v == nil {
		return nil
	}
	if d, ok := v.(*smbvfs.DirInfo); ok {
		return &dirInfoWrapper{info: d}
	}
	return nil
}

// NewAttrsWithMode creates a new Attributes object with the specified unix mode
// This is used by the NFS adapter to construct attributes for SetAttr calls
func NewAttrsWithMode(mode uint32) *smbvfs.Attributes {
	attrs := &smbvfs.Attributes{}
	attrs.SetUnixMode(mode)
	return attrs
}
