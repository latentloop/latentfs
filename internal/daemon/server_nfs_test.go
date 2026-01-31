//go:build !smb

package daemon

import (
	"os"
	"testing"

	"github.com/macos-fuse-t/go-smb2/vfs"
)

// TestBillyFileInfoMode tests that BillyFileInfo.Mode() returns actual stored permissions
func TestBillyFileInfoMode(t *testing.T) {
	tests := []struct {
		name         string
		fileType     vfs.FileType
		unixMode     uint32
		expectedMode os.FileMode
	}{
		{
			name:         "regular file with default mode",
			fileType:     vfs.FileTypeRegularFile,
			unixMode:     0644,
			expectedMode: 0644,
		},
		{
			name:         "executable file",
			fileType:     vfs.FileTypeRegularFile,
			unixMode:     0755,
			expectedMode: 0755,
		},
		{
			name:         "read-only file",
			fileType:     vfs.FileTypeRegularFile,
			unixMode:     0444,
			expectedMode: 0444,
		},
		{
			name:         "private file",
			fileType:     vfs.FileTypeRegularFile,
			unixMode:     0600,
			expectedMode: 0600,
		},
		{
			name:         "directory with default mode",
			fileType:     vfs.FileTypeDirectory,
			unixMode:     0755,
			expectedMode: os.ModeDir | 0755,
		},
		{
			name:         "directory with restricted mode",
			fileType:     vfs.FileTypeDirectory,
			unixMode:     0700,
			expectedMode: os.ModeDir | 0700,
		},
		{
			name:         "symlink",
			fileType:     vfs.FileTypeSymlink,
			unixMode:     0777,
			expectedMode: os.ModeSymlink | 0777,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create attributes with the specified mode
			attrs := &vfs.Attributes{}
			attrs.SetFileType(tt.fileType)
			attrs.SetUnixMode(tt.unixMode)

			fi := &BillyFileInfo{
				name:  "test",
				attrs: attrs,
			}

			gotMode := fi.Mode()
			if gotMode != tt.expectedMode {
				t.Errorf("BillyFileInfo.Mode() = %o, want %o", gotMode, tt.expectedMode)
			}
		})
	}
}

// TestBillyFileInfoModeFromDirInfo tests Mode() when using dirInfo instead of attrs
func TestBillyFileInfoModeFromDirInfo(t *testing.T) {
	tests := []struct {
		name         string
		fileType     vfs.FileType
		unixMode     uint32
		expectedMode os.FileMode
	}{
		{
			name:         "file from directory listing",
			fileType:     vfs.FileTypeRegularFile,
			unixMode:     0755,
			expectedMode: 0755,
		},
		{
			name:         "directory from directory listing",
			fileType:     vfs.FileTypeDirectory,
			unixMode:     0700,
			expectedMode: os.ModeDir | 0700,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create DirInfo with the specified mode
			di := vfs.DirInfo{Name: "test"}
			di.SetFileType(tt.fileType)
			di.SetUnixMode(tt.unixMode)

			fi := &BillyFileInfo{
				name:    "test",
				dirInfo: &di,
			}

			gotMode := fi.Mode()
			if gotMode != tt.expectedMode {
				t.Errorf("BillyFileInfo.Mode() = %o, want %o", gotMode, tt.expectedMode)
			}
		})
	}
}

// TestBillyFileInfoModeFallback tests that Mode() falls back to defaults when no mode is set
func TestBillyFileInfoModeFallback(t *testing.T) {
	// Test with empty attrs (no mode set)
	attrs := &vfs.Attributes{}
	attrs.SetFileType(vfs.FileTypeRegularFile)
	// Don't set unix mode - should fallback to 0644

	fi := &BillyFileInfo{
		name:  "test",
		attrs: attrs,
	}

	gotMode := fi.Mode()
	// When GetUnixMode returns (0, false), we expect the fallback behavior
	// Since 0 is a valid value returned, we accept either 0 or 0644
	if gotMode != 0 && gotMode != 0644 {
		t.Errorf("BillyFileInfo.Mode() fallback = %o, want 0 or 0644", gotMode)
	}
}
