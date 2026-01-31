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

package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInode_IsDir(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     uint32
		expected bool
	}{
		{"directory", ModeDir | 0755, true},
		{"file", ModeFile | 0644, false},
		{"symlink", ModeSymlink | 0777, false},
		{"default dir mode", DefaultDirMode, true},
		{"default file mode", DefaultFileMode, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			i := &Inode{Mode: tt.mode}
			assert.Equal(t, tt.expected, i.IsDir(), "mode=%o", tt.mode)
		})
	}
}

func TestInode_IsFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     uint32
		expected bool
	}{
		{"directory", ModeDir | 0755, false},
		{"file", ModeFile | 0644, true},
		{"symlink", ModeSymlink | 0777, false},
		{"default dir mode", DefaultDirMode, false},
		{"default file mode", DefaultFileMode, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			i := &Inode{Mode: tt.mode}
			assert.Equal(t, tt.expected, i.IsFile(), "mode=%o", tt.mode)
		})
	}
}

func TestInode_IsSymlink(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     uint32
		expected bool
	}{
		{"directory", ModeDir | 0755, false},
		{"file", ModeFile | 0644, false},
		{"symlink", ModeSymlink | 0777, true},
		{"symlink with different perms", ModeSymlink | 0644, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			i := &Inode{Mode: tt.mode}
			assert.Equal(t, tt.expected, i.IsSymlink(), "mode=%o", tt.mode)
		})
	}
}

func TestInode_IsDeleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     uint32
		expected bool
	}{
		{"tombstone", ModeDeleted, true},
		{"directory", ModeDir | 0755, false},
		{"file", ModeFile | 0644, false},
		{"symlink", ModeSymlink | 0777, false},
		{"default dir mode", DefaultDirMode, false},
		{"default file mode", DefaultFileMode, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			i := &Inode{Mode: tt.mode}
			assert.Equal(t, tt.expected, i.IsDeleted(), "mode=%o", tt.mode)
		})
	}
}

func TestInode_Permissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     uint32
		expected uint32
	}{
		{"dir 755", ModeDir | 0755, 0755},
		{"file 644", ModeFile | 0644, 0644},
		{"symlink 777", ModeSymlink | 0777, 0777},
		{"file 600", ModeFile | 0600, 0600},
		{"dir 700", ModeDir | 0700, 0700},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			i := &Inode{Mode: tt.mode}
			assert.Equal(t, tt.expected, i.Permissions())
		})
	}
}

func TestInode_FullStruct(t *testing.T) {
	t.Parallel()

	now := time.Now()
	i := &Inode{
		Ino:   42,
		Mode:  DefaultFileMode,
		Uid:   1000,
		Gid:   1000,
		Size:  1024,
		Atime: now,
		Mtime: now,
		Ctime: now,
		Nlink: 1,
	}

	assert.Equal(t, int64(42), i.Ino)
	assert.Equal(t, int64(1024), i.Size)
	assert.Equal(t, int32(1), i.Nlink)
	assert.True(t, i.IsFile())
}

func TestDentry(t *testing.T) {
	t.Parallel()

	d := &Dentry{
		ParentIno: 1,
		Name:      "test.txt",
		Ino:       2,
	}

	assert.Equal(t, int64(1), d.ParentIno)
	assert.Equal(t, "test.txt", d.Name)
	assert.Equal(t, int64(2), d.Ino)
}

func TestDirEntry(t *testing.T) {
	t.Parallel()

	now := time.Now()
	e := &DirEntry{
		Name:  "subdir",
		Ino:   3,
		Mode:  DefaultDirMode,
		Size:  0,
		Mtime: now,
	}

	assert.Equal(t, "subdir", e.Name)
	assert.Equal(t, uint32(ModeDir), e.Mode&ModeMask, "expected mode to indicate directory")
}

func TestInodeUpdate(t *testing.T) {
	t.Parallel()

	mode := uint32(0755)
	size := int64(2048)
	now := time.Now()

	update := &InodeUpdate{
		Mode:  &mode,
		Size:  &size,
		Mtime: &now,
	}

	assert.NotNil(t, update.Mode)
	assert.Equal(t, uint32(0755), *update.Mode)
	assert.NotNil(t, update.Size)
	assert.Equal(t, int64(2048), *update.Size)
	assert.NotNil(t, update.Mtime)
	assert.Nil(t, update.Uid)
	assert.Nil(t, update.Gid)
	assert.Nil(t, update.Atime)
}
