package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"latentfs/internal/common"
)

// testDataFile creates a temporary data file for testing.
// Uses t.TempDir() which automatically cleans up after the test.
func testDataFile(t *testing.T) (*DataFile, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.latentfs")

	df, err := Create(path)
	require.NoError(t, err, "failed to create data file")

	return df, func() {
		df.Close()
	}
}

func TestCreate(t *testing.T) {
	t.Parallel()

	t.Run("creates new file", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "new.latentfs")

		df, err := Create(path)
		require.NoError(t, err)
		defer df.Close()

		// Verify file exists
		_, err = os.Stat(path)
		assert.NoError(t, err, "data file should exist")

		// Verify path is returned correctly
		assert.Equal(t, path, df.Path())

		// Verify root inode exists
		root, err := df.GetInode(RootIno)
		require.NoError(t, err)
		assert.True(t, root.IsDir(), "root inode should be a directory")
	})

	t.Run("fails when file already exists", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		_, err := Create(df.Path())
		assert.Error(t, err, "Create() should fail when file exists")
	})
}

func TestOpen(t *testing.T) {
	t.Parallel()

	t.Run("reopens existing file", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		path := df.Path()
		df.Close()
		defer cleanup()

		df2, err := Open(path)
		require.NoError(t, err)
		defer df2.Close()

		root, err := df2.GetInode(RootIno)
		require.NoError(t, err)
		assert.True(t, root.IsDir(), "root inode should be a directory")
	})

	t.Run("fails for nonexistent file", func(t *testing.T) {
		t.Parallel()
		_, err := Open("/nonexistent/path/file.latentfs")
		assert.Error(t, err)
	})
}

func TestInode(t *testing.T) {
	t.Parallel()

	t.Run("GetInode returns ErrNotFound for missing inode", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		_, err := df.GetInode(99999)
		assert.ErrorIs(t, err, common.ErrNotFound)
	})

	t.Run("CreateInode creates file inode", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, err := df.CreateInode(DefaultFileMode)
		require.NoError(t, err)
		assert.Greater(t, ino, int64(0), "should return valid inode number")

		inode, err := df.GetInode(ino)
		require.NoError(t, err)
		assert.True(t, inode.IsFile(), "created inode should be a file")
		assert.Equal(t, int64(0), inode.Size)
		assert.Equal(t, int32(1), inode.Nlink)
	})

	t.Run("CreateInode creates directory inode", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, err := df.CreateInode(DefaultDirMode)
		require.NoError(t, err)

		inode, err := df.GetInode(ino)
		require.NoError(t, err)
		assert.True(t, inode.IsDir(), "created inode should be a directory")
	})

	t.Run("DeleteInode removes inode", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, err := df.CreateInode(DefaultFileMode)
		require.NoError(t, err)

		err = df.DeleteInode(ino)
		require.NoError(t, err)

		_, err = df.GetInode(ino)
		assert.ErrorIs(t, err, common.ErrNotFound)
	})
}

func TestUpdateInode(t *testing.T) {
	t.Parallel()

	t.Run("updates size", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, err := df.CreateInode(DefaultFileMode)
		require.NoError(t, err)

		newSize := int64(1024)
		err = df.UpdateInode(ino, &InodeUpdate{Size: &newSize})
		require.NoError(t, err)

		inode, err := df.GetInode(ino)
		require.NoError(t, err)
		assert.Equal(t, int64(1024), inode.Size)
	})

	t.Run("updates multiple fields", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, err := df.CreateInode(DefaultFileMode)
		require.NoError(t, err)

		newMode := uint32(ModeFile | 0600)
		newSize := int64(2048)
		newTime := time.Now().Add(-time.Hour)

		err = df.UpdateInode(ino, &InodeUpdate{
			Mode:  &newMode,
			Size:  &newSize,
			Mtime: &newTime,
		})
		require.NoError(t, err)

		inode, err := df.GetInode(ino)
		require.NoError(t, err)
		assert.Equal(t, uint32(0600), inode.Permissions())
		assert.Equal(t, int64(2048), inode.Size)
	})

	t.Run("handles nil update", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, err := df.CreateInode(DefaultFileMode)
		require.NoError(t, err)

		err = df.UpdateInode(ino, nil)
		assert.NoError(t, err)
	})
}

func TestLookup(t *testing.T) {
	t.Parallel()

	t.Run("finds existing entry", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		fileIno, err := df.CreateInode(DefaultFileMode)
		require.NoError(t, err)

		err = df.CreateDentry(RootIno, "test.txt", fileIno)
		require.NoError(t, err)

		dentry, err := df.Lookup(RootIno, "test.txt")
		require.NoError(t, err)
		assert.Equal(t, "test.txt", dentry.Name)
		assert.Equal(t, fileIno, dentry.Ino)
	})

	t.Run("returns ErrNotFound for missing entry", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		_, err := df.Lookup(RootIno, "nonexistent")
		assert.ErrorIs(t, err, common.ErrNotFound)
	})
}

func TestListDir(t *testing.T) {
	t.Parallel()

	t.Run("lists entries in sorted order", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		for _, name := range []string{"c.txt", "a.txt", "b.txt"} {
			ino, err := df.CreateInode(DefaultFileMode)
			require.NoError(t, err)
			err = df.CreateDentry(RootIno, name, ino)
			require.NoError(t, err)
		}

		entries, err := df.ListDir(RootIno)
		require.NoError(t, err)
		assert.Len(t, entries, 3)
		assert.Equal(t, "a.txt", entries[0].Name)
		assert.Equal(t, "b.txt", entries[1].Name)
		assert.Equal(t, "c.txt", entries[2].Name)
	})

	t.Run("returns empty for empty directory", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		entries, err := df.ListDir(RootIno)
		require.NoError(t, err)
		assert.Empty(t, entries)
	})
}

func TestDentryOperations(t *testing.T) {
	t.Parallel()

	t.Run("CreateDentry increments parent nlink for directories", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		root, _ := df.GetInode(RootIno)
		initialNlink := root.Nlink

		dirIno, err := df.CreateInode(DefaultDirMode)
		require.NoError(t, err)
		err = df.CreateDentry(RootIno, "subdir", dirIno)
		require.NoError(t, err)

		root, _ = df.GetInode(RootIno)
		assert.Equal(t, initialNlink+1, root.Nlink)
	})

	t.Run("DeleteDentry removes entry", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		fileIno, _ := df.CreateInode(DefaultFileMode)
		df.CreateDentry(RootIno, "file.txt", fileIno)

		err := df.DeleteDentry(RootIno, "file.txt")
		require.NoError(t, err)

		_, err = df.Lookup(RootIno, "file.txt")
		assert.ErrorIs(t, err, common.ErrNotFound)
	})

	t.Run("RenameDentry moves entry", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		fileIno, _ := df.CreateInode(DefaultFileMode)
		df.CreateDentry(RootIno, "old.txt", fileIno)

		err := df.RenameDentry(RootIno, "old.txt", RootIno, "new.txt")
		require.NoError(t, err)

		_, err = df.Lookup(RootIno, "old.txt")
		assert.ErrorIs(t, err, common.ErrNotFound)

		dentry, err := df.Lookup(RootIno, "new.txt")
		require.NoError(t, err)
		assert.Equal(t, fileIno, dentry.Ino)
	})
}

func TestContent(t *testing.T) {
	t.Parallel()

	t.Run("WriteContent and ReadContent basic", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, _ := df.CreateInode(DefaultFileMode)
		testData := []byte("Hello, World!")

		err := df.WriteContent(ino, 0, testData)
		require.NoError(t, err)

		data, err := df.ReadContent(ino, 0, len(testData))
		require.NoError(t, err)
		assert.Equal(t, string(testData), string(data))

		inode, _ := df.GetInode(ino)
		assert.Equal(t, int64(len(testData)), inode.Size)
	})

	t.Run("WriteContent at offset", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, _ := df.CreateInode(DefaultFileMode)
		df.WriteContent(ino, 0, []byte("Hello"))

		err := df.WriteContent(ino, 5, []byte(" World"))
		require.NoError(t, err)

		data, _ := df.ReadContent(ino, 0, 11)
		assert.Equal(t, "Hello World", string(data))
	})

	t.Run("WriteContent large file spanning chunks", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, _ := df.CreateInode(DefaultFileMode)
		largeData := make([]byte, ChunkSize*3)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		err := df.WriteContent(ino, 0, largeData)
		require.NoError(t, err)

		data, err := df.ReadContent(ino, 0, len(largeData))
		require.NoError(t, err)
		assert.Len(t, data, len(largeData))

		for i, b := range data {
			if b != byte(i%256) {
				t.Errorf("data[%d] = %d, want %d", i, b, i%256)
				break
			}
		}
	})

	t.Run("ReadContent partial chunk", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, _ := df.CreateInode(DefaultFileMode)
		df.WriteContent(ino, 0, []byte("0123456789"))

		data, err := df.ReadContent(ino, 2, 5)
		require.NoError(t, err)
		assert.Equal(t, "23456", string(data))
	})

	t.Run("ReadContent empty file", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, _ := df.CreateInode(DefaultFileMode)

		data, err := df.ReadContent(ino, 0, 100)
		require.NoError(t, err)
		assert.Empty(t, data)
	})

	t.Run("ReadContent zero length", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, _ := df.CreateInode(DefaultFileMode)
		df.WriteContent(ino, 0, []byte("test"))

		data, err := df.ReadContent(ino, 0, 0)
		require.NoError(t, err)
		assert.Nil(t, data)
	})
}

func TestTruncate(t *testing.T) {
	t.Parallel()

	t.Run("TruncateContent shortens file", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, _ := df.CreateInode(DefaultFileMode)
		df.WriteContent(ino, 0, []byte("Hello, World!"))

		err := df.TruncateContent(ino, 5)
		require.NoError(t, err)

		inode, _ := df.GetInode(ino)
		assert.Equal(t, int64(5), inode.Size)

		data, _ := df.ReadContent(ino, 0, 10)
		assert.Equal(t, "Hello", string(data))
	})

	t.Run("TruncateContent to zero", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, _ := df.CreateInode(DefaultFileMode)
		df.WriteContent(ino, 0, []byte("Hello, World!"))

		err := df.TruncateContent(ino, 0)
		require.NoError(t, err)

		inode, _ := df.GetInode(ino)
		assert.Equal(t, int64(0), inode.Size)
	})
}

func TestResolvePath(t *testing.T) {
	t.Parallel()

	t.Run("resolves various paths", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		// Create /dir/file.txt
		dirIno, _ := df.CreateInode(DefaultDirMode)
		df.CreateDentry(RootIno, "dir", dirIno)
		fileIno, _ := df.CreateInode(DefaultFileMode)
		df.CreateDentry(dirIno, "file.txt", fileIno)

		tests := []struct {
			path string
			want int64
		}{
			{"/", RootIno},
			{"", RootIno},
			{"/dir", dirIno},
			{"dir", dirIno},
			{"/dir/file.txt", fileIno},
			{"dir/file.txt", fileIno},
		}

		for _, tt := range tests {
			t.Run(tt.path, func(t *testing.T) {
				ino, err := df.ResolvePath(tt.path)
				require.NoError(t, err)
				assert.Equal(t, tt.want, ino)
			})
		}
	})

	t.Run("returns ErrNotFound for missing path", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		_, err := df.ResolvePath("/nonexistent")
		assert.ErrorIs(t, err, common.ErrNotFound)
	})
}

func TestSymlink(t *testing.T) {
	t.Parallel()

	t.Run("creates and reads symlink", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		ino, _ := df.CreateInode(ModeSymlink | 0777)
		target := "/path/to/target"

		err := df.CreateSymlink(ino, target)
		require.NoError(t, err)

		readTarget, err := df.ReadSymlink(ino)
		require.NoError(t, err)
		assert.Equal(t, target, readTarget)
	})

	t.Run("ReadSymlink returns ErrNotFound for missing", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		_, err := df.ReadSymlink(99999)
		assert.ErrorIs(t, err, common.ErrNotFound)
	})
}

func TestSourceFolder(t *testing.T) {
	t.Parallel()

	df, cleanup := testDataFile(t)
	defer cleanup()

	// Initially no source folder
	assert.Empty(t, df.GetSourceFolder())

	// Set source folder
	sourceFolder := "/path/to/source"
	err := df.SetSourceFolder(sourceFolder)
	require.NoError(t, err)
	assert.Equal(t, sourceFolder, df.GetSourceFolder())

	// Update source folder
	newSource := "/path/to/new/source"
	err = df.SetSourceFolder(newSource)
	require.NoError(t, err)
	assert.Equal(t, newSource, df.GetSourceFolder())
}

func TestForkType(t *testing.T) {
	t.Parallel()

	t.Run("manages fork type", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		// Initially no fork type
		assert.Empty(t, df.GetForkType())
		assert.False(t, df.IsSoftFork())

		// Set soft-fork type
		err := df.SetForkType(ForkTypeSoft)
		require.NoError(t, err)
		assert.Equal(t, ForkTypeSoft, df.GetForkType())
		assert.True(t, df.IsSoftFork())

		// Change to hard-fork type
		err = df.SetForkType(ForkTypeHard)
		require.NoError(t, err)
		assert.Equal(t, ForkTypeHard, df.GetForkType())
		assert.False(t, df.IsSoftFork())
	})

	t.Run("constants have correct values", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, ForkTypeNone)
		assert.Equal(t, "soft", ForkTypeSoft)
		assert.Equal(t, "hard", ForkTypeHard)
	})
}

func TestCheckForExternalChanges(t *testing.T) {
	t.Parallel()

	t.Run("returns false when no changes", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		// First call should return false (no changes since init)
		changed := df.CheckForExternalChanges()
		assert.False(t, changed, "should not detect changes when nothing changed")
	})

	t.Run("returns false for own writes", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		defer cleanup()

		// Make a change using the same connection
		ino, err := df.CreateInode(DefaultFileMode)
		require.NoError(t, err)
		err = df.WriteContent(ino, 0, []byte("test data"))
		require.NoError(t, err)

		// Should NOT detect own writes (PRAGMA data_version only increments for OTHER connections)
		changed := df.CheckForExternalChanges()
		assert.False(t, changed, "should not detect own writes as external changes")
	})

	t.Run("detects external changes from another connection", func(t *testing.T) {
		t.Parallel()
		df1, cleanup := testDataFile(t)
		defer cleanup()

		path := df1.Path()

		// Open a second connection (simulating another process like CLI)
		df2, err := Open(path)
		require.NoError(t, err)
		defer df2.Close()

		// Make a change via the second connection
		ino, err := df2.CreateInode(DefaultFileMode)
		require.NoError(t, err)
		err = df2.WriteContent(ino, 0, []byte("external data"))
		require.NoError(t, err)

		// df1 should detect the external change
		changed := df1.CheckForExternalChanges()
		assert.True(t, changed, "should detect changes from another connection")

		// Subsequent check should return false (no new changes)
		changed = df1.CheckForExternalChanges()
		assert.False(t, changed, "should not detect changes after refresh")
	})

	t.Run("refreshes epochs when external changes detected", func(t *testing.T) {
		t.Parallel()
		df1, cleanup := testDataFile(t)
		defer cleanup()

		path := df1.Path()
		originalEpoch := df1.GetReadableEpoch()

		// Open a second connection and create a new epoch
		df2, err := Open(path)
		require.NoError(t, err)
		defer df2.Close()

		// Create and commit a new epoch via df2
		newEpoch, err := df2.CreateWriteEpoch("ready", "test")
		require.NoError(t, err)
		assert.Greater(t, newEpoch, originalEpoch)

		// df1's cached epoch should still be old
		assert.Equal(t, originalEpoch, df1.GetReadableEpoch())

		// CheckForExternalChanges should detect and refresh
		changed := df1.CheckForExternalChanges()
		assert.True(t, changed)

		// Now df1 should have the new epoch
		assert.Equal(t, newEpoch, df1.GetReadableEpoch())
	})
}
