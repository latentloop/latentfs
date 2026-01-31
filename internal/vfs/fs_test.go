package vfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/macos-fuse-t/go-smb2/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"latentfs/internal/storage"
)

// testLatentFS creates a LatentFS for testing.
// Uses t.TempDir() which automatically cleans up after the test.
func testLatentFS(t *testing.T) (*LatentFS, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.latentfs")

	df, err := storage.Create(path)
	require.NoError(t, err, "failed to create data file")

	fs := NewLatentFS(df)

	return fs, func() {
		df.Close()
	}
}

func TestLatentFS(t *testing.T) {
	t.Parallel()

	t.Run("NewLatentFS initializes correctly", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		require.NotNil(t, fs)
		assert.NotNil(t, fs.dataFile)
		assert.NotNil(t, fs.handles)
	})

	t.Run("DataFile returns datafile", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		assert.NotNil(t, fs.DataFile())
	})
}

func TestOpenDir(t *testing.T) {
	t.Parallel()

	t.Run("opens root", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		handle, err := fs.OpenDir("/")
		require.NoError(t, err)
		assert.NotZero(t, handle)
		require.NoError(t, fs.Close(handle))
	})

	t.Run("opens empty path as root", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		handle, err := fs.OpenDir("")
		require.NoError(t, err)
		assert.NotZero(t, handle)
		fs.Close(handle)
	})

	t.Run("returns ENOENT for nonexistent", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		_, err := fs.OpenDir("/nonexistent")
		assert.Equal(t, ENOENT, err)
	})
}

func TestMkdir(t *testing.T) {
	t.Parallel()

	t.Run("creates directory", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		attrs, err := fs.Mkdir("/testdir", 0755)
		require.NoError(t, err)
		require.NotNil(t, attrs)
		assert.Equal(t, vfs.FileTypeDirectory, attrs.GetFileType())

		handle, err := fs.OpenDir("/testdir")
		require.NoError(t, err)
		fs.Close(handle)
	})

	t.Run("creates nested directory", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		fs.Mkdir("/parent", 0755)
		attrs, err := fs.Mkdir("/parent/child", 0755)
		require.NoError(t, err)
		assert.Equal(t, vfs.FileTypeDirectory, attrs.GetFileType())
	})

	t.Run("returns EEXIST for duplicate", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		fs.Mkdir("/testdir", 0755)
		_, err := fs.Mkdir("/testdir", 0755)
		assert.Equal(t, EEXIST, err)
	})

	t.Run("returns ENOENT for missing parent", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		_, err := fs.Mkdir("/nonexistent/child", 0755)
		assert.Equal(t, ENOENT, err)
	})
}

func TestOpen(t *testing.T) {
	t.Parallel()

	t.Run("creates file with O_CREATE", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		handle, err := fs.Open("/test.txt", os.O_CREATE|os.O_RDWR, 0644)
		require.NoError(t, err)
		assert.NotZero(t, handle)
		fs.Close(handle)
	})

	t.Run("returns ENOENT for nonexistent", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		_, err := fs.Open("/nonexistent.txt", os.O_RDONLY, 0)
		assert.Equal(t, ENOENT, err)
	})

	t.Run("opens existing file", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h1, _ := fs.Open("/test.txt", os.O_CREATE|os.O_RDWR, 0644)
		fs.Close(h1)

		h2, err := fs.Open("/test.txt", os.O_RDONLY, 0)
		require.NoError(t, err)
		fs.Close(h2)
	})

	t.Run("returns EISDIR for directory", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		fs.Mkdir("/testdir", 0755)
		_, err := fs.Open("/testdir", os.O_RDONLY, 0)
		assert.Equal(t, EISDIR, err)
	})
}

func TestReadWrite(t *testing.T) {
	t.Parallel()

	t.Run("writes and reads data", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		handle, _ := fs.Open("/test.txt", os.O_CREATE|os.O_RDWR, 0644)
		defer fs.Close(handle)

		data := []byte("Hello, World!")
		n, err := fs.Write(handle, data, 0, 0)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)

		buf := make([]byte, 100)
		n, err = fs.Read(handle, buf, 0, 0)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)
		assert.Equal(t, string(data), string(buf[:n]))
	})

	t.Run("writes at offset", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		handle, _ := fs.Open("/test.txt", os.O_CREATE|os.O_RDWR, 0644)
		defer fs.Close(handle)

		fs.Write(handle, []byte("Hello"), 0, 0)
		fs.Write(handle, []byte(" World"), 5, 0)

		buf := make([]byte, 100)
		n, _ := fs.Read(handle, buf, 0, 0)
		assert.Equal(t, "Hello World", string(buf[:n]))
	})

	t.Run("Read returns EBADF for invalid handle", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		buf := make([]byte, 100)
		_, err := fs.Read(999, buf, 0, 0)
		assert.Equal(t, EBADF, err)
	})

	t.Run("Write returns EBADF for invalid handle", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		_, err := fs.Write(999, []byte("test"), 0, 0)
		assert.Equal(t, EBADF, err)
	})

	t.Run("Read returns EISDIR for directory", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		fs.Mkdir("/testdir", 0755)
		handle, _ := fs.OpenDir("/testdir")
		defer fs.Close(handle)

		buf := make([]byte, 100)
		_, err := fs.Read(handle, buf, 0, 0)
		assert.Equal(t, EISDIR, err)
	})
}

func TestTruncate(t *testing.T) {
	t.Parallel()

	t.Run("truncates file", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		handle, _ := fs.Open("/test.txt", os.O_CREATE|os.O_RDWR, 0644)
		defer fs.Close(handle)

		fs.Write(handle, []byte("Hello, World!"), 0, 0)

		err := fs.Truncate(handle, 5)
		require.NoError(t, err)

		buf := make([]byte, 100)
		n, _ := fs.Read(handle, buf, 0, 0)
		assert.Equal(t, "Hello", string(buf[:n]))
	})

	t.Run("returns EBADF for invalid handle", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		err := fs.Truncate(999, 0)
		assert.Equal(t, EBADF, err)
	})
}

func TestReadDir(t *testing.T) {
	t.Parallel()

	t.Run("returns dot entries for empty dir", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		handle, _ := fs.OpenDir("/")
		defer fs.Close(handle)

		entries, err := fs.ReadDir(handle, 0, 0)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(entries), 2, "should have . and .. entries")
	})

	t.Run("lists files and directories", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h1, _ := fs.Open("/file1.txt", os.O_CREATE, 0644)
		fs.Close(h1)
		h2, _ := fs.Open("/file2.txt", os.O_CREATE, 0644)
		fs.Close(h2)
		fs.Mkdir("/subdir", 0755)

		handle, _ := fs.OpenDir("/")
		defer fs.Close(handle)

		entries, err := fs.ReadDir(handle, 0, 0)
		require.NoError(t, err)
		assert.Len(t, entries, 5, "should have ., .., file1.txt, file2.txt, subdir")
	})

	t.Run("returns EOF on second read", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		handle, _ := fs.OpenDir("/")
		defer fs.Close(handle)

		_, err := fs.ReadDir(handle, 0, 0)
		require.NoError(t, err)

		_, err = fs.ReadDir(handle, 0, 0)
		assert.Error(t, err, "second read should return EOF")
	})

	t.Run("restarts with offset", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		handle, _ := fs.OpenDir("/")
		defer fs.Close(handle)

		fs.ReadDir(handle, 0, 0)

		entries, err := fs.ReadDir(handle, 1, 0)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(entries), 2)
	})
}

func TestGetAttr(t *testing.T) {
	t.Parallel()

	t.Run("returns root for handle 0", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		attrs, err := fs.GetAttr(0)
		require.NoError(t, err)
		assert.Equal(t, vfs.FileTypeDirectory, attrs.GetFileType())
	})

	t.Run("returns file attributes", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		handle, _ := fs.Open("/test.txt", os.O_CREATE|os.O_RDWR, 0644)
		fs.Write(handle, []byte("hello"), 0, 0)

		attrs, err := fs.GetAttr(handle)
		require.NoError(t, err)
		assert.Equal(t, vfs.FileTypeRegularFile, attrs.GetFileType())
		size, _ := attrs.GetSizeBytes()
		assert.Equal(t, uint64(5), size)
		fs.Close(handle)
	})

	t.Run("returns EBADF for invalid handle", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		_, err := fs.GetAttr(999)
		assert.Equal(t, EBADF, err)
	})
}

func TestLookup(t *testing.T) {
	t.Parallel()

	t.Run("finds file", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h, _ := fs.Open("/test.txt", os.O_CREATE, 0644)
		fs.Close(h)

		attrs, err := fs.Lookup(0, "test.txt")
		require.NoError(t, err)
		assert.Equal(t, vfs.FileTypeRegularFile, attrs.GetFileType())
	})

	t.Run("finds root with /", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		attrs, err := fs.Lookup(0, "/")
		require.NoError(t, err)
		assert.Equal(t, vfs.FileTypeDirectory, attrs.GetFileType())
	})

	t.Run("returns ENOENT for nonexistent", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		_, err := fs.Lookup(0, "nonexistent")
		assert.Equal(t, ENOENT, err)
	})

	t.Run("finds nested path", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		fs.Mkdir("/parent", 0755)
		h, _ := fs.Open("/parent/child.txt", os.O_CREATE, 0644)
		fs.Close(h)

		attrs, err := fs.Lookup(0, "parent/child.txt")
		require.NoError(t, err)
		assert.Equal(t, vfs.FileTypeRegularFile, attrs.GetFileType())
	})
}

func TestUnlink(t *testing.T) {
	t.Parallel()

	t.Run("deletes file", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h, _ := fs.Open("/test.txt", os.O_CREATE, 0644)
		err := fs.Unlink(h)
		require.NoError(t, err)
		fs.Close(h)

		_, err = fs.Lookup(0, "test.txt")
		assert.Equal(t, ENOENT, err)
	})

	t.Run("returns EBADF for invalid handle", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		err := fs.Unlink(999)
		assert.Equal(t, EBADF, err)
	})

	t.Run("deletes empty directory via OpenDir handle", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		// Create directory
		_, err := fs.Mkdir("/testdir", 0755)
		require.NoError(t, err)

		// Open directory
		dirHandle, err := fs.OpenDir("/testdir")
		require.NoError(t, err)

		// Unlink directory
		err = fs.Unlink(dirHandle)
		require.NoError(t, err)
		fs.Close(dirHandle)

		// Verify directory is gone
		_, err = fs.Lookup(0, "testdir")
		assert.Equal(t, ENOENT, err)
	})

	t.Run("returns ENOTEMPTY for non-empty directory", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		// Create directory with a file inside
		_, err := fs.Mkdir("/testdir", 0755)
		require.NoError(t, err)
		h, err := fs.Open("/testdir/file.txt", os.O_CREATE, 0644)
		require.NoError(t, err)
		fs.Close(h)

		// Open directory
		dirHandle, err := fs.OpenDir("/testdir")
		require.NoError(t, err)

		// Try to unlink non-empty directory
		err = fs.Unlink(dirHandle)
		assert.Equal(t, ENOTEMPTY, err)
		fs.Close(dirHandle)

		// Verify directory still exists
		_, err = fs.Lookup(0, "testdir")
		require.NoError(t, err)
	})
}

func TestRename(t *testing.T) {
	t.Parallel()

	t.Run("renames file", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h, _ := fs.Open("/old.txt", os.O_CREATE, 0644)
		err := fs.Rename(h, "new.txt", 0)
		require.NoError(t, err)
		fs.Close(h)

		_, err = fs.Lookup(0, "old.txt")
		assert.Equal(t, ENOENT, err)

		_, err = fs.Lookup(0, "new.txt")
		require.NoError(t, err)
	})

	t.Run("renames directory via OpenDir handle", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		// Create directory
		_, err := fs.Mkdir("/olddir", 0755)
		require.NoError(t, err)

		// Open directory
		dirHandle, err := fs.OpenDir("/olddir")
		require.NoError(t, err)

		// Rename directory
		err = fs.Rename(dirHandle, "newdir", 0)
		require.NoError(t, err)
		fs.Close(dirHandle)

		// Verify old name is gone
		_, err = fs.Lookup(0, "olddir")
		assert.Equal(t, ENOENT, err)

		// Verify new name exists
		_, err = fs.Lookup(0, "newdir")
		require.NoError(t, err)
	})
}

func TestMiscOperations(t *testing.T) {
	t.Parallel()

	t.Run("StatFS returns attributes", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		attrs, err := fs.StatFS(0)
		require.NoError(t, err)
		require.NotNil(t, attrs)
		blockSize, _ := attrs.GetBlockSize()
		assert.Equal(t, uint64(4096), blockSize)
	})

	t.Run("FSync succeeds", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h, _ := fs.Open("/test.txt", os.O_CREATE, 0644)
		defer fs.Close(h)

		err := fs.FSync(h)
		assert.NoError(t, err)
	})

	t.Run("Flush succeeds", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h, _ := fs.Open("/test.txt", os.O_CREATE, 0644)
		defer fs.Close(h)

		err := fs.Flush(h)
		assert.NoError(t, err)
	})
}

func TestXattr(t *testing.T) {
	t.Parallel()

	t.Run("Listxattr returns empty", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h, _ := fs.Open("/test.txt", os.O_CREATE, 0644)
		defer fs.Close(h)

		names, err := fs.Listxattr(h)
		assert.NoError(t, err)
		assert.Empty(t, names)
	})

	t.Run("Getxattr returns ENOATTR", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h, _ := fs.Open("/test.txt", os.O_CREATE, 0644)
		defer fs.Close(h)

		buf := make([]byte, 100)
		_, err := fs.Getxattr(h, "user.test", buf)
		assert.Equal(t, ENOATTR, err)
	})

	t.Run("Setxattr succeeds silently", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h, _ := fs.Open("/test.txt", os.O_CREATE, 0644)
		defer fs.Close(h)

		err := fs.Setxattr(h, "user.test", []byte("value"))
		assert.NoError(t, err)
	})

	t.Run("Removexattr succeeds silently", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h, _ := fs.Open("/test.txt", os.O_CREATE, 0644)
		defer fs.Close(h)

		err := fs.Removexattr(h, "user.test")
		assert.NoError(t, err)
	})
}

func TestSymlink(t *testing.T) {
	t.Parallel()

	t.Run("Link returns ENOTSUP", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		_, err := fs.Link(0, 0, "link")
		assert.Equal(t, ENOTSUP, err)
	})

	t.Run("creates symlink", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h, _ := fs.Open("/test.txt", os.O_CREATE, 0644)
		attrs, err := fs.Symlink(h, "/target/path", 0777)
		require.NoError(t, err)
		assert.Equal(t, vfs.FileTypeSymlink, attrs.GetFileType())
		fs.Close(h)
	})

	t.Run("reads symlink target", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h, _ := fs.Open("/link", os.O_CREATE, 0644)
		fs.Symlink(h, "/target/path", 0777)

		target, err := fs.Readlink(h)
		require.NoError(t, err)
		assert.Equal(t, "/target/path", target)
		fs.Close(h)
	})

	t.Run("Readlink returns EINVAL for non-symlink", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		h, _ := fs.Open("/test.txt", os.O_CREATE, 0644)
		defer fs.Close(h)

		_, err := fs.Readlink(h)
		assert.Equal(t, EINVAL, err)
	})
}

func TestInodeToAttributes(t *testing.T) {
	t.Parallel()

	t.Run("regular file", func(t *testing.T) {
		t.Parallel()
		inode := &storage.Inode{
			Ino:   42,
			Mode:  storage.DefaultFileMode,
			Uid:   1000,
			Gid:   1000,
			Size:  1024,
			Nlink: 1,
		}

		attrs := inodeToAttributes(inode)

		assert.Equal(t, vfs.FileTypeRegularFile, attrs.GetFileType())
		size, _ := attrs.GetSizeBytes()
		assert.Equal(t, uint64(1024), size)
		uid, _ := attrs.GetUID()
		assert.Equal(t, uint32(1000), uid)
	})

	t.Run("directory", func(t *testing.T) {
		t.Parallel()
		inode := &storage.Inode{
			Ino:  1,
			Mode: storage.DefaultDirMode,
		}

		attrs := inodeToAttributes(inode)
		assert.Equal(t, vfs.FileTypeDirectory, attrs.GetFileType())
	})

	t.Run("symlink", func(t *testing.T) {
		t.Parallel()
		inode := &storage.Inode{
			Ino:  1,
			Mode: storage.ModeSymlink | 0777,
		}

		attrs := inodeToAttributes(inode)
		assert.Equal(t, vfs.FileTypeSymlink, attrs.GetFileType())
	})
}

// =============================================================================
// Non-cascaded Soft-Fork Tests (sourceIsCascaded optimization)
// =============================================================================

// testSoftForkLatentFS creates a LatentFS with a source folder for soft-fork testing.
// The source folder is a real filesystem path (non-cascaded).
func testSoftForkLatentFS(t *testing.T) (*LatentFS, string, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	dataFilePath := filepath.Join(tmpDir, "test.latentfs")
	sourceDir := filepath.Join(tmpDir, "source")

	// Create source directory
	require.NoError(t, os.MkdirAll(sourceDir, 0755))

	// Create data file
	df, err := storage.Create(dataFilePath)
	require.NoError(t, err, "failed to create data file")

	// Create soft-fork LatentFS (without resolver - simulates non-cascaded fork)
	fs := NewForkLatentFS(df, sourceDir)

	return fs, sourceDir, func() {
		df.Close()
	}
}

func TestIsSourceCascaded(t *testing.T) {
	t.Parallel()

	t.Run("returns false for non-fork LatentFS", func(t *testing.T) {
		t.Parallel()
		fs, cleanup := testLatentFS(t)
		defer cleanup()

		// Regular LatentFS has no resolver and no source folder
		assert.False(t, fs.isSourceCascaded())
	})

	t.Run("returns false for soft-fork without resolver", func(t *testing.T) {
		t.Parallel()
		fs, _, cleanup := testSoftForkLatentFS(t)
		defer cleanup()

		// Soft-fork created with NewForkLatentFS has no resolver
		// This represents a non-cascaded fork (source is real filesystem)
		assert.False(t, fs.isSourceCascaded())
	})

	t.Run("caches result after first call", func(t *testing.T) {
		t.Parallel()
		fs, _, cleanup := testSoftForkLatentFS(t)
		defer cleanup()

		// Call multiple times - should return same result (cached)
		result1 := fs.isSourceCascaded()
		result2 := fs.isSourceCascaded()
		result3 := fs.isSourceCascaded()

		assert.Equal(t, result1, result2)
		assert.Equal(t, result2, result3)
		assert.False(t, result1)
	})
}

func TestNonCascadedSoftForkReadThrough(t *testing.T) {
	t.Parallel()

	t.Run("reads file attrs from source folder", func(t *testing.T) {
		t.Parallel()
		fs, sourceDir, cleanup := testSoftForkLatentFS(t)
		defer cleanup()

		// Create file in source folder
		require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "test.txt"), []byte("content"), 0644))

		// Read through soft-fork - should get source content via direct filesystem access
		attrs, err := fs.GetAttrByPath("/test.txt")
		require.NoError(t, err)
		assert.Equal(t, vfs.FileTypeRegularFile, attrs.GetFileType())
	})

	t.Run("lists directory from source folder", func(t *testing.T) {
		t.Parallel()
		fs, sourceDir, cleanup := testSoftForkLatentFS(t)
		defer cleanup()

		// Create files in source folder
		require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "file1.txt"), []byte("1"), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "file2.txt"), []byte("2"), 0644))
		require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "subdir"), 0755))

		// List root directory - should show source entries
		entries, err := fs.ListDir("/")
		require.NoError(t, err)

		// Should have at least 3 entries (file1.txt, file2.txt, subdir)
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name)
		}
		assert.Contains(t, names, "file1.txt")
		assert.Contains(t, names, "file2.txt")
		assert.Contains(t, names, "subdir")
	})

	t.Run("returns ENOENT for nonexistent source file", func(t *testing.T) {
		t.Parallel()
		fs, _, cleanup := testSoftForkLatentFS(t)
		defer cleanup()

		_, err := fs.GetAttrByPath("/nonexistent.txt")
		assert.Equal(t, ENOENT, err)
	})
}

func TestNonCascadedSoftForkCopyOnWrite(t *testing.T) {
	t.Parallel()

	t.Run("creates new file in overlay", func(t *testing.T) {
		t.Parallel()
		fs, sourceDir, cleanup := testSoftForkLatentFS(t)
		defer cleanup()

		// Create file through VFS
		handle, err := fs.Open("/newfile.txt", os.O_CREATE|os.O_RDWR, 0644)
		require.NoError(t, err)

		content := "new file content"
		_, err = fs.Write(handle, []byte(content), 0, 0)
		require.NoError(t, err)
		fs.Close(handle)

		// Verify file exists in VFS
		attrs, err := fs.GetAttrByPath("/newfile.txt")
		require.NoError(t, err)
		assert.Equal(t, vfs.FileTypeRegularFile, attrs.GetFileType())

		// Verify file does NOT exist in source
		_, err = os.Stat(filepath.Join(sourceDir, "newfile.txt"))
		assert.True(t, os.IsNotExist(err))
	})
}

func TestNonCascadedSoftForkDirectoryOperations(t *testing.T) {
	t.Parallel()

	t.Run("creates directory in overlay", func(t *testing.T) {
		t.Parallel()
		fs, sourceDir, cleanup := testSoftForkLatentFS(t)
		defer cleanup()

		// Create directory through VFS
		attrs, err := fs.Mkdir("/newdir", 0755)
		require.NoError(t, err)
		assert.Equal(t, vfs.FileTypeDirectory, attrs.GetFileType())

		// Verify it's accessible
		handle, err := fs.OpenDir("/newdir")
		require.NoError(t, err)
		fs.Close(handle)

		// Verify directory does NOT exist in source
		_, err = os.Stat(filepath.Join(sourceDir, "newdir"))
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("opens source directory", func(t *testing.T) {
		t.Parallel()
		fs, sourceDir, cleanup := testSoftForkLatentFS(t)
		defer cleanup()

		// Create directory in source
		require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "srcdir"), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "srcdir", "file.txt"), []byte("x"), 0644))

		// Open source directory through VFS
		handle, err := fs.OpenDir("/srcdir")
		require.NoError(t, err)
		defer fs.Close(handle)

		// Read directory entries
		entries, err := fs.ReadDir(handle, 0, 0)
		require.NoError(t, err)

		// Should have . and .. plus file.txt
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name)
		}
		assert.Contains(t, names, ".")
		assert.Contains(t, names, "..")
		assert.Contains(t, names, "file.txt")
	})
}

func TestNonCascadedSoftForkTombstone(t *testing.T) {
	t.Parallel()

	t.Run("delete source file creates tombstone", func(t *testing.T) {
		t.Parallel()
		fs, sourceDir, cleanup := testSoftForkLatentFS(t)
		defer cleanup()

		// Create file in source folder
		require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "to_delete.txt"), []byte("content"), 0644))

		// Verify file is visible through VFS
		attrs, err := fs.GetAttrByPath("/to_delete.txt")
		require.NoError(t, err, "file should be visible before deletion")
		assert.Equal(t, vfs.FileTypeRegularFile, attrs.GetFileType())

		// Delete through VFS
		err = fs.UnlinkByPath("/to_delete.txt")
		require.NoError(t, err, "delete should succeed")

		// Verify file is NOT visible through VFS (tombstone blocks)
		_, err = fs.GetAttrByPath("/to_delete.txt")
		assert.Equal(t, ENOENT, err, "file should return ENOENT after deletion")

		// Verify source file still exists on disk
		_, err = os.Stat(filepath.Join(sourceDir, "to_delete.txt"))
		assert.NoError(t, err, "source file should still exist")
	})
}
