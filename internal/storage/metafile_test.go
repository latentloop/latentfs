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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testMetaFile creates a temporary meta file for testing.
// Uses t.TempDir() which automatically cleans up after the test.
func testMetaFile(t *testing.T) (*MetaFile, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test_meta.latentfs")

	mf, err := CreateMeta(path)
	require.NoError(t, err, "failed to create meta file")

	return mf, func() {
		mf.Close()
	}
}

func TestCreateMeta(t *testing.T) {
	t.Parallel()

	t.Run("creates new file", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "new_meta.latentfs")

		mf, err := CreateMeta(path)
		require.NoError(t, err)
		defer mf.Close()

		assert.FileExists(t, path)
		assert.Equal(t, path, mf.Path())
	})

	t.Run("fails when file exists", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		_, err := CreateMeta(mf.Path())
		assert.Error(t, err)
	})
}

func TestOpenMeta(t *testing.T) {
	t.Parallel()

	t.Run("reopens existing file", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		path := mf.Path()
		mf.Close()
		defer cleanup()

		mf2, err := OpenMeta(path)
		require.NoError(t, err)
		defer mf2.Close()

		assert.Equal(t, path, mf2.Path())
	})

	t.Run("fails for nonexistent file", func(t *testing.T) {
		t.Parallel()
		_, err := OpenMeta("/nonexistent/path/meta.latentfs")
		assert.Error(t, err)
	})

	t.Run("fails for wrong file type", func(t *testing.T) {
		t.Parallel()
		df, cleanup := testDataFile(t)
		path := df.Path()
		df.Close()
		defer cleanup()

		_, err := OpenMeta(path)
		assert.Error(t, err)
	})
}

func TestOpenOrCreateMeta(t *testing.T) {
	t.Parallel()

	t.Run("creates new file", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "new_meta.latentfs")

		mf, err := OpenOrCreateMeta(path)
		require.NoError(t, err)
		defer mf.Close()

		assert.FileExists(t, path)
	})

	t.Run("opens existing file with data", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		path := mf.Path()
		mf.AddMount("test", "/data/test.latentfs")
		mf.Close()
		defer cleanup()

		mf2, err := OpenOrCreateMeta(path)
		require.NoError(t, err)
		defer mf2.Close()

		mounts, err := mf2.ListMounts()
		require.NoError(t, err)
		assert.Len(t, mounts, 1)
	})
}

func TestAddMount(t *testing.T) {
	t.Parallel()

	t.Run("adds new mount", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		err := mf.AddMount("subpath", "/path/to/data.latentfs")
		require.NoError(t, err)

		mounts, err := mf.ListMounts()
		require.NoError(t, err)
		require.Len(t, mounts, 1)
		assert.Equal(t, "subpath", mounts[0].SubPath)
		assert.Equal(t, "/path/to/data.latentfs", mounts[0].DataFile)
		assert.Empty(t, mounts[0].SourceFolder)
	})

	t.Run("upserts existing mount", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		mf.AddMount("subpath", "/path/v1.latentfs")
		mf.AddMount("subpath", "/path/v2.latentfs")

		mounts, _ := mf.ListMounts()
		require.Len(t, mounts, 1)
		assert.Equal(t, "/path/v2.latentfs", mounts[0].DataFile)
	})
}

func TestAddForkMount(t *testing.T) {
	t.Parallel()

	t.Run("adds soft fork", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		err := mf.AddForkMount("uuid-123", "/data/fork.latentfs", "/source/folder", ForkTypeSoft, "/user/target")
		require.NoError(t, err)

		mounts, _ := mf.ListMounts()
		require.Len(t, mounts, 1)
		assert.Equal(t, "uuid-123", mounts[0].SubPath)
		assert.Equal(t, "/source/folder", mounts[0].SourceFolder)
		assert.Equal(t, ForkTypeSoft, mounts[0].ForkType)
		assert.Equal(t, "/user/target", mounts[0].SymlinkTarget)
	})

	t.Run("adds empty source as regular mount", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		err := mf.AddForkMount("uuid-456", "/data/fork.latentfs", "", "", "/user/target")
		require.NoError(t, err)

		mounts, _ := mf.ListMounts()
		assert.Empty(t, mounts[0].SourceFolder)
		assert.Empty(t, mounts[0].ForkType)
	})

	t.Run("adds hard fork", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		err := mf.AddForkMount("uuid-789", "/data/hardfork.latentfs", "/source/folder", ForkTypeHard, "/user/target")
		require.NoError(t, err)

		mounts, _ := mf.ListMounts()
		assert.Equal(t, "/source/folder", mounts[0].SourceFolder)
		assert.Equal(t, ForkTypeHard, mounts[0].ForkType)
	})
}

func TestRemoveMount(t *testing.T) {
	t.Parallel()

	t.Run("removes existing mount", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		mf.AddMount("mount1", "/data1.latentfs")
		mf.AddMount("mount2", "/data2.latentfs")

		err := mf.RemoveMount("mount1")
		require.NoError(t, err)

		mounts, _ := mf.ListMounts()
		require.Len(t, mounts, 1)
		assert.Equal(t, "mount2", mounts[0].SubPath)
	})

	t.Run("fails for nonexistent mount", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		err := mf.RemoveMount("nonexistent")
		assert.Error(t, err)
	})
}

func TestListMounts(t *testing.T) {
	t.Parallel()

	t.Run("returns empty for empty meta", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		mounts, err := mf.ListMounts()
		require.NoError(t, err)
		assert.Empty(t, mounts)
	})

	t.Run("returns sorted list", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		mf.AddMount("zebra", "/z.latentfs")
		mf.AddMount("alpha", "/a.latentfs")
		mf.AddMount("middle", "/m.latentfs")

		mounts, _ := mf.ListMounts()
		require.Len(t, mounts, 3)
		assert.Equal(t, "alpha", mounts[0].SubPath)
		assert.Equal(t, "middle", mounts[1].SubPath)
		assert.Equal(t, "zebra", mounts[2].SubPath)
	})
}

func TestLookupMount(t *testing.T) {
	t.Parallel()

	t.Run("finds exact match", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		mf.AddMount("mydata", "/data.latentfs")

		entry, remaining, err := mf.LookupMount("mydata")
		require.NoError(t, err)
		require.NotNil(t, entry)
		assert.Equal(t, "mydata", entry.SubPath)
		assert.Empty(t, remaining)
	})

	t.Run("finds with subpath", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		mf.AddMount("mydata", "/data.latentfs")

		entry, remaining, err := mf.LookupMount("mydata/subdir/file.txt")
		require.NoError(t, err)
		require.NotNil(t, entry)
		assert.Equal(t, "mydata", entry.SubPath)
		assert.Equal(t, "subdir/file.txt", remaining)
	})

	t.Run("returns nil for not found", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		entry, _, err := mf.LookupMount("nonexistent")
		require.NoError(t, err)
		assert.Nil(t, entry)
	})

	t.Run("finds longest match", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		mf.AddMount("data", "/data1.latentfs")
		mf.AddMount("data/nested", "/data2.latentfs")

		entry, remaining, _ := mf.LookupMount("data/nested/file.txt")
		assert.Equal(t, "data/nested", entry.SubPath)
		assert.Equal(t, "file.txt", remaining)
	})
}

func TestConfig(t *testing.T) {
	t.Parallel()

	t.Run("sets and gets config", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		err := mf.SetConfig("key1", "value1")
		require.NoError(t, err)

		value, err := mf.GetConfig("key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", value)
	})

	t.Run("returns empty for nonexistent key", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		value, err := mf.GetConfig("nonexistent")
		require.NoError(t, err)
		assert.Empty(t, value)
	})

	t.Run("upserts config", func(t *testing.T) {
		t.Parallel()
		mf, cleanup := testMetaFile(t)
		defer cleanup()

		mf.SetConfig("key", "v1")
		mf.SetConfig("key", "v2")

		value, _ := mf.GetConfig("key")
		assert.Equal(t, "v2", value)
	})
}

func TestMountEntry_Fields(t *testing.T) {
	t.Parallel()

	mf, cleanup := testMetaFile(t)
	defer cleanup()

	mf.AddForkMount("test-uuid", "/data.latentfs", "/source", ForkTypeSoft, "/target")

	mounts, _ := mf.ListMounts()
	entry := mounts[0]

	assert.Greater(t, entry.ID, int64(0))
	assert.Equal(t, "test-uuid", entry.SubPath)
	assert.Equal(t, "/data.latentfs", entry.DataFile)
	assert.Equal(t, "/source", entry.SourceFolder)
	assert.Equal(t, ForkTypeSoft, entry.ForkType)
	assert.Equal(t, "/target", entry.SymlinkTarget)
	assert.False(t, entry.CreatedAt.IsZero())
}
