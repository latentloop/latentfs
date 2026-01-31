package daemon

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDaemonName(t *testing.T) {
	// daemonName() always returns "daemon" - test isolation is via LATENTFS_CONFIG_DIR
	assert.Equal(t, "daemon", daemonName())
}

func TestConfigDir(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		original := os.Getenv("LATENTFS_CONFIG_DIR")
		os.Unsetenv("LATENTFS_CONFIG_DIR")
		defer os.Setenv("LATENTFS_CONFIG_DIR", original)

		dir := ConfigDir()
		assert.NotEmpty(t, dir)
		assert.True(t, strings.HasSuffix(dir, ".latentfs"), "should end with .latentfs")
	})

	t.Run("override with LATENTFS_CONFIG_DIR", func(t *testing.T) {
		original := os.Getenv("LATENTFS_CONFIG_DIR")
		os.Setenv("LATENTFS_CONFIG_DIR", "/tmp/test-latentfs-config")
		defer os.Setenv("LATENTFS_CONFIG_DIR", original)

		assert.Equal(t, "/tmp/test-latentfs-config", ConfigDir())
	})
}

func TestPathFunctions(t *testing.T) {
	// Use isolated config dir for test
	tmpDir := t.TempDir()
	original := os.Getenv("LATENTFS_CONFIG_DIR")
	os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
	defer os.Setenv("LATENTFS_CONFIG_DIR", original)

	// daemonName() always returns "daemon"
	tests := []struct {
		name   string
		fn     func() string
		suffix string
	}{
		{"SocketPath", SocketPath, "daemon.sock"},
		{"PidPath", PidPath, "daemon.pid"},
		{"LogPath", LogPath, "daemon.log"},
		{"LockPath", LockPath, "daemon.lock"},
		{"MetaFilePath", MetaFilePath, "daemon_meta.latentfs"},
		{"CentralMountPath", CentralMountPath, "mnt_daemon"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.fn()
			assert.True(t, strings.HasSuffix(path, tt.suffix),
				"%s() = %q should end with %q", tt.name, path, tt.suffix)
			assert.True(t, strings.HasPrefix(path, ConfigDir()),
				"%s() = %q should be in config dir %q", tt.name, path, ConfigDir())
		})
	}
}

func TestEnsureConfigDir(t *testing.T) {
	// Use isolated config dir
	tmpDir := t.TempDir()
	original := os.Getenv("LATENTFS_CONFIG_DIR")
	os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
	defer os.Setenv("LATENTFS_CONFIG_DIR", original)

	err := EnsureConfigDir()
	require.NoError(t, err)

	info, err := os.Stat(ConfigDir())
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestInitConfigDir(t *testing.T) {
	// Use isolated config dir
	tmpDir := t.TempDir()
	originalDir := os.Getenv("LATENTFS_CONFIG_DIR")
	os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
	defer os.Setenv("LATENTFS_CONFIG_DIR", originalDir)

	err := InitConfigDir()
	require.NoError(t, err)

	_, err = os.Stat(CentralMountPath())
	assert.NoError(t, err, "central mount path should be created")

	// Verify settings file was created
	_, err = os.Stat(GlobalSettingsPath())
	assert.NoError(t, err, "global settings file should be created")
}

func TestGlobalSettings(t *testing.T) {
	t.Run("defaults from embedded artifact", func(t *testing.T) {
		// Use isolated config dir to test fallback to embedded defaults
		tmpDir := t.TempDir()
		original := os.Getenv("LATENTFS_CONFIG_DIR")
		os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
		defer os.Setenv("LATENTFS_CONFIG_DIR", original)

		// LoadGlobalSettings should return defaults from embedded artifact when file doesn't exist
		settings, err := LoadGlobalSettings()
		require.NoError(t, err)

		assert.False(t, settings.LoginStart)   // default is false
		assert.False(t, settings.MountOnStart) // default is false
		assert.Empty(t, settings.LogLevel)     // default is empty (off)
		assert.Equal(t, 30000, settings.DaemonBusyTimeout)
		assert.Equal(t, 30000, settings.CLIBusyTimeout)
	})

	t.Run("save and load", func(t *testing.T) {
		// Use isolated config dir
		tmpDir := t.TempDir()
		original := os.Getenv("LATENTFS_CONFIG_DIR")
		os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
		defer os.Setenv("LATENTFS_CONFIG_DIR", original)

		settings := &GlobalSettings{
			LoginStart:        false,
			MountOnStart:      true,
			LogLevel:          "debug",
			DaemonBusyTimeout: 5000,
			CLIBusyTimeout:    10000,
		}

		err := SaveGlobalSettings(settings)
		require.NoError(t, err)

		loaded, err := LoadGlobalSettings()
		require.NoError(t, err)

		assert.False(t, loaded.LoginStart)
		assert.True(t, loaded.MountOnStart)
		assert.Equal(t, "debug", loaded.LogLevel)
		assert.Equal(t, 5000, loaded.DaemonBusyTimeout)
		assert.Equal(t, 10000, loaded.CLIBusyTimeout)
	})

	t.Run("MountOnStart save and load", func(t *testing.T) {
		// Use isolated config dir
		tmpDir := t.TempDir()
		original := os.Getenv("LATENTFS_CONFIG_DIR")
		os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
		defer os.Setenv("LATENTFS_CONFIG_DIR", original)

		// Test MountOnStart = false (default)
		settings := &GlobalSettings{MountOnStart: false}
		err := SaveGlobalSettings(settings)
		require.NoError(t, err)

		loaded, err := LoadGlobalSettings()
		require.NoError(t, err)
		assert.False(t, loaded.MountOnStart)

		// Test MountOnStart = true
		settings.MountOnStart = true
		err = SaveGlobalSettings(settings)
		require.NoError(t, err)

		loaded, err = LoadGlobalSettings()
		require.NoError(t, err)
		assert.True(t, loaded.MountOnStart)
	})
}
