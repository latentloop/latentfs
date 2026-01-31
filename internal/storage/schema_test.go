package storage

import (
	"os"
	"testing"
)

func TestSchemaConstants(t *testing.T) {
	// Verify schema version
	if SchemaVersion != "1" {
		t.Errorf("SchemaVersion = %s, want 1", SchemaVersion)
	}

	// Verify chunk size
	if ChunkSize != 16384 {
		t.Errorf("ChunkSize = %d, want 16384", ChunkSize)
	}
}

func TestModeConstants(t *testing.T) {
	// Verify mode bits match POSIX
	if ModeDeleted != 0 {
		t.Errorf("ModeDeleted = %o, want 0", ModeDeleted)
	}
	if ModeDir != 0040000 {
		t.Errorf("ModeDir = %o, want 040000", ModeDir)
	}
	if ModeFile != 0100000 {
		t.Errorf("ModeFile = %o, want 0100000", ModeFile)
	}
	if ModeSymlink != 0120000 {
		t.Errorf("ModeSymlink = %o, want 0120000", ModeSymlink)
	}
	if ModeMask != 0170000 {
		t.Errorf("ModeMask = %o, want 0170000", ModeMask)
	}
}

func TestDefaultModes(t *testing.T) {
	// Verify default dir mode is directory with 755 permissions
	if DefaultDirMode&ModeMask != ModeDir {
		t.Error("DefaultDirMode should be a directory")
	}
	if DefaultDirMode&0777 != 0755 {
		t.Errorf("DefaultDirMode permissions = %o, want 0755", DefaultDirMode&0777)
	}

	// Verify default file mode is file with 644 permissions
	if DefaultFileMode&ModeMask != ModeFile {
		t.Error("DefaultFileMode should be a file")
	}
	if DefaultFileMode&0777 != 0644 {
		t.Errorf("DefaultFileMode permissions = %o, want 0644", DefaultFileMode&0777)
	}
}

func TestRootIno(t *testing.T) {
	if RootIno != 1 {
		t.Errorf("RootIno = %d, want 1", RootIno)
	}
}

func TestModeMask_Extracts_Type(t *testing.T) {
	tests := []struct {
		name string
		mode uint32
		want uint32
	}{
		{"dir 755", ModeDir | 0755, ModeDir},
		{"file 644", ModeFile | 0644, ModeFile},
		{"symlink 777", ModeSymlink | 0777, ModeSymlink},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.mode & ModeMask
			if got != tt.want {
				t.Errorf("(%o & ModeMask) = %o, want %o", tt.mode, got, tt.want)
			}
		})
	}
}

func TestPermissionMask(t *testing.T) {
	tests := []struct {
		mode uint32
		perm uint32
	}{
		{ModeDir | 0755, 0755},
		{ModeFile | 0644, 0644},
		{ModeSymlink | 0777, 0777},
		{ModeFile | 0600, 0600},
		{ModeDir | 0700, 0700},
	}

	for _, tt := range tests {
		got := tt.mode & 0777
		if got != tt.perm {
			t.Errorf("(%o & 0777) = %o, want %o", tt.mode, got, tt.perm)
		}
	}
}

func TestGetBusyTimeout(t *testing.T) {
	// Save original values
	origDaemonConfig := configDaemonBusyTimeout
	origCLIConfig := configCLIBusyTimeout
	defer func() {
		configDaemonBusyTimeout = origDaemonConfig
		configCLIBusyTimeout = origCLIConfig
	}()

	// Clear env vars for clean test
	os.Unsetenv(EnvBusyTimeout)
	os.Unsetenv(EnvDaemonBusyTimeout)
	os.Unsetenv(EnvCLIBusyTimeout)

	// Test 1: Default value when nothing is set
	configDaemonBusyTimeout = 0
	configCLIBusyTimeout = 0
	if got := GetBusyTimeout(DBContextDaemon); got != DefaultBusyTimeout {
		t.Errorf("default daemon timeout = %d, want %d", got, DefaultBusyTimeout)
	}
	if got := GetBusyTimeout(DBContextCLI); got != DefaultBusyTimeout {
		t.Errorf("default CLI timeout = %d, want %d", got, DefaultBusyTimeout)
	}

	// Test 2: Config file values
	configDaemonBusyTimeout = 5000
	configCLIBusyTimeout = 10000
	if got := GetBusyTimeout(DBContextDaemon); got != 5000 {
		t.Errorf("config daemon timeout = %d, want 5000", got)
	}
	if got := GetBusyTimeout(DBContextCLI); got != 10000 {
		t.Errorf("config CLI timeout = %d, want 10000", got)
	}

	// Test 3: General env var overrides config
	os.Setenv(EnvBusyTimeout, "15000")
	defer os.Unsetenv(EnvBusyTimeout)
	if got := GetBusyTimeout(DBContextDaemon); got != 15000 {
		t.Errorf("general env daemon timeout = %d, want 15000", got)
	}
	if got := GetBusyTimeout(DBContextCLI); got != 15000 {
		t.Errorf("general env CLI timeout = %d, want 15000", got)
	}

	// Test 4: Specific env var overrides general env var
	os.Setenv(EnvDaemonBusyTimeout, "20000")
	defer os.Unsetenv(EnvDaemonBusyTimeout)
	if got := GetBusyTimeout(DBContextDaemon); got != 20000 {
		t.Errorf("specific env daemon timeout = %d, want 20000", got)
	}
	// CLI should still use general env var
	if got := GetBusyTimeout(DBContextCLI); got != 15000 {
		t.Errorf("CLI should use general env = %d, want 15000", got)
	}

	// Test 5: CLI-specific env var
	os.Setenv(EnvCLIBusyTimeout, "25000")
	defer os.Unsetenv(EnvCLIBusyTimeout)
	if got := GetBusyTimeout(DBContextCLI); got != 25000 {
		t.Errorf("specific env CLI timeout = %d, want 25000", got)
	}
}

func TestSetConfigBusyTimeouts(t *testing.T) {
	// Save original values
	origDaemon := configDaemonBusyTimeout
	origCLI := configCLIBusyTimeout
	defer func() {
		configDaemonBusyTimeout = origDaemon
		configCLIBusyTimeout = origCLI
	}()

	SetConfigBusyTimeouts(1000, 2000)
	if configDaemonBusyTimeout != 1000 {
		t.Errorf("configDaemonBusyTimeout = %d, want 1000", configDaemonBusyTimeout)
	}
	if configCLIBusyTimeout != 2000 {
		t.Errorf("configCLIBusyTimeout = %d, want 2000", configCLIBusyTimeout)
	}
}
