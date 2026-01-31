package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"latentfs/internal/artifacts"
)

// getConfigDir returns the config directory path.
// Uses LATENTFS_CONFIG_DIR env var if set, otherwise defaults to ~/.latentfs.
// This is computed dynamically to support test isolation.
func getConfigDir() string {
	if dir := os.Getenv("LATENTFS_CONFIG_DIR"); dir != "" {
		return dir
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".latentfs")
}

// daemonName returns the fixed daemon name "daemon".
// Test isolation is achieved via LATENTFS_CONFIG_DIR instead of multiple daemon names.
func daemonName() string {
	return "daemon"
}


// ConfigDir returns the configuration directory path
func ConfigDir() string {
	return getConfigDir()
}

// SocketPath returns the Unix socket path
func SocketPath() string {
	return filepath.Join(getConfigDir(), daemonName()+".sock")
}

// PidPath returns the PID file path
func PidPath() string {
	return filepath.Join(getConfigDir(), daemonName()+".pid")
}

// LogPath returns the log file path.
// Uses LATENTFS_DAEMON_LOG env var if set, otherwise defaults to config_dir/daemon_name.log.
func LogPath() string {
	if envPath := os.Getenv("LATENTFS_DAEMON_LOG"); envPath != "" {
		return envPath
	}
	return filepath.Join(getConfigDir(), daemonName()+".log")
}

// LockPath returns the lock file path
func LockPath() string {
	return filepath.Join(getConfigDir(), daemonName()+".lock")
}

// GlobalSettingsPath returns the global settings file path
// This file is shared across all daemon instances
func GlobalSettingsPath() string {
	return filepath.Join(getConfigDir(), "settings.yaml")
}

// MetaFilePath returns the path to the central metadata file (daemon-specific)
func MetaFilePath() string {
	return filepath.Join(getConfigDir(), daemonName()+"_meta.latentfs")
}

// CentralMountPath returns the path to the central mount point
func CentralMountPath() string {
	return filepath.Join(getConfigDir(), "mnt_"+daemonName())
}

// EnsureConfigDir creates the config directory if it doesn't exist
func EnsureConfigDir() error {
	return os.MkdirAll(getConfigDir(), 0700)
}

// InitConfigDir initializes the config directory with default files
func InitConfigDir() error {
	// Create config directory
	if err := EnsureConfigDir(); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// Create default global settings file if not exists (using template)
	settingsPath := GlobalSettingsPath()
	if _, err := os.Stat(settingsPath); os.IsNotExist(err) {
		if err := os.WriteFile(settingsPath, artifacts.GlobalSettings, 0600); err != nil {
			return fmt.Errorf("failed to create default settings: %w", err)
		}
	}

	// Create central mount point directory if not exists
	mountPath := CentralMountPath()
	if err := os.MkdirAll(mountPath, 0755); err != nil {
		return fmt.Errorf("failed to create mount point: %w", err)
	}

	return nil
}

// ProjectConfig represents per-project configuration from {project_dir}/.latentfs/config.yaml
type ProjectConfig struct {
	Autosave     bool     `yaml:"autosave"`
	Logging      string   `yaml:"logging"`         // logging level: none, debug, info, trace (case insensitive)
	DataFile     string   `yaml:"datafile"`        // default: "autosave.latentfs"
	Gitignore    *bool    `yaml:"gitignore"`       // default: true (pointer to detect missing)
	Includes     []string `yaml:"includes"`        // default: [".git"]
	Excludes     []string `yaml:"excludes"`        // default: [] (force-exclude paths)
	SaveStrategy string   `yaml:"save-strategy"`   // default: "standard"
}

// ApplyDefaults fills zero-value fields with their defaults.
func (cfg *ProjectConfig) ApplyDefaults() {
	if cfg.DataFile == "" {
		cfg.DataFile = "autosave.latentfs"
	}
	if cfg.Gitignore == nil {
		t := true
		cfg.Gitignore = &t
	}
	if cfg.Includes == nil {
		cfg.Includes = []string{".git"}
	}
	if cfg.SaveStrategy == "" {
		cfg.SaveStrategy = "standard"
	}
}

// GitignoreEnabled returns whether gitignore filtering is enabled (defaults to true).
func (cfg *ProjectConfig) GitignoreEnabled() bool {
	if cfg.Gitignore == nil {
		return true
	}
	return *cfg.Gitignore
}

// LoggingEnabled returns whether logging is enabled (any level other than "none" or empty).
func (cfg *ProjectConfig) LoggingEnabled() bool {
	level := strings.ToLower(cfg.Logging)
	return level != "" && level != "none"
}

// LogLevel returns the normalized (lowercase) logging level.
// Returns empty string if logging is disabled.
func (cfg *ProjectConfig) LogLevel() string {
	return strings.ToLower(cfg.Logging)
}

// LoadProjectConfig loads the project config from {projectDir}/.latentfs/config.yaml.
// Returns nil if the config file does not exist.
func LoadProjectConfig(projectDir string) (*ProjectConfig, error) {
	if projectDir == "" {
		return nil, nil
	}
	configPath := filepath.Join(projectDir, ".latentfs", "config.yaml")
	return LoadProjectConfigFromPath(configPath)
}

// LoadProjectConfigFromDir loads the project config from {projectDir}/{configDir}/config.yaml.
// configDir is a relative path to the project directory (e.g., ".latentfs").
// Returns nil if the config file does not exist.
func LoadProjectConfigFromDir(projectDir, configDir string) (*ProjectConfig, error) {
	if projectDir == "" || configDir == "" {
		return nil, nil
	}
	configPath := filepath.Join(projectDir, configDir, "config.yaml")
	return LoadProjectConfigFromPath(configPath)
}

// LoadProjectConfigFromPath loads the project config from a specific config file path.
// Returns nil if the config file does not exist.
func LoadProjectConfigFromPath(configPath string) (*ProjectConfig, error) {
	if configPath == "" {
		return nil, nil
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cfg ProjectConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()
	return &cfg, nil
}

// SaveStrategyConfig defines thresholds for autosave triggering
type SaveStrategyConfig struct {
	SizeThreshold int64 `yaml:"size_threshold"` // Minimum cumulative size change (bytes) to trigger save
	TimeInterval  int   `yaml:"time_interval"`  // Minimum time (seconds) between saves
}

// globalSettingsWithStrategies is used to parse save strategies from settings file
type globalSettingsWithStrategies struct {
	SaveStrategies map[string]SaveStrategyConfig `yaml:"save_strategies"`
}

// loadSaveStrategies reads save strategies from the global settings file.
// Falls back to embedded defaults if file doesn't exist or is missing strategies.
func loadSaveStrategies() map[string]SaveStrategyConfig {
	// Try to read from user's settings file first
	data, err := os.ReadFile(GlobalSettingsPath())
	if err == nil {
		var settings globalSettingsWithStrategies
		if yaml.Unmarshal(data, &settings) == nil && settings.SaveStrategies != nil {
			return settings.SaveStrategies
		}
	}

	// Fall back to embedded defaults
	var embedded globalSettingsWithStrategies
	if err := yaml.Unmarshal(artifacts.GlobalSettings, &embedded); err != nil {
		panic("failed to parse embedded global settings: " + err.Error())
	}
	return embedded.SaveStrategies
}

// GetSaveStrategy returns the SaveStrategyConfig for the given strategy name.
// Always reads from global settings file to get latest config.
// Falls back to standard if the name is unknown.
func GetSaveStrategy(name string) SaveStrategyConfig {
	strategies := loadSaveStrategies()
	if strategy, ok := strategies[name]; ok {
		return strategy
	}
	return strategies["standard"]
}

// GlobalSettings represents global daemon settings
type GlobalSettings struct {
	LoginStart        bool   `yaml:"login_start"`         // Start daemon on login (default: false)
	MountOnStart      bool   `yaml:"mount_on_start"`      // Restore mounts from previous session on daemon start (default: false)
	LogLevel          string `yaml:"log_level"`           // Log level: trace, debug, info, warn, off (default: off)
	DaemonBusyTimeout int    `yaml:"daemon_busy_timeout"` // SQLite busy_timeout for daemon (ms), 0 = use default
	CLIBusyTimeout    int    `yaml:"cli_busy_timeout"`    // SQLite busy_timeout for CLI (ms), 0 = use default
}

// loadDefaultGlobalSettings parses default settings from embedded artifact.
func loadDefaultGlobalSettings() GlobalSettings {
	var settings GlobalSettings
	if err := yaml.Unmarshal(artifacts.GlobalSettings, &settings); err != nil {
		panic("failed to parse embedded global settings: " + err.Error())
	}
	return settings
}

// LoadGlobalSettings loads the global settings from ~/.latentfs/settings.yaml.
// Always reads from file to get latest config. Falls back to embedded defaults if file doesn't exist.
func LoadGlobalSettings() (*GlobalSettings, error) {
	data, err := os.ReadFile(GlobalSettingsPath())
	if err != nil {
		if os.IsNotExist(err) {
			// Return defaults from embedded artifact
			settings := loadDefaultGlobalSettings()
			return &settings, nil
		}
		return nil, err
	}

	var settings GlobalSettings
	if err := yaml.Unmarshal(data, &settings); err != nil {
		return nil, err
	}

	return &settings, nil
}

// SaveGlobalSettings saves the global settings to ~/.latentfs/settings.yaml
func SaveGlobalSettings(settings *GlobalSettings) error {
	if err := EnsureConfigDir(); err != nil {
		return err
	}
	data, err := yaml.Marshal(settings)
	if err != nil {
		return err
	}
	// Add header comment (same as template header)
	header := []byte("# LatentFS daemon settings\n# See: latentfs settings --help\n\n")
	return os.WriteFile(GlobalSettingsPath(), append(header, data...), 0600)
}

