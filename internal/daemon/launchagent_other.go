//go:build !darwin

package daemon

import "errors"

// ErrLaunchAgentNotSupported is returned on non-macOS platforms
var ErrLaunchAgentNotSupported = errors.New("LaunchAgent is only supported on macOS")

// LaunchAgentPath returns empty string on non-macOS platforms
func LaunchAgentPath() string {
	return ""
}

// InstallLaunchAgent returns an error on non-macOS platforms
func InstallLaunchAgent() error {
	return ErrLaunchAgentNotSupported
}

// UninstallLaunchAgent returns an error on non-macOS platforms
func UninstallLaunchAgent() error {
	return ErrLaunchAgentNotSupported
}

// LoadLaunchAgent returns an error on non-macOS platforms
func LoadLaunchAgent() error {
	return ErrLaunchAgentNotSupported
}

// UnloadLaunchAgent returns an error on non-macOS platforms
func UnloadLaunchAgent() error {
	return ErrLaunchAgentNotSupported
}

// IsLaunchAgentInstalled returns false on non-macOS platforms
func IsLaunchAgentInstalled() bool {
	return false
}

// IsLaunchAgentLoaded returns false on non-macOS platforms
func IsLaunchAgentLoaded() bool {
	return false
}

// GetLaunchAgentStatus returns status on non-macOS platforms
func GetLaunchAgentStatus() string {
	return "not supported on this platform"
}

// LaunchAgentSupported returns false on non-macOS platforms
func LaunchAgentSupported() bool {
	return false
}

