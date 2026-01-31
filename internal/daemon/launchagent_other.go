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

