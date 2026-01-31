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

//go:build darwin

package daemon

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"text/template"
)

const launchAgentLabel = "com.latentfs.daemon"

// LaunchAgent plist template
const launchAgentTemplate = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{{.Label}}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{{.Executable}}</string>
        <string>daemon</string>
        <string>start</string>
        <string>--foreground</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <false/>
    <key>StandardOutPath</key>
    <string>{{.LogPath}}</string>
    <key>StandardErrorPath</key>
    <string>{{.LogPath}}</string>
</dict>
</plist>
`

type launchAgentConfig struct {
	Label      string
	Executable string
	LogPath    string
}

// LaunchAgentPath returns the path to the LaunchAgent plist file
func LaunchAgentPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "Library", "LaunchAgents", launchAgentLabel+".plist")
}

// InstallLaunchAgent creates and installs the LaunchAgent plist.
// The daemon reads log level from settings on startup.
func InstallLaunchAgent() error {
	// Get path to our executable
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}

	// Resolve symlinks to get the actual path
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		return fmt.Errorf("failed to resolve executable path: %w", err)
	}

	// Ensure LaunchAgents directory exists
	launchAgentsDir := filepath.Dir(LaunchAgentPath())
	if err := os.MkdirAll(launchAgentsDir, 0755); err != nil {
		return fmt.Errorf("failed to create LaunchAgents directory: %w", err)
	}

	// Generate plist content
	tmpl, err := template.New("launchagent").Parse(launchAgentTemplate)
	if err != nil {
		return fmt.Errorf("failed to parse template: %w", err)
	}

	config := launchAgentConfig{
		Label:      launchAgentLabel,
		Executable: exe,
		LogPath:    LogPath(),
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, config); err != nil {
		return fmt.Errorf("failed to execute template: %w", err)
	}

	// Write plist file
	if err := os.WriteFile(LaunchAgentPath(), buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write plist: %w", err)
	}

	return nil
}

// UninstallLaunchAgent removes the LaunchAgent plist
func UninstallLaunchAgent() error {
	// Unload first if loaded
	if IsLaunchAgentLoaded() {
		_ = UnloadLaunchAgent() // Ignore errors
	}

	// Remove plist file
	err := os.Remove(LaunchAgentPath())
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove plist: %w", err)
	}

	return nil
}

// LoadLaunchAgent loads the LaunchAgent using launchctl
func LoadLaunchAgent() error {
	cmd := exec.Command("launchctl", "load", LaunchAgentPath())
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("launchctl load failed: %w: %s", err, string(output))
	}
	return nil
}

// UnloadLaunchAgent unloads the LaunchAgent using launchctl
func UnloadLaunchAgent() error {
	cmd := exec.Command("launchctl", "unload", LaunchAgentPath())
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("launchctl unload failed: %w: %s", err, string(output))
	}
	return nil
}

// IsLaunchAgentInstalled checks if the LaunchAgent plist exists
func IsLaunchAgentInstalled() bool {
	_, err := os.Stat(LaunchAgentPath())
	return err == nil
}

// IsLaunchAgentLoaded checks if the LaunchAgent is currently loaded
func IsLaunchAgentLoaded() bool {
	cmd := exec.Command("launchctl", "list", launchAgentLabel)
	err := cmd.Run()
	return err == nil
}

// GetLaunchAgentStatus returns a human-readable status of the LaunchAgent
func GetLaunchAgentStatus() string {
	if !IsLaunchAgentInstalled() {
		return "not installed"
	}
	if IsLaunchAgentLoaded() {
		return "installed and loaded"
	}
	return "installed but not loaded"
}

// LaunchAgentSupported returns true on macOS
func LaunchAgentSupported() bool {
	return true
}

