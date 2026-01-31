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

package commands

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"latentfs/internal/daemon"
	"latentfs/internal/storage"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// SetVersion sets the version info for --version flag
func SetVersion(v, c, d string) {
	version = v
	commit = c
	date = d
	rootCmd.Version = getVersionString()
}

// getVersionString returns the version string with build info
func getVersionString() string {
	buildDate := formatBuildDate(date)
	if strings.HasSuffix(version, "-dev") {
		// Dev build: include epoch and commit for troubleshooting
		return fmt.Sprintf("%s (%s, epoch: %s, commit: %s)", version, buildDate, date, commit)
	}
	// Prod build: version with date
	return fmt.Sprintf("%s (%s)", version, buildDate)
}

// formatBuildDate converts epoch timestamp to readable date
func formatBuildDate(epoch string) string {
	ts, err := strconv.ParseInt(epoch, 10, 64)
	if err != nil {
		return epoch
	}
	return time.Unix(ts, 0).Format("2006-01-02")
}

var rootCmd = &cobra.Command{
	Use:   "latentfs",
	Short: "Intermediate layer between AI agent and file system",
	Long:  `Intermediate layer between AI agent and the file system. Support data fork/save/restore through OS VFS interface.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Skip initialization for help commands
		if cmd.Name() == "help" || cmd.Name() == "completion" {
			return nil
		}

		// Skip auto-daemon for daemon subcommands (they manage themselves)
		if cmd.Parent() != nil && cmd.Parent().Name() == "daemon" {
			return nil
		}
		if cmd.Name() == "daemon" {
			return nil
		}

		// Initialize config directory
		if err := daemon.InitConfigDir(); err != nil {
			return fmt.Errorf("failed to initialize config: %w", err)
		}

		// Load global settings and set busy_timeout values for CLI
		if settings, err := daemon.LoadGlobalSettings(); err == nil {
			storage.SetConfigBusyTimeouts(settings.DaemonBusyTimeout, settings.CLIBusyTimeout)
		}

		// Auto-start daemon if not running
		if !daemon.IsDaemonRunning() {
			if err := StartDaemonIfNeeded(true); err != nil {
				// Don't fail - just warn, some commands might not need daemon
				fmt.Fprintf(os.Stderr, "Warning: could not auto-start daemon: %v\n", err)
			}
		}

		return nil
	},
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	rootCmd.SetVersionTemplate("latentfs version {{.Version}}\n")
}

// Execute runs the root command
func Execute() error {
	return rootCmd.Execute()
}

