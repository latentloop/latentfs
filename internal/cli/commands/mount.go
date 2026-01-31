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
	"path/filepath"

	"github.com/spf13/cobra"

	"latentfs/internal/daemon"
)

var mountCmd = &cobra.Command{
	Use:   "mount <mount-point> -d <path>",
	Short: "Mount a .latentfs data file",
	Long: `Mounts a .latentfs data file at the specified mount point.

The daemon will be started automatically if not running.

The mount uses a central mount point with a UUID subfolder and creates
a symlink from the target path to the actual mount location.

Examples:
  latentfs mount ./my-mount -d ./data.latentfs
  latentfs mount /Volumes/mydata --data-file ~/backups/mydata.latentfs`,
	Args: cobra.ExactArgs(1),
	RunE: runMount,
}

var mountLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List active mounts",
	Long:  `Lists all currently active latentfs mounts.`,
	Args:  cobra.NoArgs,
	RunE:  runMountLs,
}

var mountCheckCmd = &cobra.Command{
	Use:   "check <path>...",
	Short: "Check if paths are mounted",
	Long: `Check if one or more paths are currently mounted.

Returns exit code 0 if ALL paths are mounted, non-zero otherwise.
Use -q/--quiet to suppress output (useful in scripts).`,
	Args: cobra.MinimumNArgs(1),
	RunE: runMountCheck,
}

var (
	mountCheckQuiet bool
	mountDataFile   string
)

func init() {
	rootCmd.AddCommand(mountCmd)
	mountCmd.AddCommand(mountLsCmd)
	mountCmd.AddCommand(mountCheckCmd)
	mountCheckCmd.Flags().BoolVarP(&mountCheckQuiet, "quiet", "q", false, "Suppress output, only set exit code")
	mountCmd.Flags().StringVarP(&mountDataFile, "data-file", "d", "", "Path to the .latentfs data file (required)")
	mountCmd.MarkFlagRequired("data-file")
}

func runMount(cmd *cobra.Command, args []string) error {
	mountPoint := args[0]
	dataFile := mountDataFile

	// Resolve data file path
	absDataFile, err := filepath.Abs(dataFile)
	if err != nil {
		return fmt.Errorf("failed to resolve data file path: %w", err)
	}

	// Check file exists
	if _, err := os.Stat(absDataFile); os.IsNotExist(err) {
		return fmt.Errorf("data file not found: %s", absDataFile)
	}

	// Check if data file is inside a mount - causes NFS deadlock
	if err := checkDataFileNotInMount(absDataFile); err != nil {
		return err
	}

	// Resolve mount point
	absMountPoint, err := filepath.Abs(mountPoint)
	if err != nil {
		return fmt.Errorf("failed to resolve mount point: %w", err)
	}

	// Check if target is inside a mount - mounting inside a mount is not supported
	if err := checkTargetNotInMount(absMountPoint); err != nil {
		return err
	}

	// Check target doesn't exist or is an empty directory
	if info, err := os.Lstat(absMountPoint); err == nil {
		// Target exists - check if it's an empty directory
		if !info.IsDir() {
			return fmt.Errorf("target exists and is not a directory: %s", absMountPoint)
		}
		entries, err := os.ReadDir(absMountPoint)
		if err != nil {
			return fmt.Errorf("failed to read target directory: %w", err)
		}
		if len(entries) > 0 {
			return fmt.Errorf("target directory is not empty: %s", absMountPoint)
		}
		// Empty directory - remove it so symlink can be created
		if err := os.Remove(absMountPoint); err != nil {
			return fmt.Errorf("failed to remove empty target directory: %w", err)
		}
	}

	// Start daemon if not running
	if err := StartDaemonIfNeeded(true); err != nil {
		return fmt.Errorf("failed to start daemon: %w", err)
	}

	// Connect to daemon
	client, err := daemon.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to daemon: %w", err)
	}
	defer client.Close()

	// Send mount request (source is read from data file by daemon)
	resp, err := client.Mount(absDataFile, absMountPoint)
	if err != nil {
		return fmt.Errorf("mount request failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}

	fmt.Println(resp.Message)
	return nil
}

func runMountLs(cmd *cobra.Command, args []string) error {
	if !daemon.IsDaemonRunning() {
		fmt.Println("No active mounts (daemon not running)")
		return nil
	}

	client, err := daemon.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to daemon: %w", err)
	}
	defer client.Close()

	resp, err := client.Status()
	if err != nil {
		return fmt.Errorf("failed to get status: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}

	if len(resp.Mounts) == 0 {
		fmt.Println("No active mounts")
		return nil
	}

	fmt.Printf("Active mounts (%d):\n", len(resp.Mounts))
	for _, m := range resp.Mounts {
		switch m.ForkType {
		case "soft":
			fmt.Printf("  %s -> %s [soft-fork]\n", m.DataFile, m.Target)
			fmt.Printf("    source: %s (copy-on-write)\n", m.Source)
		case "hard":
			fmt.Printf("  %s -> %s [hard-fork]\n", m.DataFile, m.Target)
			fmt.Printf("    origin: %s (independent copy)\n", m.Source)
		default:
			fmt.Printf("  %s -> %s\n", m.DataFile, m.Target)
		}
	}

	return nil
}

func runMountCheck(cmd *cobra.Command, args []string) error {
	if !daemon.IsDaemonRunning() {
		if !mountCheckQuiet {
			fmt.Println("daemon not running")
		}
		return fmt.Errorf("daemon not running")
	}

	client, err := daemon.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to daemon: %w", err)
	}
	defer client.Close()

	// Resolve all paths first
	absPaths := make([]string, 0, len(args))
	pathErrors := make(map[string]bool)
	for _, path := range args {
		absPath, err := filepath.Abs(path)
		if err != nil {
			if !mountCheckQuiet {
				fmt.Printf("%s: error resolving path\n", path)
			}
			pathErrors[path] = true
			continue
		}
		absPaths = append(absPaths, absPath)
	}

	// Use batch IPC for efficiency
	mountedPaths, allMounted, err := client.CheckMounts(absPaths)
	if err != nil {
		return fmt.Errorf("failed to check mounts: %w", err)
	}

	// Print results if not quiet
	if !mountCheckQuiet {
		for _, absPath := range absPaths {
			if mountedPaths[absPath] {
				fmt.Printf("%s: mounted\n", absPath)
			} else {
				fmt.Printf("%s: not mounted\n", absPath)
			}
		}
	}

	// Check for path resolution errors
	if len(pathErrors) > 0 {
		allMounted = false
	}

	if !allMounted {
		return fmt.Errorf("one or more paths not mounted")
	}
	return nil
}

// checkDataFileNotInMount checks if the data file path is inside any latentfs mount.
// Returns an error if the data file is inside a mount, as opening it would cause NFS deadlock.
func checkDataFileNotInMount(dataFilePath string) error {
	// Try to connect to daemon - if not running, no mounts exist
	client, err := daemon.Connect()
	if err != nil {
		return nil // Daemon not running, no mounts to check
	}
	defer client.Close()

	// Use IPC to check if path is inside any mount
	isInMount, mountPath, err := client.CheckPathInMount(dataFilePath)
	if err != nil {
		return nil // Can't check, allow to proceed
	}

	if isInMount {
		return fmt.Errorf("data file inside a mount is not supported\n\nThe data file '%s' is inside a latentfs mount at '%s'.\nPlease move the data file to a location outside the mount.", dataFilePath, mountPath)
	}

	return nil
}

// checkTargetNotInMount checks if the target path is inside any latentfs mount.
// Returns an error if the target is inside a mount, as mounting inside a mount is not supported.
func checkTargetNotInMount(targetPath string) error {
	// Try to connect to daemon - if not running, no mounts exist
	client, err := daemon.Connect()
	if err != nil {
		return nil // Daemon not running, no mounts to check
	}
	defer client.Close()

	// Use IPC to check if path is inside any mount
	isInMount, mountPath, err := client.CheckPathInMount(targetPath)
	if err != nil {
		return nil // Can't check, allow to proceed
	}

	if isInMount {
		return fmt.Errorf("mount target inside a mount path is not supported yet\n\nThe target path '%s' is inside a latentfs mount at '%s'.", targetPath, mountPath)
	}

	return nil
}
