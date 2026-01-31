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
	"strings"

	"github.com/spf13/cobra"

	"latentfs/internal/daemon"
	"latentfs/internal/storage"
)

var infoCmd = &cobra.Command{
	Use:   "info [path]",
	Short: "Show mount information for a path",
	Long: `Check if a path is inside a latentfs mount and show the corresponding data file.

This is useful for determining which data file backs a particular directory or file.
If no path is specified, uses the current directory.

Examples:
  latentfs info
  latentfs info .
  latentfs info ~/projects/my-fork
  latentfs info /path/to/file.txt`,
	Args: cobra.MaximumNArgs(1),
	RunE: runInfo,
}

func init() {
	rootCmd.AddCommand(infoCmd)
}

func runInfo(cmd *cobra.Command, args []string) error {
	targetPath := "."
	if len(args) > 0 {
		targetPath = args[0]
	}

	// Resolve to absolute path
	absPath, err := filepath.Abs(targetPath)
	if err != nil {
		return fmt.Errorf("failed to resolve path: %w", err)
	}

	// Check if path exists
	if _, err := os.Lstat(absPath); err != nil {
		return fmt.Errorf("path not found: %s", absPath)
	}

	// Try to resolve to data file
	dataFilePath, err := resolveFromForkPath(absPath)
	if err != nil {
		fmt.Printf("Path: %s\n", absPath)
		fmt.Println("Mount: not in a latentfs mount")
		return nil
	}

	fmt.Printf("Path: %s\n", absPath)
	fmt.Printf("Mount: yes\n")
	fmt.Printf("Data file: %s\n", dataFilePath)

	// Try to get additional info from the data file
	dataFile, err := storage.OpenWithContext(dataFilePath, storage.DBContextCLI)
	if err == nil {
		defer dataFile.Close()

		// Get source folder if it's a soft-fork
		if sourceFolder := dataFile.GetSourceFolder(); sourceFolder != "" {
			fmt.Printf("Source folder: %s\n", sourceFolder)
		}

		// Get snapshot count
		if snapshots, err := dataFile.ListSnapshots(); err == nil {
			fmt.Printf("Snapshots: %d\n", len(snapshots))
		}
	}

	return nil
}

// resolveFromForkPath resolves a fork path to its data file
func resolveFromForkPath(forkPath string) (string, error) {
	absPath, err := filepath.Abs(forkPath)
	if err != nil {
		return "", err
	}

	// Check if it's a symlink pointing to central mount
	info, err := os.Lstat(absPath)
	if err != nil {
		return "", err
	}

	if info.Mode()&os.ModeSymlink != 0 {
		// Read symlink target
		target, err := os.Readlink(absPath)
		if err != nil {
			return "", err
		}

		// Check if target is under central mount
		centralMount := daemon.CentralMountPath()
		if strings.HasPrefix(target, centralMount) {
			// Extract UUID from path
			subPath := strings.TrimPrefix(target, centralMount)
			subPath = strings.TrimPrefix(subPath, "/")
			parts := strings.SplitN(subPath, "/", 2)
			if len(parts) > 0 && parts[0] != "" {
				// Query meta file for mount info
				metaFile, err := storage.OpenMetaWithContext(daemon.MetaFilePath(), storage.DBContextCLI)
				if err != nil {
					return "", err
				}
				defer metaFile.Close()

				entry, _, err := metaFile.LookupMount(parts[0])
				if err != nil {
					return "", err
				}
				if entry != nil {
					return entry.DataFile, nil
				}
			}
		}
	}

	// Check if path is directly under central mount
	centralMount := daemon.CentralMountPath()
	if strings.HasPrefix(absPath, centralMount+"/") {
		relPath := strings.TrimPrefix(absPath, centralMount+"/")
		parts := strings.SplitN(relPath, "/", 2)
		if len(parts) > 0 && parts[0] != "" {
			if daemon.IsDaemonRunning() {
				metaFile, err := storage.OpenMetaWithContext(daemon.MetaFilePath(), storage.DBContextCLI)
				if err == nil {
					defer metaFile.Close()
					entry, _, err := metaFile.LookupMount(parts[0])
					if err == nil && entry != nil {
						return entry.DataFile, nil
					}
				}
			}
		}
	}

	// Walk up directory tree looking for mount point
	current := absPath
	for {
		parent := filepath.Dir(current)
		if parent == current {
			break
		}

		parentInfo, err := os.Lstat(parent)
		if err == nil && parentInfo.Mode()&os.ModeSymlink != 0 {
			result, err := resolveFromForkPath(parent)
			if err == nil {
				return result, nil
			}
		}

		current = parent
	}

	return "", fmt.Errorf("not inside a latentfs mount")
}
