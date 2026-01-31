package commands

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"latentfs/internal/daemon"
	"latentfs/internal/storage"
)

var forkCmd = &cobra.Command{
	Use:   "fork [source] [flags]",
	Short: "Fork a folder with copy-on-write semantics",
	Long: `Creates a copy-on-write overlay on top of an existing folder.

The fork command:
  - Creates a data file (-d/--data-file or derived from --mount)
  - Optionally mounts it and creates a symlink from <target> to the mount
  - Reads fall through to the source folder (overlay mode)
  - Writes are captured in the overlay

Fork types (--type):
  - overlay: Copy-on-write semantics (default). Reads fall through to source.
  - full: Copies all files from source into the data file.

Examples:
  # Fork and mount an existing folder (overlay mode)
  latentfs fork ~/projects/myapp -m ~/experiments/myapp-test

  # Full fork (copies all files)
  latentfs fork ~/projects/myapp -m ~/experiments/test --type=full

  # Fork without mounting (create data file only)
  latentfs fork ~/projects/myapp -d ~/data/myapp.latentfs

  # Create an empty data file without mounting
  latentfs fork --source-empty -d ~/data/empty.latentfs

  # Create an empty fork and mount it
  latentfs fork --source-empty -m ~/workspace/newproject`,
	Args: cobra.RangeArgs(0, 1),
	RunE: runFork,
}

var (
	sourceEmpty      bool
	forkDataFile     string
	forkMount        string
	forkType         string
	forkAllowPartial bool
	forkConfigDir    string
)

func init() {
	forkCmd.Flags().BoolVar(&sourceEmpty, "source-empty", false, "Create an empty fork (no source folder)")
	forkCmd.Flags().StringVarP(&forkDataFile, "data-file", "d", "", "Custom path for the data file")
	forkCmd.Flags().StringVarP(&forkMount, "mount", "m", "", "Target path for the mount symlink")
	forkCmd.Flags().StringVarP(&forkType, "type", "t", "overlay", "Fork type: 'overlay' (copy-on-write) or 'full' (copies all files)")
	forkCmd.Flags().BoolVar(&forkAllowPartial, "allow-partial", false, "Continue full fork even if some files cannot be read (skips unreadable files)")
	forkCmd.Flags().StringVar(&forkConfigDir, "config-dir", ".latentfs", "Config directory relative to source folder (contains config.yaml with gitignore/includes/excludes)")
	rootCmd.AddCommand(forkCmd)
}

func runFork(cmd *cobra.Command, args []string) error {
	var source string
	target := forkMount // Use --mount flag for target

	// Validate fork type
	if forkType != "overlay" && forkType != "full" {
		return fmt.Errorf("invalid --type value '%s': must be 'overlay' or 'full'", forkType)
	}
	isFullFork := forkType == "full"

	if sourceEmpty {
		// With --source-empty, no source is needed
		source = ""

		// full fork with --source-empty doesn't make sense
		if isFullFork {
			return fmt.Errorf("--type=full cannot be used with --source-empty (no source to copy)")
		}
	} else {
		// Without --source-empty, source is required
		if len(args) < 1 {
			return cmd.Help()
		}
		source = args[0]
	}

	// Must have either --mount or --data-file
	if target == "" && forkDataFile == "" {
		fmt.Println("Error: must specify either --mount (-m) or --data-file")
		fmt.Println()
		return cmd.Help()
	}

	// Resolve target path if mounting
	var absTarget string
	if target != "" {
		var err error
		absTarget, err = filepath.Abs(target)
		if err != nil {
			return fmt.Errorf("failed to resolve target path: %w", err)
		}

		// Check if target is inside a mount - mounting inside a mount is not supported
		if err := checkTargetNotInMount(absTarget); err != nil {
			return err
		}
	}

	var absSource string
	if source != "" {
		var err error
		absSource, err = filepath.Abs(source)
		if err != nil {
			return fmt.Errorf("failed to resolve source path: %w", err)
		}

		// Verify source exists and is a directory
		info, err := os.Stat(absSource)
		if err != nil {
			return fmt.Errorf("source folder not found: %s", absSource)
		}
		if !info.IsDir() {
			return fmt.Errorf("source is not a directory: %s", absSource)
		}

		// Check if source is inside a mount - soft-fork is not supported for mounted paths
		if !isFullFork {
			if err := checkSourceNotInMount(absSource); err != nil {
				return err
			}
		}
	}

	// Check target doesn't exist or is an empty directory (only when mounting)
	if absTarget != "" {
		if info, err := os.Lstat(absTarget); err == nil {
			// Target exists - check if it's an empty directory
			if !info.IsDir() {
				return fmt.Errorf("target exists and is not a directory: %s", absTarget)
			}
			entries, err := os.ReadDir(absTarget)
			if err != nil {
				return fmt.Errorf("failed to read target directory: %w", err)
			}
			if len(entries) > 0 {
				return fmt.Errorf("target directory is not empty: %s", absTarget)
			}
			// Empty directory - remove it so symlink can be created
			if err := os.Remove(absTarget); err != nil {
				return fmt.Errorf("failed to remove empty target directory: %w", err)
			}
		}
	}

	// Determine data file path
	dataFilePath := forkDataFile
	if dataFilePath == "" {
		// Derive from mount target (--mount must be specified if --data-file is not)
		dataFilePath = absTarget + ".latentfs"
		fmt.Printf("No data file specified, using default: %s\n", dataFilePath)
	} else {
		var err error
		dataFilePath, err = filepath.Abs(dataFilePath)
		if err != nil {
			return fmt.Errorf("failed to resolve data file path: %w", err)
		}
	}

	// Ensure .latentfs extension
	if !strings.HasSuffix(dataFilePath, ".latentfs") {
		dataFilePath += ".latentfs"
	}

	// Check data file doesn't exist
	if _, err := os.Stat(dataFilePath); err == nil {
		return fmt.Errorf("failed to create fork: data file already exists: %s", dataFilePath)
	}

	// Check if data file location is inside a mount - causes NFS deadlock when daemon opens it
	if err := checkDataFileNotInMount(dataFilePath); err != nil {
		return err
	}

	// Create the data file
	df, err := storage.Create(dataFilePath)
	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}

	// For full fork, copy all files from source into the data file
	if isFullFork && absSource != "" {
		fmt.Printf("Performing full fork: copying files from %s\n", absSource)

		// Load config from config-dir if specified (for filtering)
		var filter storage.FileFilter
		if forkConfigDir != "" {
			cfg, cfgErr := daemon.LoadProjectConfigFromDir(absSource, forkConfigDir)
			if cfgErr != nil {
				log.Printf("Warning: failed to load config from %s/%s: %v", absSource, forkConfigDir, cfgErr)
			} else if cfg != nil {
				filter = daemon.BuildFileFilterFromConfig(absSource, cfg)
				fmt.Printf("Using config from %s/%s/config.yaml (gitignore=%v, includes=%v, excludes=%v)\n",
					absSource, forkConfigDir, cfg.GitignoreEnabled(), cfg.Includes, cfg.Excludes)
			}
		}

		result, err := copyFilesForHardFork(df, absSource, forkAllowPartial, filter)
		if err != nil {
			df.Close()
			os.Remove(dataFilePath)
			if !forkAllowPartial {
				return fmt.Errorf("failed to copy files for full fork: %w\n\nUse --allow-partial to skip unreadable files and continue", err)
			}
			return fmt.Errorf("failed to copy files for full fork: %w", err)
		}
		// Report skipped files if any
		if len(result.SkippedFiles) > 0 {
			fmt.Printf("\nWarning: %d file(s) skipped due to read errors:\n", len(result.SkippedFiles))
			for _, f := range result.SkippedFiles {
				fmt.Printf("  - %s\n", f)
			}
			fmt.Println()
		}
	}

	// Store source folder in data file (for both overlay and full forks)
	// This records the origin for history/tracking purposes
	if absSource != "" {
		if err := df.SetSourceFolder(absSource); err != nil {
			df.Close()
			os.Remove(dataFilePath)
			return fmt.Errorf("failed to store source folder: %w", err)
		}
	}

	// Store fork type in data file
	// - Soft-fork: daemon uses source folder for copy-on-write
	// - Hard-fork: files already copied, source is just metadata
	if absSource != "" {
		forkType := storage.ForkTypeSoft
		if isFullFork {
			forkType = storage.ForkTypeHard
		}
		if err := df.SetForkType(forkType); err != nil {
			df.Close()
			os.Remove(dataFilePath)
			return fmt.Errorf("failed to store fork type: %w", err)
		}
	}

	df.Close()

	fmt.Printf("Created data file: %s\n", dataFilePath)

	// If no mount target specified, we're done (data file only mode)
	if absTarget == "" {
		fmt.Println("Fork created without mounting. Use 'latentfs mount' to mount later.")
		return nil
	}

	// Start daemon if not running
	if err := StartDaemonIfNeeded(true); err != nil {
		// Clean up data file
		os.Remove(dataFilePath)
		return fmt.Errorf("failed to start daemon: %w", err)
	}

	// Connect to daemon
	client, err := daemon.Connect()
	if err != nil {
		os.Remove(dataFilePath)
		return fmt.Errorf("failed to connect to daemon: %w", err)
	}
	defer client.Close()

	// Send mount request (daemon reads source from data file)
	resp, err := client.Mount(dataFilePath, absTarget)
	if err != nil {
		os.Remove(dataFilePath)
		return fmt.Errorf("fork request failed: %w", err)
	}

	if !resp.Success {
		os.Remove(dataFilePath)
		return fmt.Errorf("%s", resp.Error)
	}

	fmt.Println(resp.Message)
	return nil
}

// checkSourceNotInMount checks if the source path is inside any latentfs mount.
// Returns an error if the source is inside a mount, as overlay fork is not supported for mounted paths.
func checkSourceNotInMount(sourcePath string) error {
	// Try to connect to daemon - if not running, no mounts exist
	client, err := daemon.Connect()
	if err != nil {
		return nil // Daemon not running, no mounts to check
	}
	defer client.Close()

	// Use IPC to check if path is inside any mount
	isInMount, mountPath, err := client.CheckPathInMount(sourcePath)
	if err != nil {
		return nil // Can't check, allow to proceed
	}

	if isInMount {
		return fmt.Errorf("overlay fork of a mount path is not supported yet\n\nThe source folder '%s' is inside a latentfs mount at '%s'.\nConsider using --type=full instead to create a full snapshot.", sourcePath, mountPath)
	}

	return nil
}

// fullForkResult contains the result of a full fork copy operation
type fullForkResult struct {
	SkippedFiles []string // Files that couldn't be read (when allowPartial is true)
}

// copyFilesForHardFork copies all files from source directory into the data file
// Uses BulkCopier for optimized performance with batched transactions and prepared statements
// If allowPartial is true, continues on read errors and collects skipped files
// If filter is non-nil, only files passing the filter will be copied
func copyFilesForHardFork(dataFile *storage.DataFile, sourcePath string, allowPartial bool, filter storage.FileFilter) (*fullForkResult, error) {
	// Configure bulk copier
	config := storage.DefaultBulkCopyConfig()
	config.AllowPartial = allowPartial
	config.SkipHidden = true
	config.Filter = filter

	// Use BulkCopier for optimized performance
	copier := storage.NewBulkCopier(dataFile, config)
	bulkResult, err := copier.CopyFromDirectory(sourcePath)

	// Convert BulkCopyResult to fullForkResult
	result := &fullForkResult{
		SkippedFiles: bulkResult.SkippedFiles,
	}

	return result, err
}


// createDirInDataFile creates a directory in the data file with default permissions (0755)
func createDirInDataFile(dataFile *storage.DataFile, dirPath string) error {
	return createDirInDataFileWithMode(dataFile, dirPath, os.FileMode(0755))
}

// createDirInDataFileWithMode creates a directory in the data file with specified permissions
func createDirInDataFileWithMode(dataFile *storage.DataFile, dirPath string, mode os.FileMode) error {
	// Check if already exists
	if _, err := dataFile.ResolvePath(dirPath); err == nil {
		return nil
	}

	// Ensure parent exists (use default mode for auto-created parents)
	parentPath := filepath.Dir(dirPath)
	if parentPath != "/" {
		if err := createDirInDataFile(dataFile, parentPath); err != nil {
			return err
		}
	}

	// Get parent inode
	parentIno, err := dataFile.ResolvePath(parentPath)
	if err != nil {
		return fmt.Errorf("parent not found: %s", parentPath)
	}

	// Create directory inode with the specified mode (preserving source permissions)
	dirMode := uint32(storage.ModeDir | (int(mode) & 0777))
	ino, err := dataFile.CreateInode(dirMode)
	if err != nil {
		return err
	}

	// Create dentry
	name := filepath.Base(dirPath)
	if err := dataFile.CreateDentry(parentIno, name, ino); err != nil {
		dataFile.DeleteInode(ino)
		return err
	}

	return nil
}

// copyFileToDataFile copies a file from the filesystem into the data file
func copyFileToDataFile(dataFile *storage.DataFile, destPath, srcPath string, info os.FileInfo) error {
	// Ensure parent directory exists
	parentPath := filepath.Dir(destPath)
	if parentPath != "/" {
		if err := createDirInDataFile(dataFile, parentPath); err != nil {
			return err
		}
	}

	// Get parent inode
	parentIno, err := dataFile.ResolvePath(parentPath)
	if err != nil {
		return fmt.Errorf("parent not found: %s", parentPath)
	}

	// Handle symlinks
	if info.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(srcPath)
		if err != nil {
			return err
		}
		return createSymlinkInDataFile(dataFile, parentIno, filepath.Base(destPath), target)
	}

	// Read source file content FIRST (before creating inode)
	// This ensures we don't leave orphaned inodes if reading fails
	content, err := os.ReadFile(srcPath)
	if err != nil {
		return err
	}

	// Create file inode
	fileMode := uint32(storage.ModeFile | (int(info.Mode()) & 0777))
	ino, err := dataFile.CreateInode(fileMode)
	if err != nil {
		return err
	}

	// Create dentry
	name := filepath.Base(destPath)
	if err := dataFile.CreateDentry(parentIno, name, ino); err != nil {
		dataFile.DeleteInode(ino)
		return err
	}

	// Write content to data file
	if len(content) > 0 {
		if err := dataFile.WriteContent(ino, 0, content); err != nil {
			return err
		}
	}

	return nil
}

// createSymlinkInDataFile creates a symlink in the data file
func createSymlinkInDataFile(dataFile *storage.DataFile, parentIno int64, name, target string) error {
	symlinkMode := uint32(storage.ModeSymlink | 0777)
	ino, err := dataFile.CreateInode(symlinkMode)
	if err != nil {
		return err
	}

	if err := dataFile.CreateDentry(parentIno, name, ino); err != nil {
		dataFile.DeleteInode(ino)
		return err
	}

	if err := dataFile.CreateSymlink(ino, target); err != nil {
		return err
	}

	return nil
}
