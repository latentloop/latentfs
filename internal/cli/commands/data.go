package commands

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"latentfs/internal/daemon"
)

// ErrDaemonNotRunning is returned when a command requires the daemon but it's not running
var ErrDaemonNotRunning = fmt.Errorf("daemon not running. Start it with: latentfs daemon start")

// requireDaemon returns a connected client or an error if daemon is not running
func requireDaemon() (*daemon.Client, error) {
	if !daemon.IsDaemonRunning() {
		return nil, ErrDaemonNotRunning
	}
	client, err := daemon.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to daemon: %w", err)
	}
	return client, nil
}

var dataCmd = &cobra.Command{
	Use:   "data",
	Short: "Manage data snapshots",
	Long: `Manage snapshots for a latentfs data file.

Subcommands:
  save      Create a snapshot of the current filesystem state
  restore   Restore files from a snapshot
  ls-saves  List all snapshots

Examples:
  # Create a snapshot
  latentfs data save -m "Initial version" -t v1

  # List snapshots
  latentfs data ls-saves

  # Restore from a snapshot
  latentfs data restore v1

  # Restore specific files
  latentfs data restore v1 -d ~/data/project.latentfs -- file1.txt file2.txt`,
}

// save command
var saveCmd = &cobra.Command{
	Use:   "save",
	Short: "Create a snapshot of the current filesystem state",
	Long: `Create a snapshot of the current filesystem state.

A snapshot captures files and directories at a point in time.
Snapshots can be restored later using 'data restore'.

Snapshot behavior depends on fork type:
  - Overlay fork: saves only modified files (overlay data) by default
  - Full fork: saves all files (complete snapshot)
  - Use --full flag to include source folder files for overlay forks

The target can be specified as:
  - A .latentfs data file path (-d/--data-file)
  - Auto-detected from current working directory

Examples:
  # Save with a message (overlay only for overlay forks)
  latentfs data save -m "Initial version"

  # Save with a tag
  latentfs data save -m "Release" -t v1.0

  # Full snapshot including source folder
  latentfs data save -m "Complete backup" --full

  # Save specific data file
  latentfs data save -d ~/data/project.latentfs -m "Backup"`,
	RunE: runSave,
}

// restore command
var restoreCmd = &cobra.Command{
	Use:   "restore <snapshot> [flags] [-- <path>...]",
	Short: "Restore files from a snapshot",
	Long: `Restore files from a snapshot.

The snapshot can be specified by:
  - Snapshot ID (full or prefix)
  - Tag name

If no paths are specified, all files from the snapshot are restored.
If paths are specified (after --), only those files are restored.

This works similar to 'git restore' or 'git checkout':
  - Restores files to their state in the snapshot
  - Does not change snapshots, just the current filesystem state

IMPORTANT: All flags must come BEFORE the -- separator.
Anything after -- is interpreted as file paths, not flags.

Examples:
  # Restore all files from a tagged snapshot
  latentfs data restore v1

  # Restore from a specific data file (flags before --)
  latentfs data restore v1 -d ~/data/project.latentfs

  # Restore without confirmation prompt
  latentfs data restore v1 -y

  # Restore specific files from snapshot
  latentfs data restore abc123 -- src/main.go

  # Restore multiple files with flags
  latentfs data restore v1 -d ~/data/project.latentfs -y -- file1.txt file2.txt`,
	Args: cobra.MinimumNArgs(1),
	RunE: runRestore,
}

// ls-saves command
var lsSavesCmd = &cobra.Command{
	Use:   "ls-saves",
	Short: "List all snapshots",
	Long: `List all snapshots in a data file.

Shows snapshot ID, tag, message, and creation time.

Examples:
  # List snapshots (auto-detect data file)
  latentfs data ls-saves

  # List snapshots from specific data file
  latentfs data ls-saves -d ~/data/project.latentfs`,
	RunE: runLsSaves,
}

// remove command
var removeCmd = &cobra.Command{
	Use:   "remove <snapshot>",
	Short: "Remove a snapshot",
	Long: `Remove a snapshot from a data file.

The snapshot can be specified by:
  - Snapshot ID (full or prefix)
  - Tag name

This permanently deletes the snapshot metadata and references.
Shared content blocks are preserved for other snapshots.

Examples:
  latentfs data remove v1.0
  latentfs data remove v1.0 -y
  latentfs data remove abc123 -d ~/project.latentfs`,
	Args: cobra.ExactArgs(1),
	RunE: runRemove,
}

// gc command
var gcCmd = &cobra.Command{
	Use:   "gc",
	Short: "Run garbage collection to clean orphaned data",
	Long: `Run garbage collection to remove orphaned data and free storage.

This cleans:
  - Content blocks not referenced by any snapshot
  - Data from deprecated write epochs (MVCC cleanup)

Output format:
  Storage: 2.3 MB total (5 snapshots, 142 content blocks)
  Reclaimable: 512.0 KB (12 orphaned blocks, 2 deprecated epochs)

Examples:
  latentfs data gc
  latentfs data gc --stats      # Show stats without cleaning
  latentfs data gc --vacuum     # Also run SQLite VACUUM
  latentfs data gc -d my.latentfs`,
	RunE: runGC,
}


// Flag variables
var (
	// save flags
	saveMessage      string
	saveTag          string
	saveDataFile     string
	saveAllowPartial bool
	saveFullSnapshot bool

	// restore flags
	restoreSkipConfirm bool
	restoreDataFile    string

	// ls flags
	lsDataFile string

	// remove flags
	removeSkipConfirm bool
	removeDataFile    string

	// gc flags
	gcDataFile  string
	gcStatsOnly bool
	gcVacuum    bool
)

func init() {
	// Register data command
	rootCmd.AddCommand(dataCmd)

	// save subcommand
	saveCmd.Flags().StringVarP(&saveMessage, "message", "m", "", "Snapshot message")
	saveCmd.Flags().StringVarP(&saveTag, "tag", "t", "", "Snapshot tag (must be unique)")
	saveCmd.Flags().StringVarP(&saveDataFile, "data-file", "d", "", "Path to .latentfs data file")
	saveCmd.Flags().BoolVar(&saveAllowPartial, "allow-partial", false, "Continue snapshot even if some source files cannot be read (skips unreadable files)")
	saveCmd.Flags().BoolVar(&saveFullSnapshot, "full", false, "Create full snapshot including source folder (for overlay forks)")
	dataCmd.AddCommand(saveCmd)

	// restore subcommand
	restoreCmd.Flags().BoolVarP(&restoreSkipConfirm, "yes", "y", false, "Skip confirmation prompt for full snapshot restore")
	restoreCmd.Flags().StringVarP(&restoreDataFile, "data-file", "d", "", "Path to .latentfs data file")
	dataCmd.AddCommand(restoreCmd)

	// ls-saves subcommand
	lsSavesCmd.Flags().StringVarP(&lsDataFile, "data-file", "d", "", "Path to .latentfs data file")
	dataCmd.AddCommand(lsSavesCmd)

	// remove subcommand
	removeCmd.Flags().BoolVarP(&removeSkipConfirm, "yes", "y", false, "Skip confirmation prompt")
	removeCmd.Flags().StringVarP(&removeDataFile, "data-file", "d", "", "Path to .latentfs data file")
	dataCmd.AddCommand(removeCmd)

	// gc subcommand
	gcCmd.Flags().StringVarP(&gcDataFile, "data-file", "d", "", "Path to .latentfs data file")
	gcCmd.Flags().BoolVar(&gcStatsOnly, "stats", false, "Show storage stats without cleaning")
	gcCmd.Flags().BoolVar(&gcVacuum, "vacuum", false, "Run SQLite VACUUM after cleanup")
	dataCmd.AddCommand(gcCmd)
}

func runSave(cmd *cobra.Command, args []string) error {
	// Truncate message if too long
	const maxMessageLength = 4096
	if len(saveMessage) > maxMessageLength {
		fmt.Printf("Warning: message truncated to %d characters (was %d)\n", maxMessageLength, len(saveMessage))
		saveMessage = saveMessage[:maxMessageLength]
	}

	// Resolve data file path
	dataFilePath, err := resolveDataFilePath(saveDataFile)
	if err != nil {
		return err
	}

	// Connect to daemon (required for all data operations)
	client, err := requireDaemon()
	if err != nil {
		return err
	}
	defer client.Close()

	// Create snapshot via IPC
	resp, err := client.Save(dataFilePath, saveMessage, saveTag, saveAllowPartial, saveFullSnapshot)
	if err != nil {
		return err
	}

	// Show source folder for soft-forks
	if resp.SourceFolder != "" {
		fmt.Printf("Creating snapshot with source folder: %s\n", resp.SourceFolder)
	}
	fmt.Printf("Created snapshot %s (%s)\n", resp.SnapshotID, resp.SnapshotType)
	if saveTag != "" {
		fmt.Printf("  Tag: %s\n", saveTag)
	}
	if saveMessage != "" {
		fmt.Printf("  Message: %s\n", saveMessage)
	}

	// Report skipped files if any
	if len(resp.SkippedFiles) > 0 {
		fmt.Printf("\nWarning: %d file(s) skipped due to read errors:\n", len(resp.SkippedFiles))
		for _, f := range resp.SkippedFiles {
			fmt.Printf("  - %s\n", f)
		}
		fmt.Println()
	}

	return nil
}

func runRestore(cmd *cobra.Command, args []string) error {
	snapshotRef := args[0]

	// Get paths after -- if any
	var paths []string
	if cmd.ArgsLenAtDash() >= 0 {
		paths = args[cmd.ArgsLenAtDash():]
	} else if len(args) > 1 {
		paths = args[1:]
	}

	// Resolve data file path
	dataFilePath, err := resolveDataFilePath(restoreDataFile)
	if err != nil {
		return err
	}

	// If restoring whole snapshot (no specific paths), ask for confirmation BEFORE IPC
	if len(paths) == 0 && !restoreSkipConfirm {
		fmt.Printf("Restoring entire snapshot '%s' will replace all current files.\n", snapshotRef)
		fmt.Print("Continue? [y/N] ")

		reader := bufio.NewReader(os.Stdin)
		response, _ := reader.ReadString('\n')
		response = strings.TrimSpace(strings.ToLower(response))

		if response != "y" && response != "yes" {
			fmt.Println("Restore cancelled")
			return nil
		}
	}

	// Connect to daemon (required for all data operations)
	client, err := requireDaemon()
	if err != nil {
		return err
	}
	defer client.Close()

	// Restore via IPC
	resp, err := client.Restore(dataFilePath, snapshotRef, "", paths)
	if err != nil {
		return err
	}

	if len(resp.RestoredPaths) == 0 {
		fmt.Println("No files restored")
		if len(paths) > 0 {
			fmt.Println("(specified paths not found in snapshot)")
		}
		return nil
	}

	fmt.Printf("Restored from snapshot %s\n\n", resp.SnapshotID[:8])
	printRestoredPaths(resp.RestoredPaths)
	fmt.Printf("\n%d file(s) restored\n", len(resp.RestoredPaths))

	return nil
}

// printRestoredPaths prints the list of restored paths, truncating if too many
func printRestoredPaths(paths []string) {
	if len(paths) <= 10 {
		for _, p := range paths {
			fmt.Printf("  R %s\n", p)
		}
	} else {
		for i := 0; i < 5; i++ {
			fmt.Printf("  R %s\n", paths[i])
		}
		fmt.Printf("  ... and %d more files\n", len(paths)-5)
	}
}

func runLsSaves(cmd *cobra.Command, args []string) error {
	// Resolve data file path
	dataFilePath, err := resolveDataFilePath(lsDataFile)
	if err != nil {
		return err
	}

	// Connect to daemon (required for all data operations)
	client, err := requireDaemon()
	if err != nil {
		return err
	}
	defer client.Close()

	// List snapshots via IPC
	snapshots, err := client.ListSnapshots(dataFilePath)
	if err != nil {
		return err
	}

	if len(snapshots) == 0 {
		fmt.Println("No snapshots")
		return nil
	}

	printSnapshotList(snapshots)
	return nil
}

// printSnapshotList prints a list of snapshots in git-log style
func printSnapshotList(snapshots []daemon.SnapshotInfo) {
	// ANSI color codes
	yellow := "\033[33m"
	cyan := "\033[36m"
	reset := "\033[0m"

	for _, s := range snapshots {
		// Line 1: snapshot <id> (tag: <tag>)
		fmt.Printf("%ssnapshot %s%s", yellow, s.ID[:8], reset)
		if s.Tag != "" {
			fmt.Printf(" (%stag: %s%s)", cyan, s.Tag, reset)
		}
		fmt.Println()

		// Line 2: Date
		t := time.Unix(s.CreatedAt, 0)
		fmt.Printf("Date:   %s\n", t.Format("Mon Jan 2 15:04:05 2006"))

		// Line 3+: Message (indented, each line prefixed with 4 spaces)
		if s.Message != "" {
			fmt.Println()
			lines := strings.Split(s.Message, "\n")
			for _, line := range lines {
				fmt.Printf("    %s\n", line)
			}
		}
		fmt.Println()
	}
}

func runRemove(cmd *cobra.Command, args []string) error {
	snapshotRef := args[0]

	// Resolve data file path
	dataFilePath, err := resolveDataFilePath(removeDataFile)
	if err != nil {
		return err
	}

	// Confirmation prompt (if not skipped)
	if !removeSkipConfirm {
		fmt.Printf("This will permanently delete snapshot '%s'.\n", snapshotRef)
		fmt.Print("Continue? [y/N] ")

		reader := bufio.NewReader(os.Stdin)
		response, _ := reader.ReadString('\n')
		response = strings.TrimSpace(strings.ToLower(response))

		if response != "y" && response != "yes" {
			fmt.Println("Remove cancelled")
			return nil
		}
	}

	// Connect to daemon (required for all data operations)
	client, err := requireDaemon()
	if err != nil {
		return err
	}
	defer client.Close()

	// Delete snapshot via IPC
	if err := client.DeleteSnapshot(dataFilePath, snapshotRef, ""); err != nil {
		return err
	}

	fmt.Printf("Removed snapshot %s\n", snapshotRef)
	return nil
}

func runGC(cmd *cobra.Command, args []string) error {
	// Resolve data file path
	dataFilePath, err := resolveDataFilePath(gcDataFile)
	if err != nil {
		return err
	}

	// Connect to daemon (required for all data operations)
	client, err := requireDaemon()
	if err != nil {
		return err
	}
	defer client.Close()

	// If stats-only, just show stats via IPC
	if gcStatsOnly {
		resp, err := client.GC(dataFilePath, false, true)
		if err != nil {
			return err
		}
		if resp.Stats == nil {
			return fmt.Errorf("no stats returned")
		}

		// Line 1: Total storage with breakdown
		fmt.Printf("Storage: %s total (%d snapshots, %d content blocks)\n",
			formatBytes(resp.Stats.TotalContentBlocksBytes),
			resp.Stats.SnapshotCount,
			resp.Stats.TotalContentBlocks)

		// Line 2: Reclaimable data
		if resp.Stats.OrphanedBlocksBytes > 0 || resp.Stats.DeprecatedEpochCount > 0 {
			fmt.Printf("Reclaimable: %s (%d orphaned blocks, %d deprecated epochs)\n",
				formatBytes(resp.Stats.OrphanedBlocksBytes),
				resp.Stats.OrphanedContentBlocks,
				resp.Stats.DeprecatedEpochCount)
		} else {
			fmt.Println("Reclaimable: none")
		}
		return nil
	}

	// Run GC via IPC
	fmt.Println("Running garbage collection...")
	resp, err := client.GC(dataFilePath, gcVacuum, false)
	if err != nil {
		return err
	}

	if resp.GCResult != nil {
		fmt.Printf("Cleaned: %s freed (%d blocks, %d epochs)\n",
			formatBytes(resp.GCResult.OrphanedBlocksBytes),
			resp.GCResult.OrphanedContentBlocks,
			resp.GCResult.DeprecatedEpochs)
	}

	if gcVacuum {
		fmt.Println("VACUUM completed.")
	}

	return nil
}

// formatBytes formats bytes in human-readable form
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// resolveDataFilePath resolves the data file path from explicit path or auto-detection
func resolveDataFilePath(dataFile string) (string, error) {
	// If data file is specified directly, use it
	if dataFile != "" {
		absPath, err := filepath.Abs(dataFile)
		if err != nil {
			return "", err
		}
		if !strings.HasSuffix(absPath, ".latentfs") {
			absPath += ".latentfs"
		}
		if _, err := os.Stat(absPath); err != nil {
			return "", fmt.Errorf("data file not found: %s", absPath)
		}
		return absPath, nil
	}

	// Try to auto-detect from current working directory
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	dataFilePath, err := resolveFromForkPath(cwd)
	if err != nil {
		return "", fmt.Errorf("could not auto-detect data file: %w\nUse --data-file to specify the target", err)
	}

	fmt.Printf("Auto-detected data file: %s\n", dataFilePath)
	return dataFilePath, nil
}

// getSymlinkTargetFromDaemon queries the daemon for the symlink target of a mounted data file
// Returns empty string if daemon is not running or data file is not mounted
// This handles the "daemon not running" case gracefully - snapshots will still work,
// they just won't be able to exclude the fork target from the source folder
func getSymlinkTargetFromDaemon(dataFilePath string) string {
	// Check if daemon is running
	if !daemon.IsDaemonRunning() {
		return ""
	}

	// Connect to daemon
	client, err := daemon.Connect()
	if err != nil {
		return ""
	}
	defer client.Close()

	// Get mount info for this data file
	mountInfo, err := client.GetMountInfo(dataFilePath)
	if err != nil || mountInfo == nil {
		return ""
	}

	return mountInfo.SymlinkTarget
}

// invalidateDaemonCache tells the daemon to invalidate cache for a data file
// This ensures NFS mount reflects the new state after restore
// Silently does nothing if daemon is not running
func invalidateDaemonCache(dataFilePath string) {
	// Check if daemon is running
	if !daemon.IsDaemonRunning() {
		return
	}

	// Connect to daemon
	client, err := daemon.Connect()
	if err != nil {
		return
	}
	defer client.Close()

	// Invalidate cache
	if err := client.InvalidateCache(dataFilePath); err != nil {
		fmt.Printf("Warning: could not invalidate cache: %v\n", err)
	} else {
		fmt.Println("Cache invalidated")
	}
}
