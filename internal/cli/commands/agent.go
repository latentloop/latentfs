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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"latentfs/internal/daemon"
	"latentfs/internal/storage"
)

// TODO: Temporarily disabled - agent subcommand
// var agentCmd = &cobra.Command{
// 	Use:   "agent",
// 	Short: "Agent integration commands",
// 	Long:  `Commands for AI agent integration (e.g., Claude Code hooks).`,
// }

var agentNotifyCmd = &cobra.Command{
	Use:   "notify <event_type>",
	Short: "Send agent hook notification and run autosave if enabled",
	Long: `Sends an agent event notification. If autosave is enabled in the project config
and the event type is PRE_TOOL_USE or POST_TOOL_USE, creates a snapshot of the project directory.

Reads JSON payload from stdin. Project directory is read from CLAUDE_PROJECT_DIR env var.`,
	Args: cobra.ExactArgs(1),
	RunE: runAgentNotify,
}

var agentProtectCmd = &cobra.Command{
	Use:   "protect <data_file>",
	Short: "Protect a data file from deletion",
	Long: `Requests the daemon to keep a data file open, preventing it from being deleted.
This is useful for autosave data files that should persist even during rm -rf operations.`,
	Args: cobra.ExactArgs(1),
	RunE: runAgentProtect,
}

var agentReleaseCmd = &cobra.Command{
	Use:   "release <data_file>",
	Short: "Release protection on a data file",
	Long:  `Releases the daemon's protection on a data file, allowing it to be deleted.`,
	Args:  cobra.ExactArgs(1),
	RunE:  runAgentRelease,
}

var agentLsProtectCmd = &cobra.Command{
	Use:   "ls-protect",
	Short: "List protected data files",
	Long:  `Lists all data files currently protected by the daemon.`,
	RunE:  runAgentLsProtect,
}

// TODO: Temporarily disabled - config-template moved to 'latentfs init'
// var agentConfigTemplateCmd = &cobra.Command{
// 	Use:   "config-template",
// 	Short: "Generate config template for agent integration",
// 	Long: `Generates configuration template files for agent integration.
//
// Use --claude to copy Claude Code config files to a directory.
// All files from the claude artifact directory will be copied.`,
// 	RunE: runAgentConfigTemplate,
// }

// var configTemplateClaudeDir string

func init() {
	// TODO: Temporarily disabled - agent subcommand
	// agentConfigTemplateCmd.Flags().StringVar(&configTemplateClaudeDir, "claude", "", "Directory to write Claude Code config files")
	// agentCmd.AddCommand(agentNotifyCmd)
	// agentCmd.AddCommand(agentProtectCmd)
	// agentCmd.AddCommand(agentReleaseCmd)
	// agentCmd.AddCommand(agentLsProtectCmd)
	// agentCmd.AddCommand(agentConfigTemplateCmd)
	// rootCmd.AddCommand(agentCmd)
}

// TODO: Temporarily disabled - config-template moved to 'latentfs init'
// func runAgentConfigTemplate(cmd *cobra.Command, args []string) error {
// 	if configTemplateClaudeDir == "" {
// 		return fmt.Errorf("no template type specified (use --claude <path>)")
// 	}
//
// 	return writeTemplateDir(configTemplateClaudeDir, artifacts.AgentClaudeDir, "agent/claude")
// }

// TODO: Temporarily disabled - writeTemplateDir no longer needed
// func writeTemplateDir(destDir string, embedFS embed.FS, subdir string) error {
// 	// Expand ~ in path
// 	outputDir := destDir
// 	if strings.HasPrefix(outputDir, "~/") {
// 		home, err := os.UserHomeDir()
// 		if err != nil {
// 			return fmt.Errorf("failed to get home directory: %w", err)
// 		}
// 		outputDir = filepath.Join(home, outputDir[2:])
// 	}
//
// 	// Create destination directory if needed (but not parent directories)
// 	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
// 		if err := os.Mkdir(outputDir, 0755); err != nil {
// 			return fmt.Errorf("failed to create directory %s: %w", outputDir, err)
// 		}
// 	}
//
// 	// Walk and copy all files from the embedded FS
// 	return fs.WalkDir(embedFS, subdir, func(path string, d fs.DirEntry, err error) error {
// 		if err != nil {
// 			return err
// 		}
//
// 		// Get relative path from subdir
// 		relPath, err := filepath.Rel(subdir, path)
// 		if err != nil {
// 			return err
// 		}
//
// 		// Skip the root directory itself
// 		if relPath == "." {
// 			return nil
// 		}
//
// 		destPath := filepath.Join(outputDir, relPath)
//
// 		if d.IsDir() {
// 			if err := os.Mkdir(destPath, 0755); err != nil && !os.IsExist(err) {
// 				return fmt.Errorf("failed to create directory %s: %w", destPath, err)
// 			}
// 			return nil
// 		}
//
// 		// Read file content from embedded FS
// 		content, err := embedFS.ReadFile(path)
// 		if err != nil {
// 			return fmt.Errorf("failed to read embedded file %s: %w", path, err)
// 		}
//
// 		// Write file to destination
// 		if err := os.WriteFile(destPath, content, 0644); err != nil {
// 			return fmt.Errorf("failed to write file %s: %w", destPath, err)
// 		}
//
// 		fmt.Printf("Created %s\n", destPath)
// 		return nil
// 	})
// }

// hookPayload represents the JSON payload from Claude Code hooks
type hookPayload struct {
	SessionID string `json:"session_id"`
}

func runAgentNotify(cmd *cobra.Command, args []string) error {
	eventType := args[0]

	// Read JSON payload from stdin
	var payload json.RawMessage
	var sessionID string
	if stat, _ := os.Stdin.Stat(); (stat.Mode() & os.ModeCharDevice) == 0 {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("failed to read stdin: %w", err)
		}
		if len(data) > 0 {
			payload = json.RawMessage(data)
			// Extract session_id from payload
			var hp hookPayload
			if json.Unmarshal(data, &hp) == nil && hp.SessionID != "" {
				sessionID = hp.SessionID
			}
		}
	}

	// Get project directory from CLAUDE_PROJECT_DIR (set by Claude Code)
	projectDir := os.Getenv("CLAUDE_PROJECT_DIR")

	// Notify daemon (for logging purposes)
	if daemon.IsDaemonRunning() {
		client, err := daemon.Connect()
		if err == nil {
			defer client.Close()
			client.AgentNotify(eventType, payload, projectDir)
		}
	}

	// Run autosave in CLI (not daemon) if enabled
	if projectDir != "" && (eventType == "PRE_TOOL_USE" || eventType == "POST_TOOL_USE") {
		cfg, err := daemon.LoadProjectConfig(projectDir)
		if err == nil && cfg != nil && cfg.Autosave {
			runAutosave(projectDir, cfg, sessionID)
		}
	}

	return nil
}

func runAgentProtect(cmd *cobra.Command, args []string) error {
	dataFile := args[0]

	// Resolve to absolute path
	absPath, err := filepath.Abs(dataFile)
	if err != nil {
		return fmt.Errorf("failed to resolve path: %w", err)
	}

	// Validate that this is a .latentfs datafile
	if !isLatentFSDataFile(absPath) {
		return fmt.Errorf("protect only works with .latentfs datafiles (path must contain .latentfs/)")
	}

	// Ensure daemon is running
	if !daemon.IsDaemonRunning() {
		return fmt.Errorf("daemon is not running (start with: latentfs daemon start)")
	}

	client, err := daemon.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to daemon: %w", err)
	}
	defer client.Close()

	resp, err := client.Protect(absPath)
	if err != nil {
		return fmt.Errorf("protect request failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("protect failed: %s", resp.Error)
	}

	fmt.Println(resp.Message)
	return nil
}

func runAgentRelease(cmd *cobra.Command, args []string) error {
	dataFile := args[0]

	// Resolve to absolute path
	absPath, err := filepath.Abs(dataFile)
	if err != nil {
		return fmt.Errorf("failed to resolve path: %w", err)
	}

	// Ensure daemon is running
	if !daemon.IsDaemonRunning() {
		return fmt.Errorf("daemon is not running (start with: latentfs daemon start)")
	}

	client, err := daemon.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to daemon: %w", err)
	}
	defer client.Close()

	resp, err := client.Release(absPath)
	if err != nil {
		return fmt.Errorf("release request failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("release failed: %s", resp.Error)
	}

	fmt.Println(resp.Message)
	return nil
}

func runAgentLsProtect(cmd *cobra.Command, args []string) error {
	// Ensure daemon is running
	if !daemon.IsDaemonRunning() {
		return fmt.Errorf("daemon is not running (start with: latentfs daemon start)")
	}

	client, err := daemon.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to daemon: %w", err)
	}
	defer client.Close()

	files, err := client.ListProtect()
	if err != nil {
		return fmt.Errorf("list protect failed: %w", err)
	}

	if len(files) == 0 {
		fmt.Println("No protected files")
		return nil
	}

	fmt.Printf("Protected files (%d):\n", len(files))
	for _, f := range files {
		t := time.Unix(f.CreatedAt, 0).Format("2006-01-02 15:04:05")
		fmt.Printf("  %s  %s\n", t, f.DataFile)
	}
	return nil
}

// autosaveMessagePrefix is the special prefix used to identify autosave snapshots
const autosaveMessagePrefix = "==Auto Save=="

// fileInfo holds information about a file for diff comparison
type fileInfo struct {
	path     string
	size     int64
	lines    int // -1 for binary files
	isBinary bool
	isDir    bool
}

// findLastAutosave finds the most recent autosave snapshot from the datafile.
// Returns the CreatedAt timestamp (Unix seconds) or 0 if no autosave found.
func findLastAutosave(df *storage.DataFile) int64 {
	snapshots, err := df.ListSnapshots()
	if err != nil || len(snapshots) == 0 {
		return 0
	}

	// Snapshots are ordered by created_at DESC, so first match is most recent
	for _, snap := range snapshots {
		if strings.HasPrefix(snap.Message, autosaveMessagePrefix) {
			return snap.CreatedAt.Unix()
		}
	}
	return 0
}

// calculateSizeChange computes the total size change between two file sets
func calculateSizeChange(prevFiles, currentFiles map[string]fileInfo) int64 {
	var totalChange int64

	// Added files: count their size
	for path, curr := range currentFiles {
		if _, exists := prevFiles[path]; !exists {
			totalChange += curr.size
		}
	}

	// Modified files: count absolute size difference
	for path, curr := range currentFiles {
		if prev, exists := prevFiles[path]; exists {
			diff := curr.size - prev.size
			if diff < 0 {
				diff = -diff
			}
			totalChange += diff
		}
	}

	// Deleted files: count their size (from previous)
	for path, prev := range prevFiles {
		if _, exists := currentFiles[path]; !exists {
			totalChange += prev.size
		}
	}

	return totalChange
}

// runAutosave performs an autosave snapshot for the given project directory.
// It applies save strategy thresholds to decide whether to actually save.
// Last autosave time is detected from the datafile by looking for snapshots
// with the autosaveMessagePrefix.
func runAutosave(projectDir string, cfg *daemon.ProjectConfig, sessionID string) {
	t0 := time.Now()

	dataFilePath := filepath.Join(projectDir, ".latentfs", cfg.DataFile)

	// Resolve to absolute path for protection
	absDataFilePath, err := filepath.Abs(dataFilePath)
	if err != nil {
		log.Printf("autosave: failed to resolve path %s: %v", dataFilePath, err)
		return
	}

	// Request daemon to protect the data file before opening
	// This keeps the file open in the daemon, preventing rm -rf from deleting it
	if daemon.IsDaemonRunning() {
		client, err := daemon.Connect()
		if err == nil {
			resp, err := client.Protect(absDataFilePath)
			if err != nil {
				log.Printf("autosave: protect request failed: %v", err)
			} else if !resp.Success {
				log.Printf("autosave: protect failed: %s", resp.Error)
			} else if resp.Success && !strings.HasPrefix(resp.Message, "already protected:") {
				// First-time protection - notify user
				log.Printf("autosave: data file protected from deletion: %s", absDataFilePath)
				log.Printf("autosave: the daemon will keep this file open to prevent rm -rf from deleting it")
				log.Printf("autosave: to release protection, run: latentfs agent release %s", absDataFilePath)
			}
			client.Close()
		}
	}

	// Open or create the data file with CLI context
	var df *storage.DataFile
	isNewFile := false
	if _, statErr := os.Stat(absDataFilePath); os.IsNotExist(statErr) {
		df, err = storage.CreateWithContext(absDataFilePath, storage.DBContextCLI)
		isNewFile = true
	} else {
		df, err = storage.OpenWithContext(absDataFilePath, storage.DBContextCLI)
	}
	if err != nil {
		log.Printf("autosave: failed to open/create %s: %v", absDataFilePath, err)
		return
	}
	defer df.Close()

	// Build file filter
	filter := buildFileFilter(projectDir, cfg.GitignoreEnabled(), cfg.Includes, cfg.Excludes)

	// Collect previous snapshot file info (for diff)
	var prevFiles map[string]fileInfo
	if !isNewFile {
		prevFiles = collectSnapshotFiles(df)
	}

	// Collect current file info
	currentFiles := collectDirFiles(projectDir, filter)

	// Calculate size change compared to last snapshot
	sizeChange := calculateSizeChange(prevFiles, currentFiles)

	// Get strategy and find last autosave time from datafile
	strategy := daemon.GetSaveStrategy(cfg.SaveStrategy)
	lastAutosaveTime := findLastAutosave(df)

	// Check if we should save based on strategy thresholds
	now := time.Now().Unix()
	timeSinceLastSave := now - lastAutosaveTime
	shouldSave := false
	reason := ""

	if isNewFile || lastAutosaveTime == 0 {
		// Always save for new datafile or no previous autosave
		shouldSave = true
		reason = "first autosave"
	} else if sizeChange >= strategy.SizeThreshold {
		// Size threshold exceeded
		shouldSave = true
		reason = fmt.Sprintf("size threshold (%d bytes >= %d)", sizeChange, strategy.SizeThreshold)
	} else if timeSinceLastSave >= int64(strategy.TimeInterval) && sizeChange > 0 {
		// Time interval exceeded and there are changes
		shouldSave = true
		reason = fmt.Sprintf("time interval (%ds >= %ds)", timeSinceLastSave, strategy.TimeInterval)
	}

	if !shouldSave {
		if cfg.LoggingEnabled() {
			log.Printf("autosave: skipped (strategy=%s, change=%d bytes, time=%ds)",
				cfg.SaveStrategy, sizeChange, timeSinceLastSave)
		}
		return
	}

	// Generate diff and build message
	message := buildAutosaveMessage(sessionID, prevFiles, currentFiles)

	// Create snapshot
	result, err := df.CreateSnapshotFromDir(projectDir, filter, message)
	if err != nil {
		log.Printf("autosave: snapshot failed for %s: %v", projectDir, err)
		return
	}

	elapsed := time.Since(t0)
	log.Printf("autosave: snapshot %s for %s (%s, took %v)", result.Snapshot.ID[:8], projectDir, reason, elapsed)
	if len(result.SkippedFiles) > 0 {
		log.Printf("autosave: skipped %d files", len(result.SkippedFiles))
	}
}

// collectSnapshotFiles collects file info from the most recent snapshot
func collectSnapshotFiles(df *storage.DataFile) map[string]fileInfo {
	files := make(map[string]fileInfo)
	paths := df.CollectAllSnapshotPaths()
	for _, p := range paths {
		// CollectAllSnapshotPaths returns paths like "/src/main.go"
		// We need to get size/lines info from the snapshot
		// For simplicity, just record the path exists (size comparison done via current walk)
		files[p] = fileInfo{path: p}
	}
	return files
}

// collectDirFiles collects file info from a directory walk
func collectDirFiles(projectDir string, filter storage.FileFilter) map[string]fileInfo {
	files := make(map[string]fileInfo)

	filepath.Walk(projectDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		relPath, relErr := filepath.Rel(projectDir, path)
		if relErr != nil || relPath == "." {
			return nil
		}

		isDir := info.IsDir()
		if !filter(relPath, isDir) {
			if isDir {
				return filepath.SkipDir
			}
			return nil
		}

		if isDir {
			return nil
		}

		// Use forward slash for consistent path format (like snapshot paths)
		normalizedPath := "/" + strings.ReplaceAll(relPath, string(filepath.Separator), "/")

		fi := fileInfo{
			path:  normalizedPath,
			size:  info.Size(),
			isDir: false,
		}

		// Detect binary vs text and count lines for text files
		fi.isBinary, fi.lines = detectFileType(path, info.Size())

		files[normalizedPath] = fi
		return nil
	})

	return files
}

// detectFileType checks if a file is binary and counts lines for text files
func detectFileType(path string, size int64) (isBinary bool, lines int) {
	// Skip large files (>1MB) - treat as binary
	if size > 1024*1024 {
		return true, -1
	}

	f, err := os.Open(path)
	if err != nil {
		return true, -1
	}
	defer f.Close()

	// Read first 8KB to detect binary
	buf := make([]byte, 8192)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return true, -1
	}
	buf = buf[:n]

	// Check for binary content (null bytes or invalid UTF-8)
	if bytes.Contains(buf, []byte{0}) || !utf8.Valid(buf) {
		return true, -1
	}

	// Count lines for text files
	f.Seek(0, 0)
	scanner := bufio.NewScanner(f)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}

	return false, lineCount
}

// changeType represents the type of change for a file
type changeType int

const (
	changeNone changeType = iota
	changeAdded
	changeModified
	changeDeleted
)

// treeNode represents a node in the file tree
type treeNode struct {
	name     string
	children map[string]*treeNode
	change   changeType
	info     string // change info like "(45 lines)" or "(+15 lines)"
	isDir    bool
}

func newTreeNode(name string, isDir bool) *treeNode {
	return &treeNode{
		name:     name,
		children: make(map[string]*treeNode),
		isDir:    isDir,
	}
}

// buildAutosaveMessage builds the snapshot message with session ID and file tree diff.
// The message starts with autosaveMessagePrefix to allow identification of autosave snapshots.
func buildAutosaveMessage(sessionID string, prevFiles, currentFiles map[string]fileInfo) string {
	var sb strings.Builder

	// Header with special prefix for identification
	fmt.Fprintf(&sb, "%s\n", autosaveMessagePrefix)
	if sessionID != "" {
		fmt.Fprintf(&sb, "session: %s\n", sessionID)
	}

	// Compute changes
	changes := make(map[string]struct {
		change changeType
		info   string
	})

	// Find added and modified files
	for path, curr := range currentFiles {
		if prev, exists := prevFiles[path]; exists {
			// File exists in both - check if modified
			if curr.size != prev.size || (curr.lines >= 0 && prev.lines >= 0 && curr.lines != prev.lines) {
				changes[path] = struct {
					change changeType
					info   string
				}{changeModified, formatFileDiff(prev, curr)}
			}
		} else {
			changes[path] = struct {
				change changeType
				info   string
			}{changeAdded, formatFileChange(curr)}
		}
	}

	// Find deleted files
	for path := range prevFiles {
		if _, exists := currentFiles[path]; !exists {
			changes[path] = struct {
				change changeType
				info   string
			}{changeDeleted, ""}
		}
	}

	if len(changes) == 0 {
		sb.WriteString("\n(no changes detected)\n")
		return sb.String()
	}

	// Build tree structure
	root := newTreeNode(".", true)
	for path, c := range changes {
		// path is like "/src/main.go" - remove leading slash
		cleanPath := strings.TrimPrefix(path, "/")
		parts := strings.Split(cleanPath, "/")

		current := root
		for i, part := range parts {
			isLast := i == len(parts)-1
			if _, exists := current.children[part]; !exists {
				current.children[part] = newTreeNode(part, !isLast)
			}
			if isLast {
				current.children[part].change = c.change
				current.children[part].info = c.info
			}
			current = current.children[part]
		}
	}

	// Render tree
	renderTree(&sb, root, "", true)

	return sb.String()
}

// renderTree renders the tree structure with box-drawing characters
func renderTree(sb *strings.Builder, node *treeNode, prefix string, isRoot bool) {
	if isRoot {
		sb.WriteString("(root)\n")
	}

	// Get sorted children names
	names := make([]string, 0, len(node.children))
	for name := range node.children {
		names = append(names, name)
	}
	sort.Strings(names)

	for i, name := range names {
		child := node.children[name]
		isLast := i == len(names)-1

		// Choose connector
		connector := "├── "
		if isLast {
			connector = "└── "
		}

		// Build the line
		var line strings.Builder
		line.WriteString(prefix)
		line.WriteString(connector)

		// Add change indicator
		switch child.change {
		case changeAdded:
			line.WriteString("+ ")
		case changeModified:
			line.WriteString("~ ")
		case changeDeleted:
			line.WriteString("- ")
		}

		// Add name (with trailing / for directories)
		line.WriteString(name)
		if child.isDir && len(child.children) > 0 {
			line.WriteString("/")
		}

		// Add change info (right-aligned with padding)
		if child.info != "" {
			// Pad to align info
			lineLen := line.Len()
			if lineLen < 40 {
				line.WriteString(strings.Repeat(" ", 40-lineLen))
			} else {
				line.WriteString("  ")
			}
			line.WriteString(child.info)
		}

		sb.WriteString(line.String())
		sb.WriteString("\n")

		// Recurse for directories
		if child.isDir && len(child.children) > 0 {
			newPrefix := prefix
			if isLast {
				newPrefix += "    "
			} else {
				newPrefix += "│   "
			}
			renderTree(sb, child, newPrefix, false)
		}
	}
}

// formatFileChange formats a single file's info (for added files)
func formatFileChange(fi fileInfo) string {
	if fi.isBinary {
		return fmt.Sprintf("(%s)", formatSize(fi.size))
	}
	if fi.lines >= 0 {
		return fmt.Sprintf("(%d lines)", fi.lines)
	}
	return ""
}

// formatFileDiff formats the diff between two file versions
func formatFileDiff(prev, curr fileInfo) string {
	if curr.isBinary || prev.isBinary {
		// Binary file - show size change
		diff := curr.size - prev.size
		if diff > 0 {
			return fmt.Sprintf("(+%s)", formatSize(diff))
		} else if diff < 0 {
			return fmt.Sprintf("(-%s)", formatSize(-diff))
		}
		return "(unchanged size)"
	}

	// Text file - show line change
	if curr.lines >= 0 && prev.lines >= 0 {
		diff := curr.lines - prev.lines
		if diff > 0 {
			return fmt.Sprintf("(+%d lines)", diff)
		} else if diff < 0 {
			return fmt.Sprintf("(%d lines)", diff)
		}
		return "(content changed)"
	}

	return ""
}

// formatSize formats a byte size in human-readable form
func formatSize(size int64) string {
	if size < 1024 {
		return fmt.Sprintf("%d bytes", size)
	} else if size < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(size)/1024)
	} else if size < 1024*1024*1024 {
		return fmt.Sprintf("%.1f MB", float64(size)/(1024*1024))
	}
	return fmt.Sprintf("%.1f GB", float64(size)/(1024*1024*1024))
}

// buildFileFilter is an alias to daemon.BuildFileFilter for local use
func buildFileFilter(projectDir string, gitignoreEnabled bool, includes, excludes []string) storage.FileFilter {
	return daemon.BuildFileFilter(projectDir, gitignoreEnabled, includes, excludes)
}

// isLatentFSDataFile checks if the given path is a .latentfs datafile
// (i.e., the path contains a .latentfs/ directory component)
func isLatentFSDataFile(absPath string) bool {
	return strings.Contains(absPath, string(filepath.Separator)+".latentfs"+string(filepath.Separator)) ||
		strings.Contains(absPath, "/.latentfs/")
}
