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

package daemon

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
)

// Request types
const (
	RequestMount           = "mount"
	RequestUnmount         = "unmount"
	RequestStatus          = "status"
	RequestStop            = "stop"
	RequestMountInfo       = "mount_info"       // Get mount info for a specific data file
	RequestInvalidateCache = "invalidate_cache" // Invalidate LatentFS cache for a data file
	RequestIsMounted       = "is_mounted"       // Check if a target path is mounted
	RequestAgentNotify     = "agent_notify"     // Agent hook notification (from Claude Code hooks)
	RequestProtect         = "protect"          // Protect a data file from deletion
	RequestRelease         = "release"          // Release protection on a data file
	RequestListProtect     = "list_protect"     // List all protected data files
	RequestCheckPath       = "check_path"       // Check if a path is inside any mount
	RequestReloadConfig    = "reload_config"    // Reload daemon config from disk

	// Snapshot operations (sync mode - blocks until complete)
	RequestSave           = "save"            // Create snapshot
	RequestRestore        = "restore"         // Restore from snapshot
	RequestGC             = "gc"              // Garbage collect
	RequestListSnapshots  = "list_snapshots"  // List snapshots
	RequestDeleteSnapshot = "delete_snapshot" // Delete snapshot
)

// Request represents an IPC request
type Request struct {
	Type     string   `json:"type"`
	DataFile string   `json:"data_file,omitempty"`
	Target   string   `json:"target,omitempty"`
	Targets  []string `json:"targets,omitempty"` // Multiple targets for batch operations (e.g., is_mounted)
	All      bool     `json:"all,omitempty"`

	// Agent notify fields
	EventType  string          `json:"event_type,omitempty"`  // Agent event type (e.g., PRE_TOOL_USE, POST_TOOL_USE)
	Payload    json.RawMessage `json:"payload,omitempty"`     // Raw JSON payload from agent hook
	ProjectDir string          `json:"project_dir,omitempty"` // Project directory (from CLAUDE_PROJECT_DIR)

	// CheckPath fields
	Path string `json:"path,omitempty"` // Path to check (for check_path request)

	// Snapshot operation fields (save/restore/gc/delete)
	Message      string   `json:"message,omitempty"`       // Save: snapshot message
	Tag          string   `json:"tag,omitempty"`           // Save: optional tag; Restore: restore by tag
	SnapshotID   string   `json:"snapshot_id,omitempty"`   // Restore/Delete: snapshot ID
	Paths        []string `json:"paths,omitempty"`         // Restore: specific paths for partial restore
	Vacuum       bool     `json:"vacuum,omitempty"`        // GC: run VACUUM after cleanup
	AllowPartial bool     `json:"allow_partial,omitempty"` // Save: allow partial snapshot on read errors
	FullSnapshot bool     `json:"full_snapshot,omitempty"` // Save: create full snapshot including source folder
	StatsOnly    bool     `json:"stats_only,omitempty"`    // GC: only return stats, don't run GC
}

// MountStatus represents a mount's status
type MountStatus struct {
	DataFile      string `json:"data_file"`
	Source        string `json:"source,omitempty"`         // Source folder for fork (empty if not a fork)
	ForkType      string `json:"fork_type,omitempty"`      // Fork type: "soft", "hard", or empty
	Target        string `json:"target"`                   // The symlink target path (where user accesses the mount)
	UUID          string `json:"uuid,omitempty"`           // UUID for fork mounts
	SymlinkTarget string `json:"symlink_target,omitempty"` // Same as Target for fork mounts (for clarity in mount_info response)
}

// ProtectedFile represents a protected data file
type ProtectedFile struct {
	DataFile  string `json:"data_file"`
	CreatedAt int64  `json:"created_at"` // Unix timestamp when protection was added
}

// SnapshotInfo represents snapshot metadata for IPC responses
type SnapshotInfo struct {
	ID        string `json:"id"`
	Message   string `json:"message"`
	Tag       string `json:"tag,omitempty"`
	CreatedAt int64  `json:"created_at"` // Unix timestamp
	FileCount int64  `json:"file_count"`
	TotalSize int64  `json:"total_size"` // bytes
}

// GCResult represents garbage collection results for IPC responses
type GCResult struct {
	OrphanedContentBlocks int   `json:"orphaned_content_blocks"`
	OrphanedBlocksBytes   int64 `json:"orphaned_blocks_bytes"`
	DeprecatedEpochs      int   `json:"deprecated_epochs"`
}

// StorageStats represents storage statistics for a data file
type StorageStats struct {
	TotalContentBlocks      int   `json:"total_content_blocks"`
	TotalContentBlocksBytes int64 `json:"total_content_blocks_bytes"`
	SnapshotCount           int   `json:"snapshot_count"`
	OrphanedContentBlocks   int   `json:"orphaned_content_blocks"`
	OrphanedBlocksBytes     int64 `json:"orphaned_blocks_bytes"`
	DeprecatedEpochCount    int   `json:"deprecated_epoch_count"`
}

// Response represents an IPC response
type Response struct {
	Success        bool            `json:"success"`
	Message        string          `json:"message,omitempty"`
	Error          string          `json:"error,omitempty"`
	PID            int             `json:"pid,omitempty"`
	Mounts         []MountStatus   `json:"mounts,omitempty"`
	MountedPaths   map[string]bool `json:"mounted_paths,omitempty"`   // For batch is_mounted: path -> mounted status
	ProtectedFiles []ProtectedFile `json:"protected_files,omitempty"` // For list_protect

	// CheckPath response fields
	IsInMount bool   `json:"is_in_mount,omitempty"` // True if path is inside a mount
	MountPath string `json:"mount_path,omitempty"`  // The mount path that contains the checked path

	// Snapshot operation response fields
	SnapshotID    string         `json:"snapshot_id,omitempty"`    // Save: created snapshot ID
	SnapshotType  string         `json:"snapshot_type,omitempty"`  // Save: "full" or "overlay"
	SourceFolder  string         `json:"source_folder,omitempty"`  // Save: source folder if soft-fork
	SkippedFiles  []string       `json:"skipped_files,omitempty"`  // Save: files skipped due to read errors
	Snapshots     []SnapshotInfo `json:"snapshots,omitempty"`      // ListSnapshots: list of snapshots
	RestoredPaths []string        `json:"restored_paths,omitempty"` // Restore: paths that were restored
	GCResult      *GCResult       `json:"gc_result,omitempty"`      // GC: cleanup statistics
	Stats         *StorageStats   `json:"stats,omitempty"`          // GC: storage statistics (when stats_only=true)
}

// Server is the IPC server
type Server struct {
	listener net.Listener
	handler  func(*Request) *Response
}

// NewServer creates a new IPC server
func NewServer(handler func(*Request) *Response) *Server {
	return &Server{handler: handler}
}

// Start starts the IPC server
func (s *Server) Start() error {
	// Remove existing socket
	os.Remove(SocketPath())

	// Create listener
	listener, err := net.Listen("unix", SocketPath())
	if err != nil {
		return fmt.Errorf("failed to create socket: %w", err)
	}
	s.listener = listener

	// Make socket accessible
	os.Chmod(SocketPath(), 0600)

	// Start accepting connections
	go s.accept()

	return nil
}

// Stop stops the IPC server
func (s *Server) Stop() {
	if s.listener != nil {
		s.listener.Close()
		os.Remove(SocketPath())
	}
}

func (s *Server) accept() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return // Server stopped
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	// Read request
	decoder := json.NewDecoder(conn)
	var req Request
	if err := decoder.Decode(&req); err != nil {
		return
	}

	// Handle request
	resp := s.handler(&req)

	// Send response
	encoder := json.NewEncoder(conn)
	encoder.Encode(resp)
}

// Client is the IPC client
type Client struct {
	conn net.Conn
}

// Connect connects to the daemon
func Connect() (*Client, error) {
	conn, err := net.Dial("unix", SocketPath())
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

// Close closes the connection
func (c *Client) Close() error {
	return c.conn.Close()
}

// Send sends a request and returns the response
func (c *Client) Send(req *Request) (*Response, error) {
	// Send request
	encoder := json.NewEncoder(c.conn)
	if err := encoder.Encode(req); err != nil {
		return nil, err
	}

	// Read response
	decoder := json.NewDecoder(c.conn)
	var resp Response
	if err := decoder.Decode(&resp); err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("daemon closed connection")
		}
		return nil, err
	}

	return &resp, nil
}

// Mount sends a mount request to mount a data file at a target path
// The data file determines if this is a soft-fork (has source folder stored) or not
func (c *Client) Mount(dataFile, target string) (*Response, error) {
	return c.Send(&Request{
		Type:     RequestMount,
		DataFile: dataFile,
		Target:   target,
	})
}

// Unmount sends an unmount request
func (c *Client) Unmount(target string, all bool) (*Response, error) {
	return c.Send(&Request{
		Type:   RequestUnmount,
		Target: target,
		All:    all,
	})
}

// Status sends a status request
func (c *Client) Status() (*Response, error) {
	return c.Send(&Request{Type: RequestStatus})
}

// Stop sends a stop request
func (c *Client) Stop() (*Response, error) {
	return c.Send(&Request{Type: RequestStop})
}

// GetMountInfo sends a request to get mount info for a specific data file
// Returns the MountStatus with symlink target info, or nil if not mounted
func (c *Client) GetMountInfo(dataFile string) (*MountStatus, error) {
	resp, err := c.Send(&Request{
		Type:     RequestMountInfo,
		DataFile: dataFile,
	})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, nil // Not mounted or not found
	}
	if len(resp.Mounts) > 0 {
		return &resp.Mounts[0], nil
	}
	return nil, nil
}

// InvalidateCache sends a request to invalidate the LatentFS cache for a data file
// This should be called after external modifications to the data file (e.g., restore)
// to ensure the NFS mount reflects the latest state
func (c *Client) InvalidateCache(dataFile string) error {
	resp, err := c.Send(&Request{
		Type:     RequestInvalidateCache,
		DataFile: dataFile,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("invalidate cache failed: %s", resp.Error)
	}
	return nil
}

// CheckMounts checks if one or more target paths are mounted.
// Returns a map of path -> mounted status, and true if ALL paths are mounted.
// For single path, use CheckMounts([]string{path}) or the convenience IsMounted method.
func (c *Client) CheckMounts(targets []string) (map[string]bool, bool, error) {
	resp, err := c.Send(&Request{
		Type:    RequestIsMounted,
		Targets: targets,
	})
	if err != nil {
		return nil, false, err
	}
	return resp.MountedPaths, resp.Success, nil
}

// IsMounted checks if a single target path is mounted (convenience wrapper).
func (c *Client) IsMounted(target string) (bool, error) {
	_, allMounted, err := c.CheckMounts([]string{target})
	return allMounted, err
}

// AgentNotify sends an agent hook notification to the daemon
func (c *Client) AgentNotify(eventType string, payload json.RawMessage, projectDir string) (*Response, error) {
	return c.Send(&Request{
		Type:       RequestAgentNotify,
		EventType:  eventType,
		Payload:    payload,
		ProjectDir: projectDir,
	})
}

// Protect requests the daemon to protect a data file from deletion
func (c *Client) Protect(dataFile string) (*Response, error) {
	return c.Send(&Request{
		Type:     RequestProtect,
		DataFile: dataFile,
	})
}

// Release requests the daemon to release protection on a data file
func (c *Client) Release(dataFile string) (*Response, error) {
	return c.Send(&Request{
		Type:     RequestRelease,
		DataFile: dataFile,
	})
}

// ListProtect requests the list of protected data files
func (c *Client) ListProtect() ([]ProtectedFile, error) {
	resp, err := c.Send(&Request{Type: RequestListProtect})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("list protect failed: %s", resp.Error)
	}
	return resp.ProtectedFiles, nil
}

// CheckPathInMount checks if a path is inside any latentfs mount.
// Returns (isInMount, mountPath, error).
// If isInMount is true, mountPath contains the mount that contains the path.
func (c *Client) CheckPathInMount(path string) (bool, string, error) {
	resp, err := c.Send(&Request{
		Type: RequestCheckPath,
		Path: path,
	})
	if err != nil {
		return false, "", err
	}
	if !resp.Success {
		return false, "", fmt.Errorf("check path failed: %s", resp.Error)
	}
	return resp.IsInMount, resp.MountPath, nil
}

// ReloadConfig requests the daemon to reload its configuration from disk
func (c *Client) ReloadConfig() error {
	resp, err := c.Send(&Request{Type: RequestReloadConfig})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("reload config failed: %s", resp.Error)
	}
	return nil
}

// Save creates a snapshot of the data file via daemon IPC
// If fullSnapshot is true, includes source folder files (for overlay forks)
func (c *Client) Save(dataFile, message, tag string, allowPartial, fullSnapshot bool) (*Response, error) {
	resp, err := c.Send(&Request{
		Type:         RequestSave,
		DataFile:     dataFile,
		Message:      message,
		Tag:          tag,
		AllowPartial: allowPartial,
		FullSnapshot: fullSnapshot,
	})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("save failed: %s", resp.Error)
	}
	return resp, nil
}

// Restore restores from a snapshot via daemon IPC
// Use snapshotID or tag (not both). paths is optional for partial restore.
func (c *Client) Restore(dataFile, snapshotID, tag string, paths []string) (*Response, error) {
	resp, err := c.Send(&Request{
		Type:       RequestRestore,
		DataFile:   dataFile,
		SnapshotID: snapshotID,
		Tag:        tag,
		Paths:      paths,
	})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("restore failed: %s", resp.Error)
	}
	return resp, nil
}

// GC runs garbage collection on the data file via daemon IPC
func (c *Client) GC(dataFile string, vacuum, statsOnly bool) (*Response, error) {
	resp, err := c.Send(&Request{
		Type:      RequestGC,
		DataFile:  dataFile,
		Vacuum:    vacuum,
		StatsOnly: statsOnly,
	})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("gc failed: %s", resp.Error)
	}
	return resp, nil
}

// ListSnapshots lists all snapshots in the data file via daemon IPC
func (c *Client) ListSnapshots(dataFile string) ([]SnapshotInfo, error) {
	resp, err := c.Send(&Request{
		Type:     RequestListSnapshots,
		DataFile: dataFile,
	})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("list snapshots failed: %s", resp.Error)
	}
	return resp.Snapshots, nil
}

// DeleteSnapshot deletes a snapshot from the data file via daemon IPC
// Use snapshotID or tag (not both)
func (c *Client) DeleteSnapshot(dataFile, snapshotID, tag string) error {
	resp, err := c.Send(&Request{
		Type:       RequestDeleteSnapshot,
		DataFile:   dataFile,
		SnapshotID: snapshotID,
		Tag:        tag,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("delete snapshot failed: %s", resp.Error)
	}
	return nil
}

// IsDaemonRunning checks if the daemon is running
func IsDaemonRunning() bool {
	client, err := Connect()
	if err != nil {
		return false
	}
	client.Close()
	return true
}
