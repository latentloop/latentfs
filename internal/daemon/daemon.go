package daemon

// CRITICAL NFS DEADLOCK WARNING:
// When the daemon processes NFS requests, any filesystem access (os.Stat, os.ReadFile,
// os.Open, etc.) to a path that is itself an NFS mount will cause a deadlock.
// This includes:
// - Symlinks that point to NFS mounts (e.g., fork symlinks)
// - Paths inside NFS-mounted directories
//
// ALWAYS assume that any path access in the daemon could potentially go through NFS.
// Use the SourceResolver's GetVFSBySymlinkTarget() to check if a path is a mounted VFS
// before performing any filesystem operations on it. The resolver accesses VFS data
// directly without going through NFS.

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gofrs/flock"
	"github.com/google/uuid"
	logrus "github.com/sirupsen/logrus"

	"latentfs/internal/storage"
	"latentfs/internal/util"
	latentfs "latentfs/internal/vfs"
)

func init() {
	// Default logging to discard until explicitly enabled via --logging flag
	logrus.SetOutput(io.Discard)
}

// Daemon manages mounts via centralFS
type Daemon struct {
	ipcServer *Server
	logFile   *os.File
	stopCh    chan struct{}
	wg        sync.WaitGroup
	lock      *flock.Flock

	// Logging configuration
	// LogLevel sets the logging level: trace, debug, info, warn, off (default: off)
	LogLevel string

	// SkipCleanup skips startup cleanup tasks (stale mounts, zombie daemons).
	// Used by integration tests to avoid interfering with parallel test daemons.
	SkipCleanup bool

	// Central mount
	centralMetaFile *storage.MetaFile
	centralFS       *latentfs.MetaFS
	centralServer   NetFSServer
	centralPort     int
	centralIP       string // unique loopback IP (e.g., "127.0.0.1")

	// Protected files: keep data files open to prevent deletion
	protectedFiles sync.Map // dataFile path -> *storage.DataFile

	// Concurrent operation coordination for save/restore/gc
	// Prevents multiple operations on the same data file
	inProgressOps map[string]string // dataFile -> operation type ("save", "restore", "gc")
	inProgressMu  sync.Mutex
}

// New creates a new daemon instance
func New() *Daemon {
	return &Daemon{
		stopCh:        make(chan struct{}),
		inProgressOps: make(map[string]string),
	}
}

// Run starts the daemon and blocks until stopped
func (d *Daemon) Run() error {
	if err := EnsureConfigDir(); err != nil {
		return err
	}

	// Load global settings and set busy_timeout values
	if settings, err := LoadGlobalSettings(); err == nil {
		storage.SetConfigBusyTimeouts(settings.DaemonBusyTimeout, settings.CLIBusyTimeout)
	}

	if !d.SkipCleanup {
		// Clean up stale mounts from previous crashed sessions
		if result, err := CleanupStaleMounts(); err == nil {
			if len(result.StaleMounts) > 0 || result.CleanedPidFile || result.CleanedSocket {
				log.Printf("Startup cleanup: %s", FormatCleanupResult(result))
			}
		}

		// Clean up our own mount point (in case it was left behind from a crash)
		if err := CleanupOwnMount(); err != nil {
			log.Printf("Warning: failed to clean up own mount: %v", err)
		}

		// Clean up all stale mount directories (empty, unmounted mnt_* dirs)
		if cleaned, err := CleanupStaleMountDirectories(); err == nil && cleaned > 0 {
			log.Printf("Cleaned up %d stale mount directories", cleaned)
		}

		// Kill zombie daemon processes (old binaries, orphaned test daemons)
		if killed := KillZombieDaemons(); killed > 0 {
			log.Printf("Killed %d zombie daemon processes", killed)
		}
	}

	// Acquire exclusive lock
	d.lock = flock.New(LockPath())
	locked, err := d.lock.TryLock()
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !locked {
		return fmt.Errorf("another daemon instance is already running")
	}
	defer d.lock.Unlock()

	// Setup logging based on log level (case insensitive)
	logLevel := strings.ToLower(d.LogLevel)
	if logLevel != "" && logLevel != "none" {
		// Truncate log file if it exceeds 50MB
		if err := d.truncateLogFile(50 * 1024 * 1024); err != nil {
			// Non-fatal, just log to stderr
			fmt.Fprintf(os.Stderr, "Warning: failed to truncate log file: %v\n", err)
		}

		logFile, err := os.OpenFile(LogPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return fmt.Errorf("failed to open log file: %w", err)
		}
		d.logFile = logFile
		log.SetOutput(logFile)
		log.SetFlags(log.LstdFlags | log.Lshortfile)

		// Also redirect logrus to the log file
		logrus.SetOutput(logFile)

		// Set logrus level based on LogLevel (case insensitive)
		switch logLevel {
		case "trace":
			logrus.SetLevel(logrus.TraceLevel)
		case "debug":
			logrus.SetLevel(logrus.DebugLevel)
		case "info":
			logrus.SetLevel(logrus.InfoLevel)
		case "warn":
			logrus.SetLevel(logrus.WarnLevel)
		default:
			logrus.SetLevel(logrus.DebugLevel)
		}
	} else {
		// Disable logging by sending to /dev/null
		log.SetOutput(io.Discard)
		logrus.SetOutput(io.Discard)
	}

	// Write PID file
	if err := d.writePidFile(); err != nil {
		return err
	}
	defer d.removePidFile()

	log.Printf("Daemon started (PID %d)", os.Getpid())

	// Start central mount BEFORE IPC server so it's ready for fork requests
	if err := d.startCentralMount(); err != nil {
		log.Printf("Warning: failed to start central mount: %v", err)
		// Continue anyway - daemon can still handle individual mounts
	} else {
		log.Printf("Central mount started at %s", CentralMountPath())

		// Restore saved mounts if mount_on_start is enabled
		if settings, err := LoadGlobalSettings(); err == nil && settings.MountOnStart {
			if err := d.restoreSavedMounts(); err != nil {
				log.Printf("Warning: failed to restore saved mounts: %v", err)
			}
		}
	}

	// Start IPC server (after central mount is ready)
	log.Printf("Starting IPC server at %s", SocketPath())
	d.ipcServer = NewServer(d.handleRequest)
	if err := d.ipcServer.Start(); err != nil {
		log.Printf("IPC server failed to start: %v", err)
		return err
	}
	log.Printf("IPC server started successfully")
	defer d.ipcServer.Stop()

	// Watch parent process (test runner) and self-terminate if it dies.
	// When Go's test timeout fires, os.Exit(2) bypasses all defers, leaving
	// daemon processes orphaned. This goroutine detects parent death and
	// triggers graceful shutdown (unmount NFS, cleanup, exit).
	if ppidStr := os.Getenv("LATENTFS_PARENT_PID"); ppidStr != "" {
		if ppid, err := strconv.Atoi(ppidStr); err == nil && ppid > 0 {
			go func() {
				ticker := time.NewTicker(500 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-d.stopCh:
						return
					case <-ticker.C:
						// syscall.Kill(pid, 0) checks if process exists without signaling.
						// Returns error when process no longer exists.
						if err := syscall.Kill(ppid, 0); err != nil {
							log.Printf("Parent process (PID %d) died, shutting down to prevent orphan daemon", ppid)
							select {
							case <-d.stopCh:
							default:
								close(d.stopCh)
							}
							return
						}
					}
				}
			}()
			log.Printf("Watching parent process PID %s for orphan prevention", ppidStr)
		}
	}

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("Received signal %v, shutting down...", sig)
	case <-d.stopCh:
		log.Printf("Stop requested, shutting down...")
	}

	// Unmount all mounts from centralFS, then stop central mount
	d.unmountAllCentralFS()
	d.stopCentralMount()

	// Wait for goroutines
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("All mount goroutines finished")
	case <-time.After(500 * time.Millisecond):
		log.Printf("Timeout waiting for mount goroutines")
	}

	log.Printf("Daemon stopped")
	return nil
}

// handleRequest processes an IPC request
func (d *Daemon) handleRequest(req *Request) *Response {
	switch req.Type {
	case RequestMount:
		return d.handleMount(req)
	case RequestUnmount:
		return d.handleUnmount(req)
	case RequestStatus:
		return d.handleStatus()
	case RequestStop:
		return d.handleStop()
	case RequestMountInfo:
		return d.handleMountInfo(req)
	case RequestInvalidateCache:
		return d.handleInvalidateCache(req)
	case RequestIsMounted:
		return d.handleIsMounted(req)
	case RequestAgentNotify:
		return d.handleAgentNotify(req)
	case RequestProtect:
		return d.handleProtect(req)
	case RequestRelease:
		return d.handleRelease(req)
	case RequestListProtect:
		return d.handleListProtect()
	case RequestCheckPath:
		return d.handleCheckPath(req)
	case RequestReloadConfig:
		return d.handleReloadConfig()
	case RequestSave:
		return d.handleSave(req)
	case RequestRestore:
		return d.handleRestore(req)
	case RequestGC:
		return d.handleGC(req)
	case RequestListSnapshots:
		return d.handleListSnapshots(req)
	case RequestDeleteSnapshot:
		return d.handleDeleteSnapshot(req)
	default:
		return &Response{Success: false, Error: "unknown request type"}
	}
}

func (d *Daemon) handleUnmount(req *Request) *Response {
	if req.All {
		// Unmount all mounts from centralFS
		if d.centralFS != nil {
			entries, err := d.centralFS.ListMounts()
			if err == nil {
				for _, e := range entries {
					// Remove symlink
					if e.SymlinkTarget != "" {
						os.Remove(e.SymlinkTarget)
					}
					// Remove sub-mount from centralFS
					d.centralFS.RemoveSubMount(e.SubPath)
				}
			}
		}
		return &Response{Success: true, Message: "All folders unmounted"}
	}

	// Check centralFS for the mount
	if d.centralFS == nil {
		return &Response{Success: false, Error: "central mount not available"}
	}

	mountEntry, err := d.centralFS.GetMountBySymlinkTarget(req.Target)
	if err != nil {
		log.Printf("handleUnmount: failed to lookup mount by symlink: %v", err)
		return &Response{Success: false, Error: fmt.Sprintf("lookup failed: %v", err)}
	}
	if mountEntry == nil {
		return &Response{Success: false, Error: fmt.Sprintf("not mounted: %s", req.Target)}
	}

	// Remove symlink and sub-mount
	log.Printf("handleUnmount: found mount for %s (subPath=%s)", req.Target, mountEntry.SubPath)

	// Remove the symlink
	if err := os.Remove(req.Target); err != nil {
		log.Printf("handleUnmount: failed to remove symlink %s: %v", req.Target, err)
		// Continue anyway - symlink might already be gone
	}

	// Remove from centralFS
	if err := d.centralFS.RemoveSubMount(mountEntry.SubPath); err != nil {
		return &Response{Success: false, Error: fmt.Sprintf("failed to remove mount: %v", err)}
	}

	log.Printf("handleUnmount: unmounted %s", req.Target)
	return &Response{Success: true, Message: fmt.Sprintf("Unmounted %s", req.Target)}
}

func (d *Daemon) handleStatus() *Response {
	var mounts []MountStatus

	// Get mounts from centralFS (the primary mount tracking system)
	if d.centralFS != nil {
		entries, err := d.centralFS.ListMounts()
		if err != nil {
			log.Printf("handleStatus: failed to list mounts: %v", err)
		} else {
			for _, e := range entries {
				mounts = append(mounts, MountStatus{
					DataFile:      e.DataFile,
					Source:        e.SourceFolder,
					ForkType:      e.ForkType,
					Target:        e.SymlinkTarget,
					UUID:          e.SubPath,
					SymlinkTarget: e.SymlinkTarget,
				})
			}
		}
	}

	return &Response{
		Success: true,
		PID:     os.Getpid(),
		Mounts:  mounts,
	}
}

func (d *Daemon) handleStop() *Response {
	select {
	case <-d.stopCh:
	default:
		close(d.stopCh)
	}
	return &Response{Success: true, Message: "Daemon stopping"}
}

func (d *Daemon) handleMountInfo(req *Request) *Response {
	log.Printf("handleMountInfo: looking up data file %s", req.DataFile)

	// Check if central mount is available
	if d.centralFS == nil {
		return &Response{Success: false, Error: "central mount not available"}
	}

	// Look up the mount by data file
	mountEntry, err := d.centralFS.IsDataFileMounted(req.DataFile)
	if err != nil {
		log.Printf("handleMountInfo: lookup failed: %v", err)
		return &Response{Success: false, Error: fmt.Sprintf("lookup failed: %v", err)}
	}
	if mountEntry == nil {
		log.Printf("handleMountInfo: data file not mounted")
		return &Response{Success: false, Error: "data file not mounted"}
	}

	log.Printf("handleMountInfo: found mount at %s", mountEntry.SymlinkTarget)
	return &Response{
		Success: true,
		Mounts: []MountStatus{{
			DataFile:      mountEntry.DataFile,
			Source:        mountEntry.SourceFolder,
			ForkType:      mountEntry.ForkType,
			Target:        mountEntry.SymlinkTarget,
			SymlinkTarget: mountEntry.SymlinkTarget,
			UUID:          mountEntry.SubPath,
		}},
	}
}

func (d *Daemon) handleInvalidateCache(req *Request) *Response {
	log.Printf("handleInvalidateCache: invalidating cache for data file %s", req.DataFile)

	// Check if central mount is available
	if d.centralFS == nil {
		return &Response{Success: false, Error: "central mount not available"}
	}

	// Invalidate cache for the data file
	if err := d.centralFS.InvalidateCacheForDataFile(req.DataFile); err != nil {
		log.Printf("handleInvalidateCache: failed: %v", err)
		return &Response{Success: false, Error: fmt.Sprintf("invalidate cache failed: %v", err)}
	}

	log.Printf("handleInvalidateCache: cache invalidated successfully")
	return &Response{Success: true, Message: "Cache invalidated"}
}

func (d *Daemon) handleIsMounted(req *Request) *Response {
	// Check if central mount is available
	if d.centralFS == nil {
		return &Response{Success: false, Error: "central mount not available"}
	}

	// Get targets (supports both single Target and multiple Targets)
	targets := req.Targets
	if len(targets) == 0 && req.Target != "" {
		targets = []string{req.Target}
	}
	if len(targets) == 0 {
		return &Response{Success: false, Error: "no target paths specified"}
	}

	// Check all targets
	mountedPaths := make(map[string]bool)
	allMounted := true
	for _, target := range targets {
		mountEntry, err := d.centralFS.GetMountBySymlinkTarget(target)
		if err != nil {
			mountedPaths[target] = false
			allMounted = false
			continue
		}
		mounted := mountEntry != nil
		mountedPaths[target] = mounted
		if !mounted {
			allMounted = false
		}
	}
	return &Response{Success: allMounted, MountedPaths: mountedPaths}
}

func (d *Daemon) handleAgentNotify(req *Request) *Response {
	// Daemon only logs agent events; autosave runs in CLI (agent.go)
	log.Printf("handleAgentNotify: event_type=%s project_dir=%s", req.EventType, req.ProjectDir)
	return &Response{Success: true, Message: fmt.Sprintf("received %s", req.EventType)}
}

func (d *Daemon) handleCheckPath(req *Request) *Response {
	if req.Path == "" {
		return &Response{Success: false, Error: "path is required"}
	}

	// Clean the path for comparison
	checkPath := filepath.Clean(req.Path)

	// Get all mounts
	if d.centralFS == nil {
		// No mounts if central FS not available
		return &Response{Success: true, IsInMount: false}
	}

	entries, err := d.centralFS.ListMounts()
	if err != nil {
		return &Response{Success: false, Error: fmt.Sprintf("failed to list mounts: %v", err)}
	}

	// Check if path is inside any mount
	for _, entry := range entries {
		mountTarget := entry.SymlinkTarget
		if mountTarget == "" {
			continue
		}

		mountTarget = filepath.Clean(mountTarget)

		// Check if path is the mount itself or inside the mount
		if checkPath == mountTarget || strings.HasPrefix(checkPath, mountTarget+string(filepath.Separator)) {
			return &Response{Success: true, IsInMount: true, MountPath: mountTarget}
		}
	}

	return &Response{Success: true, IsInMount: false}
}

func (d *Daemon) handleReloadConfig() *Response {
	log.Printf("handleReloadConfig: reloading daemon configuration")

	// Load settings from disk
	settings, err := LoadGlobalSettings()
	if err != nil {
		log.Printf("handleReloadConfig: failed to load settings: %v", err)
		return &Response{Success: false, Error: fmt.Sprintf("failed to load settings: %v", err)}
	}

	// Update log level
	logLevel := strings.ToLower(settings.LogLevel)
	d.LogLevel = settings.LogLevel

	if logLevel != "" && logLevel != "none" {
		// Enable logging
		if d.logFile == nil {
			// Open log file if not already open
			logFile, err := os.OpenFile(LogPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				return &Response{Success: false, Error: fmt.Sprintf("failed to open log file: %v", err)}
			}
			d.logFile = logFile
			log.SetOutput(logFile)
			log.SetFlags(log.LstdFlags | log.Lshortfile)
			logrus.SetOutput(logFile)
		}

		// Set logrus level
		switch logLevel {
		case "trace":
			logrus.SetLevel(logrus.TraceLevel)
		case "debug":
			logrus.SetLevel(logrus.DebugLevel)
		case "info":
			logrus.SetLevel(logrus.InfoLevel)
		case "warn":
			logrus.SetLevel(logrus.WarnLevel)
		default:
			logrus.SetLevel(logrus.DebugLevel)
		}
		log.Printf("handleReloadConfig: log level set to %s", logLevel)
	} else {
		// Disable logging
		log.SetOutput(io.Discard)
		logrus.SetOutput(io.Discard)
		if d.logFile != nil {
			d.logFile.Close()
			d.logFile = nil
		}
	}

	return &Response{Success: true, Message: fmt.Sprintf("Config reloaded, log level: %s", logLevel)}
}

func (d *Daemon) handleMount(req *Request) *Response {
	log.Printf("handleMount: received request dataFile=%s target=%s", req.DataFile, req.Target)

	// Check if central mount is available
	if d.centralFS == nil {
		return &Response{Success: false, Error: "central mount not available"}
	}

	// Check if data file is already mounted
	existingMount, err := d.centralFS.IsDataFileMounted(req.DataFile)
	if err != nil {
		log.Printf("handleMount: failed to check existing mounts: %v", err)
		return &Response{Success: false, Error: fmt.Sprintf("failed to check existing mounts: %v", err)}
	}
	if existingMount != nil {
		log.Printf("handleMount: data file already mounted at %s", existingMount.SymlinkTarget)
		return &Response{Success: false, Error: fmt.Sprintf("data file already mounted at: %s", existingMount.SymlinkTarget)}
	}

	// Generate UUID for the sub-path
	mountUUID := uuid.New().String()
	subPath := mountUUID

	// Open the data file with daemon context
	dataFile, err := storage.OpenWithContext(req.DataFile, storage.DBContextDaemon)
	if err != nil {
		log.Printf("handleMount: failed to open data file: %v", err)
		return &Response{Success: false, Error: fmt.Sprintf("failed to open data file: %v", err)}
	}

	// Read fork metadata from data file
	source := dataFile.GetSourceFolder()
	forkType := dataFile.GetForkType()
	log.Printf("handleMount: source=%s forkType=%s", source, forkType)

	// Add to central MetaFS
	// MetaFS.AddForkMount uses forkType to decide whether to enable COW:
	// - Soft-fork with source: enables copy-on-write
	// - Hard-fork/regular mount: no COW (files in data file only)
	// Source folder is stored for metadata tracking (origin of fork)
	if err := d.centralFS.AddForkMount(subPath, dataFile, source, forkType, req.Target); err != nil {
		dataFile.Close()
		log.Printf("handleMount: failed to add fork mount: %v", err)
		return &Response{Success: false, Error: fmt.Sprintf("failed to add fork mount: %v", err)}
	}

	// Create symlink from target to central mount subpath
	centralSubPath := filepath.Join(CentralMountPath(), subPath)
	if err := os.Symlink(centralSubPath, req.Target); err != nil {
		// Try to remove the mount entry on failure
		d.centralFS.RemoveSubMount(subPath)
		dataFile.Close()
		log.Printf("handleMount: failed to create symlink: %v", err)
		return &Response{Success: false, Error: fmt.Sprintf("failed to create symlink: %v", err)}
	}

	log.Printf("handleMount: success - mounted %s at %s -> %s", req.DataFile, subPath, req.Target)

	// Generate appropriate message based on fork type
	msg := fmt.Sprintf("Mounted at %s", req.Target)
	switch forkType {
	case storage.ForkTypeSoft:
		msg = fmt.Sprintf("Mounted %s at %s (soft-fork, copy-on-write)", source, req.Target)
	case storage.ForkTypeHard:
		msg = fmt.Sprintf("Mounted at %s (hard-fork from %s)", req.Target, source)
	}
	return &Response{Success: true, Message: msg}
}

// unmountAllCentralFS unmounts all mounts from centralFS (used during shutdown)
// When mount_on_start is enabled, metadata is preserved to allow auto-restore on next start.
func (d *Daemon) unmountAllCentralFS() {
	if d.centralFS == nil {
		return
	}

	// Check if mount_on_start is enabled
	preserveMetadata := false
	if settings, err := LoadGlobalSettings(); err == nil {
		preserveMetadata = settings.MountOnStart
	}

	entries, err := d.centralFS.ListMounts()
	if err != nil {
		log.Printf("unmountAllCentralFS: failed to list mounts: %v", err)
		return
	}

	for _, e := range entries {
		log.Printf("Unmounting %s (preserveMetadata=%v)", e.SymlinkTarget, preserveMetadata)
		// Remove symlink
		if e.SymlinkTarget != "" {
			os.Remove(e.SymlinkTarget)
		}
		// Remove sub-mount from centralFS
		if preserveMetadata {
			// Close VFS but keep metadata for auto-restore on next start
			if err := d.centralFS.CloseSubMountPreserveMetadata(e.SubPath); err != nil {
				log.Printf("Warning: failed to close mount %s: %v", e.SubPath, err)
			}
		} else {
			// Fully remove mount including metadata
			if err := d.centralFS.RemoveSubMount(e.SubPath); err != nil {
				log.Printf("Warning: failed to remove mount %s: %v", e.SubPath, err)
			}
		}
	}
}

func (d *Daemon) writePidFile() error {
	data := []byte(strconv.Itoa(os.Getpid()))
	return os.WriteFile(PidPath(), data, 0600)
}

func (d *Daemon) removePidFile() {
	os.Remove(PidPath())
}

// GetPID reads the daemon PID from file
func GetPID() (int, error) {
	data, err := os.ReadFile(PidPath())
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(data))
}

// findAvailablePort finds an available TCP port
func findAvailablePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

// waitForPort waits until a port is accepting connections on the given IP
func waitForPort(ip string, port int, timeout time.Duration) error {
	addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
	if util.WaitWithDeadline(time.Now().Add(timeout), 50*time.Millisecond, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		return false
	}) {
		return nil
	}
	return fmt.Errorf("timeout waiting for port %d", port)
}

// startCentralMount starts the central metadata mount
func (d *Daemon) startCentralMount() error {
	t0 := time.Now()

	// Open or create metadata file with daemon context
	metaFile, err := storage.OpenOrCreateMetaWithContext(MetaFilePath(), storage.DBContextDaemon)
	if err != nil {
		return fmt.Errorf("failed to open meta file: %w", err)
	}
	log.Printf("startCentralMount: openMeta took %v", time.Since(t0))

	// Create MetaFS using local variable — only assign to d.centralFS after
	// all error-prone operations succeed, so the daemon never ends up with
	// a non-nil centralFS backed by a closed metaFile.
	centralFS := latentfs.NewMetaFS(metaFile)

	// Find available port
	t1 := time.Now()
	port, err := findAvailablePort()
	if err != nil {
		metaFile.Close()
		return fmt.Errorf("failed to find available port: %w", err)
	}
	log.Printf("startCentralMount: findPort took %v (port=%d)", time.Since(t1), port)

	// Create network filesystem server
	t2 := time.Now()
	srv, err := createServerForMetaFS(centralFS, "latentfs")
	if err != nil {
		metaFile.Close()
		return fmt.Errorf("failed to create central server: %w", err)
	}
	log.Printf("startCentralMount: createServer took %v", time.Since(t2))

	ip := "127.0.0.1"

	// Start server in background
	addr := fmt.Sprintf("%s:%d", ip, port)
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		if err := srv.Serve(addr); err != nil {
			log.Printf("Central %s server error: %v", NetFSType(), err)
		}
	}()

	// Wait for server to be ready
	t3 := time.Now()
	if err := waitForPort(ip, port, 3*time.Second); err != nil {
		srv.Shutdown()
		metaFile.Close()
		return fmt.Errorf("central server failed to start: %w", err)
	}
	log.Printf("startCentralMount: waitForPort took %v", time.Since(t3))

	// Mount with one fast retry — mount_nfs can transiently fail under
	// parallel test load when multiple daemons mount concurrently on macOS.
	// Keep retry minimal (1 retry, 200ms pause) to stay within the 5s
	// auto-start timeout budget.
	mountPath := CentralMountPath()
	t4 := time.Now()
	var mountErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			log.Printf("startCentralMount: mount retry after %v", mountErr)
			time.Sleep(200 * time.Millisecond)
		}
		mountErr = mountNetFS(ip, port, "latentfs", mountPath)
		if mountErr == nil {
			break
		}
	}
	if mountErr != nil {
		srv.Shutdown()
		metaFile.Close()
		return fmt.Errorf("failed to mount central filesystem: %w", mountErr)
	}
	log.Printf("startCentralMount: mount took %v (total=%v)", time.Since(t4), time.Since(t0))

	// All operations succeeded — commit state to daemon
	d.centralMetaFile = metaFile
	d.centralFS = centralFS
	d.centralPort = port
	d.centralIP = ip
	d.centralServer = srv

	// Load protected files from meta file
	d.loadProtectedFiles()

	return nil
}

// restoreSavedMounts restores mounts from metadata when mount_on_start is enabled.
// This is called during daemon startup to restore mounts from the previous session.
func (d *Daemon) restoreSavedMounts() error {
	if d.centralFS == nil {
		return nil
	}

	entries, err := d.centralFS.ListMounts()
	if err != nil {
		return fmt.Errorf("failed to list saved mounts: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	log.Printf("restoreSavedMounts: found %d saved mounts to restore", len(entries))

	var restored, failed int
	for _, e := range entries {
		// Check if data file still exists
		if _, err := os.Stat(e.DataFile); os.IsNotExist(err) {
			log.Printf("restoreSavedMounts: data file no longer exists, removing mount: %s", e.DataFile)
			// Remove stale mount from metadata
			d.centralFS.RemoveSubMount(e.SubPath)
			failed++
			continue
		}

		// Load the VFS from metadata entry
		if err := d.centralFS.LoadSubMountFromMetadata(&e); err != nil {
			log.Printf("restoreSavedMounts: failed to load mount %s: %v", e.SymlinkTarget, err)
			// Remove failed mount from metadata
			d.centralFS.RemoveSubMount(e.SubPath)
			failed++
			continue
		}

		// Check if symlink target path is available
		if e.SymlinkTarget != "" {
			// Check if something already exists at target path
			if info, err := os.Lstat(e.SymlinkTarget); err == nil {
				// Something exists - check if it's a symlink pointing to our mount
				if info.Mode()&os.ModeSymlink != 0 {
					target, _ := os.Readlink(e.SymlinkTarget)
					expectedTarget := filepath.Join(CentralMountPath(), e.SubPath)
					if target == expectedTarget {
						// Symlink already correct, nothing to do
						log.Printf("restoreSavedMounts: symlink already exists for %s", e.SymlinkTarget)
						restored++
						continue
					}
					// Symlink points elsewhere, remove it
					os.Remove(e.SymlinkTarget)
				} else {
					// Not a symlink - skip this mount (user has a file/dir there)
					log.Printf("restoreSavedMounts: target path occupied (not a symlink): %s", e.SymlinkTarget)
					failed++
					continue
				}
			}

			// Create symlink from target to central mount subpath
			centralSubPath := filepath.Join(CentralMountPath(), e.SubPath)
			if err := os.Symlink(centralSubPath, e.SymlinkTarget); err != nil {
				log.Printf("restoreSavedMounts: failed to create symlink for %s: %v", e.SymlinkTarget, err)
				failed++
				continue
			}
		}

		log.Printf("restoreSavedMounts: restored mount %s -> %s", e.DataFile, e.SymlinkTarget)
		restored++
	}

	log.Printf("restoreSavedMounts: restored %d mounts, %d failed", restored, failed)
	return nil
}

// stopCentralMount stops the central metadata mount
func (d *Daemon) stopCentralMount() {
	mountPath := CentralMountPath()

	if d.centralServer != nil {
		// Unmount FIRST while the NFS server is still alive — this is fastest
		// because the kernel NFS client can communicate with the server during
		// unmount. If we shut down the server first, the kernel blocks for
		// seconds trying to reach the dead server.
		if err := Unmount(mountPath); err != nil {
			log.Printf("stopCentralMount: graceful unmount failed: %v", err)
		}

		// Now shut down the NFS server (listener close + context cancel)
		d.centralServer.Shutdown()
		d.centralServer = nil
	}

	// Close protected files before closing meta file
	d.closeProtectedFiles()

	if d.centralMetaFile != nil {
		d.centralMetaFile.Close()
		d.centralMetaFile = nil
	}
	d.centralFS = nil

	// Clean up the mount directory if it exists and is empty
	if info, err := os.Stat(mountPath); err == nil && info.IsDir() {
		// Only remove if empty (to avoid accidental data loss)
		entries, err := os.ReadDir(mountPath)
		if err == nil && len(entries) == 0 {
			os.Remove(mountPath)
			log.Printf("stopCentralMount: removed empty mount directory %s", mountPath)
		}
	}
}

// truncateLogFile truncates the log file if it exceeds maxSize bytes.
// It keeps the last half of the file content to preserve recent logs.
func (d *Daemon) truncateLogFile(maxSize int64) error {
	logPath := LogPath()

	info, err := os.Stat(logPath)
	if os.IsNotExist(err) {
		return nil // File doesn't exist, nothing to truncate
	}
	if err != nil {
		return err
	}

	if info.Size() <= maxSize {
		return nil // File is within size limit
	}

	// Read the file
	data, err := os.ReadFile(logPath)
	if err != nil {
		return err
	}

	// Keep the last half of the content (approximately)
	keepSize := len(data) / 2
	startIdx := len(data) - keepSize

	// Find the next newline to avoid cutting a line in the middle
	for i := startIdx; i < len(data); i++ {
		if data[i] == '\n' {
			startIdx = i + 1
			break
		}
	}

	// Write truncated content back
	truncatedData := data[startIdx:]
	header := []byte(fmt.Sprintf("--- Log truncated at %s (kept last %d bytes) ---\n",
		time.Now().Format(time.RFC3339), len(truncatedData)))

	return os.WriteFile(logPath, append(header, truncatedData...), 0600)
}

// acquireOpLock tries to acquire a lock for an operation on a data file.
// Returns an error if another operation is already in progress.
func (d *Daemon) acquireOpLock(dataFile, opType string) error {
	d.inProgressMu.Lock()
	defer d.inProgressMu.Unlock()

	if existingOp, exists := d.inProgressOps[dataFile]; exists {
		return fmt.Errorf("%s already in progress for this data file", existingOp)
	}
	d.inProgressOps[dataFile] = opType
	return nil
}

// releaseOpLock releases the lock for an operation on a data file.
func (d *Daemon) releaseOpLock(dataFile string) {
	d.inProgressMu.Lock()
	defer d.inProgressMu.Unlock()
	delete(d.inProgressOps, dataFile)
}

// handleSave creates a snapshot of the data file
func (d *Daemon) handleSave(req *Request) *Response {
	if req.DataFile == "" {
		return &Response{Success: false, Error: "data_file is required"}
	}

	// Acquire operation lock
	if err := d.acquireOpLock(req.DataFile, "save"); err != nil {
		log.Printf("handleSave: %v", err)
		return &Response{Success: false, Error: err.Error()}
	}
	defer d.releaseOpLock(req.DataFile)

	log.Printf("handleSave: creating snapshot for %s (message=%q, tag=%q)", req.DataFile, req.Message, req.Tag)

	// Try to use mounted DataFile first (single connection, no lock contention)
	var df *storage.DataFile
	var tempConn bool
	var sourceFolder, symlinkTarget string
	if d.centralFS != nil {
		if lfs := d.centralFS.GetVFSByDataFile(req.DataFile); lfs != nil {
			df = lfs.DataFile()
			sourceFolder = lfs.SourceFolder()
			symlinkTarget = lfs.SymlinkTarget()
			log.Printf("handleSave: using mounted DataFile for %s (source=%q)", req.DataFile, sourceFolder)
		}
	}

	// Fall back to temporary connection for unmounted data files
	if df == nil {
		var err error
		df, err = storage.OpenWithContext(req.DataFile, storage.DBContextDaemon)
		if err != nil {
			log.Printf("handleSave: failed to open data file: %v", err)
			return &Response{Success: false, Error: fmt.Sprintf("failed to open data file: %v", err)}
		}
		tempConn = true
		sourceFolder = df.GetSourceFolder()
		log.Printf("handleSave: using temporary connection for %s (source=%q)", req.DataFile, sourceFolder)
	}
	if tempConn {
		defer df.Close()
	}

	// Get fork type to determine snapshot behavior
	forkType := df.GetForkType()
	log.Printf("handleSave: fork_type=%q, source=%q, fullSnapshot=%v", forkType, sourceFolder, req.FullSnapshot)

	// Create snapshot based on fork type and flags:
	// - Overlay fork (soft) without --full: save only overlay data (modified files)
	// - Overlay fork (soft) with --full: save overlay + source folder (complete snapshot)
	// - Full fork (hard): save only overlay data (all files already in data file)
	// - No source folder: save only overlay data
	var snapshot *storage.Snapshot
	var skippedFiles []string

	useFullSnapshot := req.FullSnapshot && sourceFolder != ""
	if useFullSnapshot {
		log.Printf("handleSave: creating full snapshot with source folder: %s (allowPartial=%v)", sourceFolder, req.AllowPartial)
		result, err := df.CreateSnapshotWithSource(req.Message, req.Tag, sourceFolder, symlinkTarget, req.AllowPartial)
		if err != nil {
			log.Printf("handleSave: failed to create snapshot with source: %v", err)
			if !req.AllowPartial {
				return &Response{Success: false, Error: fmt.Sprintf("failed to create snapshot: %v\n\nUse --allow-partial to skip unreadable files and continue", err)}
			}
			return &Response{Success: false, Error: fmt.Sprintf("failed to create snapshot: %v", err)}
		}
		snapshot = result.Snapshot
		skippedFiles = result.SkippedFiles
		if len(skippedFiles) > 0 {
			log.Printf("handleSave: %d files skipped due to read errors", len(skippedFiles))
		}
	} else {
		// Save overlay only (default for overlay forks, always for full forks)
		if forkType == storage.ForkTypeSoft && sourceFolder != "" {
			log.Printf("handleSave: overlay fork - saving overlay data only (use --full for complete snapshot)")
		}
		var err error
		snapshot, err = df.CreateSnapshot(req.Message, req.Tag)
		if err != nil {
			log.Printf("handleSave: failed to create snapshot: %v", err)
			return &Response{Success: false, Error: fmt.Sprintf("failed to create snapshot: %v", err)}
		}
	}

	// Determine snapshot type for response:
	// - Full snapshot (--full flag with source) → "full"
	// - Hard fork (all files already in data file) → "full"
	// - Otherwise → "overlay"
	snapshotType := "overlay"
	if useFullSnapshot || forkType == storage.ForkTypeHard {
		snapshotType = "full"
	}

	log.Printf("handleSave: created snapshot %s (type=%s)", snapshot.ID, snapshotType)
	resp := &Response{
		Success:      true,
		SnapshotID:   snapshot.ID,
		SnapshotType: snapshotType,
		SkippedFiles: skippedFiles,
		Message:      fmt.Sprintf("Created snapshot %s", snapshot.ID),
	}
	// Only include SourceFolder in response when doing full snapshot (so CLI can inform user)
	if useFullSnapshot {
		resp.SourceFolder = sourceFolder
	}
	return resp
}

// handleRestore restores from a snapshot
func (d *Daemon) handleRestore(req *Request) *Response {
	if req.DataFile == "" {
		return &Response{Success: false, Error: "data_file is required"}
	}
	if req.SnapshotID == "" && req.Tag == "" {
		return &Response{Success: false, Error: "snapshot_id or tag is required"}
	}

	// Acquire operation lock
	if err := d.acquireOpLock(req.DataFile, "restore"); err != nil {
		log.Printf("handleRestore: %v", err)
		return &Response{Success: false, Error: err.Error()}
	}
	defer d.releaseOpLock(req.DataFile)

	log.Printf("handleRestore: restoring %s (snapshot_id=%q, tag=%q, paths=%v)",
		req.DataFile, req.SnapshotID, req.Tag, req.Paths)

	// Try to use mounted DataFile first (single connection, no lock contention)
	var df *storage.DataFile
	var lfs *latentfs.LatentFS // Keep reference for cache invalidation
	var tempConn bool
	if d.centralFS != nil {
		if lfs = d.centralFS.GetVFSByDataFile(req.DataFile); lfs != nil {
			df = lfs.DataFile()
			log.Printf("handleRestore: using mounted DataFile for %s", req.DataFile)
		}
	}

	// Fall back to temporary connection for unmounted data files
	if df == nil {
		var err error
		df, err = storage.OpenWithContext(req.DataFile, storage.DBContextDaemon)
		if err != nil {
			log.Printf("handleRestore: failed to open data file: %v", err)
			return &Response{Success: false, Error: fmt.Sprintf("failed to open data file: %v", err)}
		}
		tempConn = true
		log.Printf("handleRestore: using temporary connection for %s", req.DataFile)
	}
	if tempConn {
		defer df.Close()
	}

	// Determine snapshot ID (resolve tag if needed)
	snapshotID := req.SnapshotID
	if snapshotID == "" && req.Tag != "" {
		// GetSnapshot handles both ID and tag
		snapshot, err := df.GetSnapshot(req.Tag)
		if err != nil {
			log.Printf("handleRestore: failed to find snapshot by tag: %v", err)
			return &Response{Success: false, Error: fmt.Sprintf("failed to find snapshot by tag: %v", err)}
		}
		snapshotID = snapshot.ID
	}

	// Restore from snapshot
	result, err := df.RestoreFromSnapshot(snapshotID, req.Paths)
	if err != nil {
		log.Printf("handleRestore: failed to restore: %v", err)
		return &Response{Success: false, Error: fmt.Sprintf("failed to restore: %v", err)}
	}

	// Invalidate VFS caches if using mounted DataFile.
	// InvalidateCacheForDataFile calls LatentFS.InvalidateCache() which clears:
	// - attrCache (path → attributes TTL cache)
	// - lookupCache (parentIno, name → childIno)
	// - parentInodeCache (WCC optimization)
	// - refreshes epoch state from database
	if d.centralFS != nil && lfs != nil {
		if err := d.centralFS.InvalidateCacheForDataFile(req.DataFile); err != nil {
			log.Printf("handleRestore: warning: failed to invalidate cache: %v", err)
		} else {
			log.Printf("handleRestore: invalidated VFS cache for %s", req.DataFile)
		}
	}

	// Sleep for 1 second to ensure NFS client cache expires.
	// The restore operation bumps parent directory mtime with unixepoch()+1,
	// so we need to wait for that future timestamp to become "past" before
	// returning, otherwise NFS clients might not see the mtime change.
	time.Sleep(1 * time.Second)

	log.Printf("handleRestore: restored %d paths from snapshot %s", len(result.RestoredPaths), result.SnapshotID)
	return &Response{
		Success:       true,
		SnapshotID:    result.SnapshotID,
		RestoredPaths: result.RestoredPaths,
		Message:       fmt.Sprintf("Restored from snapshot %s", result.SnapshotID),
	}
}

// handleGC runs garbage collection on the data file
func (d *Daemon) handleGC(req *Request) *Response {
	if req.DataFile == "" {
		return &Response{Success: false, Error: "data_file is required"}
	}

	// Acquire operation lock (even for stats-only to ensure consistency)
	if err := d.acquireOpLock(req.DataFile, "gc"); err != nil {
		log.Printf("handleGC: %v", err)
		return &Response{Success: false, Error: err.Error()}
	}
	defer d.releaseOpLock(req.DataFile)

	log.Printf("handleGC: %s for %s (vacuum=%v, statsOnly=%v)",
		map[bool]string{true: "getting stats", false: "running garbage collection"}[req.StatsOnly],
		req.DataFile, req.Vacuum, req.StatsOnly)

	// Try to use mounted DataFile first (single connection, no lock contention)
	var df *storage.DataFile
	var tempConn bool
	if d.centralFS != nil {
		if lfs := d.centralFS.GetVFSByDataFile(req.DataFile); lfs != nil {
			df = lfs.DataFile()
			log.Printf("handleGC: using mounted DataFile for %s", req.DataFile)
		}
	}

	// Fall back to temporary connection for unmounted data files
	if df == nil {
		var err error
		df, err = storage.OpenWithContext(req.DataFile, storage.DBContextDaemon)
		if err != nil {
			log.Printf("handleGC: failed to open data file: %v", err)
			return &Response{Success: false, Error: fmt.Sprintf("failed to open data file: %v", err)}
		}
		tempConn = true
		log.Printf("handleGC: using temporary connection for %s", req.DataFile)
	}
	if tempConn {
		defer df.Close()
	}

	// If stats-only, just return stats without running GC
	if req.StatsOnly {
		stats, err := df.GetStorageStats()
		if err != nil {
			log.Printf("handleGC: failed to get storage stats: %v", err)
			return &Response{Success: false, Error: fmt.Sprintf("failed to get storage stats: %v", err)}
		}
		log.Printf("handleGC: stats - %d snapshots, %d content blocks, %d orphaned blocks",
			stats.SnapshotCount, stats.TotalContentBlocks, stats.OrphanedContentBlocks)
		return &Response{
			Success: true,
			Stats: &StorageStats{
				TotalContentBlocks:      stats.TotalContentBlocks,
				TotalContentBlocksBytes: stats.TotalContentBlocksBytes,
				SnapshotCount:           stats.SnapshotCount,
				OrphanedContentBlocks:   stats.OrphanedContentBlocks,
				OrphanedBlocksBytes:     stats.OrphanedBlocksBytes,
				DeprecatedEpochCount:    stats.DeprecatedEpochCount,
			},
			Message: fmt.Sprintf("Stats: %d snapshots, %d content blocks", stats.SnapshotCount, stats.TotalContentBlocks),
		}
	}

	// Run garbage collection
	result, err := df.GarbageCollect()
	if err != nil {
		log.Printf("handleGC: failed to garbage collect: %v", err)
		return &Response{Success: false, Error: fmt.Sprintf("failed to garbage collect: %v", err)}
	}

	// Run VACUUM if requested
	if req.Vacuum {
		if _, err := df.DB().Exec("VACUUM"); err != nil {
			log.Printf("handleGC: vacuum failed: %v", err)
			// Don't fail the whole operation, just log the error
		}
	}

	log.Printf("handleGC: cleaned %d blocks (%d bytes), %d epochs",
		result.OrphanedContentBlocks, result.OrphanedBlocksBytes, result.DeprecatedEpochs)
	return &Response{
		Success: true,
		GCResult: &GCResult{
			OrphanedContentBlocks: result.OrphanedContentBlocks,
			OrphanedBlocksBytes:   result.OrphanedBlocksBytes,
			DeprecatedEpochs:      result.DeprecatedEpochs,
		},
		Message: fmt.Sprintf("Cleaned %d blocks (%d bytes), %d epochs",
			result.OrphanedContentBlocks, result.OrphanedBlocksBytes, result.DeprecatedEpochs),
	}
}

// handleListSnapshots lists all snapshots in the data file
func (d *Daemon) handleListSnapshots(req *Request) *Response {
	if req.DataFile == "" {
		return &Response{Success: false, Error: "data_file is required"}
	}

	log.Printf("handleListSnapshots: listing snapshots for %s", req.DataFile)

	// Try to use mounted DataFile first (single connection, no lock contention)
	var df *storage.DataFile
	var tempConn bool
	if d.centralFS != nil {
		if lfs := d.centralFS.GetVFSByDataFile(req.DataFile); lfs != nil {
			df = lfs.DataFile()
			log.Printf("handleListSnapshots: using mounted DataFile for %s", req.DataFile)
		}
	}

	// Fall back to temporary connection for unmounted data files
	if df == nil {
		var err error
		df, err = storage.OpenWithContext(req.DataFile, storage.DBContextDaemon)
		if err != nil {
			log.Printf("handleListSnapshots: failed to open data file: %v", err)
			return &Response{Success: false, Error: fmt.Sprintf("failed to open data file: %v", err)}
		}
		tempConn = true
		log.Printf("handleListSnapshots: using temporary connection for %s", req.DataFile)
	}
	if tempConn {
		defer df.Close()
	}

	// List snapshots
	snapshots, err := df.ListSnapshots()
	if err != nil {
		log.Printf("handleListSnapshots: failed to list snapshots: %v", err)
		return &Response{Success: false, Error: fmt.Sprintf("failed to list snapshots: %v", err)}
	}

	// Convert to IPC format
	snapshotInfos := make([]SnapshotInfo, len(snapshots))
	for i, s := range snapshots {
		snapshotInfos[i] = SnapshotInfo{
			ID:        s.ID,
			Message:   s.Message,
			Tag:       s.Tag,
			CreatedAt: s.CreatedAt.Unix(),
			FileCount: s.FileCount,
			TotalSize: s.TotalSize,
		}
	}

	log.Printf("handleListSnapshots: found %d snapshots", len(snapshots))
	return &Response{
		Success:   true,
		Snapshots: snapshotInfos,
		Message:   fmt.Sprintf("Found %d snapshots", len(snapshots)),
	}
}

// handleDeleteSnapshot deletes a snapshot from the data file
func (d *Daemon) handleDeleteSnapshot(req *Request) *Response {
	if req.DataFile == "" {
		return &Response{Success: false, Error: "data_file is required"}
	}
	if req.SnapshotID == "" && req.Tag == "" {
		return &Response{Success: false, Error: "snapshot_id or tag is required"}
	}

	idOrTag := req.SnapshotID
	if idOrTag == "" {
		idOrTag = req.Tag
	}

	log.Printf("handleDeleteSnapshot: deleting snapshot %s from %s", idOrTag, req.DataFile)

	// Try to use mounted DataFile first (single connection, no lock contention)
	var df *storage.DataFile
	var tempConn bool
	if d.centralFS != nil {
		if lfs := d.centralFS.GetVFSByDataFile(req.DataFile); lfs != nil {
			df = lfs.DataFile()
			log.Printf("handleDeleteSnapshot: using mounted DataFile for %s", req.DataFile)
		}
	}

	// Fall back to temporary connection for unmounted data files
	if df == nil {
		var err error
		df, err = storage.OpenWithContext(req.DataFile, storage.DBContextDaemon)
		if err != nil {
			log.Printf("handleDeleteSnapshot: failed to open data file: %v", err)
			return &Response{Success: false, Error: fmt.Sprintf("failed to open data file: %v", err)}
		}
		tempConn = true
		log.Printf("handleDeleteSnapshot: using temporary connection for %s", req.DataFile)
	}
	if tempConn {
		defer df.Close()
	}

	// Delete snapshot
	if err := df.DeleteSnapshot(idOrTag); err != nil {
		log.Printf("handleDeleteSnapshot: failed to delete snapshot: %v", err)
		return &Response{Success: false, Error: fmt.Sprintf("failed to delete snapshot: %v", err)}
	}

	log.Printf("handleDeleteSnapshot: deleted snapshot %s", idOrTag)
	return &Response{
		Success: true,
		Message: fmt.Sprintf("Deleted snapshot %s", idOrTag),
	}
}
