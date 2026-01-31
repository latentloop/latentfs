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

package storage

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const SchemaVersion = "1"

const ChunkSize = 16384 // 16KB chunks for file content

// Default busy_timeout in milliseconds (30 seconds)
const DefaultBusyTimeout = 30000

// Environment variable names for busy_timeout configuration
const (
	// EnvBusyTimeout is the general busy_timeout override for all contexts
	EnvBusyTimeout = "LATENTFS_BUSY_TIMEOUT"
	// EnvDaemonBusyTimeout is the busy_timeout for daemon (NFS server) database access
	EnvDaemonBusyTimeout = "LATENTFS_DAEMON_BUSY_TIMEOUT"
	// EnvCLIBusyTimeout is the busy_timeout for CLI database access
	EnvCLIBusyTimeout = "LATENTFS_CLI_BUSY_TIMEOUT"
)

// DBContext indicates the context in which the database is being accessed
type DBContext int

const (
	// DBContextDefault uses the general busy_timeout
	DBContextDefault DBContext = iota
	// DBContextDaemon uses the daemon-specific busy_timeout
	DBContextDaemon
	// DBContextCLI uses the CLI-specific busy_timeout
	DBContextCLI
)

// Package-level config values (set via SetConfigBusyTimeouts)
var (
	configDaemonBusyTimeout int
	configCLIBusyTimeout    int
)

// SetConfigBusyTimeouts sets the config-based busy_timeout values.
// This should be called by daemon/CLI after loading the config file.
// Values of 0 are ignored (use env var or default).
func SetConfigBusyTimeouts(daemonTimeout, cliTimeout int) {
	configDaemonBusyTimeout = daemonTimeout
	configCLIBusyTimeout = cliTimeout
}

// GetBusyTimeout returns the busy_timeout value for the given context.
// Priority: specific env (daemon/cli) > general env > config file > default
func GetBusyTimeout(ctx DBContext) int {
	// Check context-specific env var first
	var specificEnv string
	var configTimeout int
	switch ctx {
	case DBContextDaemon:
		specificEnv = EnvDaemonBusyTimeout
		configTimeout = configDaemonBusyTimeout
	case DBContextCLI:
		specificEnv = EnvCLIBusyTimeout
		configTimeout = configCLIBusyTimeout
	}

	if specificEnv != "" {
		if val := os.Getenv(specificEnv); val != "" {
			if timeout, err := strconv.Atoi(val); err == nil && timeout > 0 {
				return timeout
			}
		}
	}

	// Check general env var
	if val := os.Getenv(EnvBusyTimeout); val != "" {
		if timeout, err := strconv.Atoi(val); err == nil && timeout > 0 {
			return timeout
		}
	}

	// Check config file value
	if configTimeout > 0 {
		return configTimeout
	}

	// Return default
	return DefaultBusyTimeout
}

// BuildDSN builds the SQLite DSN with the appropriate busy_timeout for the context
func BuildDSN(path string, ctx DBContext) string {
	timeout := GetBusyTimeout(ctx)
	return fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=%d", path, timeout)
}

// File mode constants (POSIX)
const (
	ModeDeleted = 0        // Tombstone - marks deleted source file in soft-fork
	ModeDir     = 0040000  // Directory
	ModeFile    = 0100000  // Regular file
	ModeSymlink = 0120000  // Symbolic link
	ModeMask    = 0170000  // Type mask
)

// Default permissions
const (
	DefaultDirMode  = ModeDir | 0755  // rwxr-xr-x
	DefaultFileMode = ModeFile | 0644 // rw-r--r--
)

// Root inode number
const RootIno = 1

// Schema SQL for data file
const dataFileSchema = `
-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_info (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Write epoch management for MVCC-like versioning
CREATE TABLE IF NOT EXISTS write_epochs (
    epoch INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'ready', 'deprecated')),
    created_by TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Config table for epoch tracking
CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- File/directory metadata (inode table) with write_epoch for MVCC
CREATE TABLE IF NOT EXISTS inodes (
    ino INTEGER NOT NULL,
    write_epoch INTEGER NOT NULL DEFAULT 0,
    mode INTEGER NOT NULL,
    uid INTEGER NOT NULL DEFAULT 0,
    gid INTEGER NOT NULL DEFAULT 0,
    size INTEGER NOT NULL DEFAULT 0,
    atime INTEGER NOT NULL,
    mtime INTEGER NOT NULL,
    ctime INTEGER NOT NULL,
    nlink INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (ino, write_epoch)
);

-- Index for efficient epoch-based inode queries
CREATE INDEX IF NOT EXISTS idx_inodes_epoch ON inodes(ino, write_epoch DESC);

-- Index for efficient tombstone detection (mode=0 means tombstone)
-- Helps queries that check for inode tombstones: WHERE ino > 0 AND mode = 0
CREATE INDEX IF NOT EXISTS idx_inodes_tombstone ON inodes(ino, mode, write_epoch DESC);

-- Directory entries with write_epoch for MVCC
CREATE TABLE IF NOT EXISTS dentries (
    parent_ino INTEGER NOT NULL,
    name TEXT NOT NULL,
    ino INTEGER NOT NULL,
    write_epoch INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (parent_ino, name, write_epoch)
);

-- Indexes for directory lookups
CREATE INDEX IF NOT EXISTS idx_dentries_parent ON dentries(parent_ino);
CREATE INDEX IF NOT EXISTS idx_dentries_epoch ON dentries(parent_ino, name, write_epoch DESC);

-- Index for efficient tombstone queries (ino=0 for dentry tombstone, ino=-1 for anti-tombstone)
-- Helps ListDentryTombstoneNames, ListAllTombstoneNames, and FindTombstonesToOverride
CREATE INDEX IF NOT EXISTS idx_dentries_tombstone ON dentries(parent_ino, ino, write_epoch DESC);

-- File content storage (chunked) with write_epoch for MVCC
CREATE TABLE IF NOT EXISTS content (
    ino INTEGER NOT NULL,
    chunk_idx INTEGER NOT NULL,
    data BLOB NOT NULL,
    write_epoch INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (ino, chunk_idx, write_epoch)
);

-- Index for efficient epoch-based content queries
CREATE INDEX IF NOT EXISTS idx_content_epoch ON content(ino, chunk_idx, write_epoch DESC);

-- Symbolic link targets with write_epoch for MVCC
CREATE TABLE IF NOT EXISTS symlinks (
    ino INTEGER NOT NULL,
    target TEXT NOT NULL,
    write_epoch INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (ino, write_epoch)
);

-- Index for efficient epoch-based symlink queries
CREATE INDEX IF NOT EXISTS idx_symlinks_epoch ON symlinks(ino, write_epoch DESC);

-- Snapshots metadata
CREATE TABLE IF NOT EXISTS snapshots (
    id TEXT PRIMARY KEY,
    message TEXT,
    tag TEXT UNIQUE,
    created_at INTEGER NOT NULL,
    file_count INTEGER NOT NULL DEFAULT 0,
    total_size INTEGER NOT NULL DEFAULT 0,
    source_epoch INTEGER  -- Epoch at which snapshot was taken (for overlay restore)
);

-- Snapshot inodes (copy of inodes at snapshot time)
CREATE TABLE IF NOT EXISTS snapshot_inodes (
    snapshot_id TEXT NOT NULL,
    ino INTEGER NOT NULL,
    mode INTEGER NOT NULL,
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    size INTEGER NOT NULL,
    atime INTEGER NOT NULL,
    mtime INTEGER NOT NULL,
    ctime INTEGER NOT NULL,
    nlink INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, ino),
    FOREIGN KEY (snapshot_id) REFERENCES snapshots(id)
);

-- Snapshot dentries (copy of dentries at snapshot time)
CREATE TABLE IF NOT EXISTS snapshot_dentries (
    snapshot_id TEXT NOT NULL,
    parent_ino INTEGER NOT NULL,
    name TEXT NOT NULL,
    ino INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, parent_ino, name),
    FOREIGN KEY (snapshot_id) REFERENCES snapshots(id)
);

-- Content-addressable storage for deduplicated snapshot content
CREATE TABLE IF NOT EXISTS content_blocks (
    hash TEXT PRIMARY KEY,
    data BLOB NOT NULL
);

-- Snapshot content (references shared content blocks by hash)
CREATE TABLE IF NOT EXISTS snapshot_content (
    snapshot_id TEXT NOT NULL,
    ino INTEGER NOT NULL,
    chunk_idx INTEGER NOT NULL,
    hash TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, ino, chunk_idx),
    FOREIGN KEY (snapshot_id) REFERENCES snapshots(id),
    FOREIGN KEY (hash) REFERENCES content_blocks(hash)
);

-- Snapshot symlinks (copy of symlinks at snapshot time)
CREATE TABLE IF NOT EXISTS snapshot_symlinks (
    snapshot_id TEXT NOT NULL,
    ino INTEGER NOT NULL,
    target TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, ino),
    FOREIGN KEY (snapshot_id) REFERENCES snapshots(id)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_tag ON snapshots(tag);
`

// Initial data for root directory
const initRootDir = `
INSERT OR IGNORE INTO schema_info (key, value) VALUES ('version', ?);
INSERT OR IGNORE INTO schema_info (key, value) VALUES ('type', 'data');
INSERT OR IGNORE INTO schema_info (key, value) VALUES ('created_at', datetime('now'));

-- Initialize write epoch 0 as ready (for initial state)
INSERT OR IGNORE INTO write_epochs (epoch, status, created_by, created_at, updated_at)
VALUES (0, 'ready', 'system:init', unixepoch(), unixepoch());

-- Set current write epoch to 0
INSERT OR IGNORE INTO config (key, value) VALUES ('current_write_epoch', '0');

-- Root directory inode (ino=1, mode=0040755, write_epoch=0)
INSERT OR IGNORE INTO inodes (ino, write_epoch, mode, uid, gid, size, atime, mtime, ctime, nlink)
VALUES (1, 0, ?, 0, 0, 0, unixepoch(), unixepoch(), unixepoch(), 2);
`

// Schema SQL for meta file
const metaFileSchema = `
CREATE TABLE IF NOT EXISTS schema_info (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sub_path TEXT NOT NULL UNIQUE,
    data_file TEXT NOT NULL,
    source_folder TEXT,
    fork_type TEXT,
    symlink_target TEXT,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mounts_path ON mounts(sub_path);

CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS protected_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data_file TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_protected_files_data_file ON protected_files(data_file);
`

const initMetaFile = `
INSERT OR IGNORE INTO schema_info (key, value) VALUES ('version', ?);
INSERT OR IGNORE INTO schema_info (key, value) VALUES ('type', 'meta');
INSERT OR IGNORE INTO schema_info (key, value) VALUES ('created_at', datetime('now'));
`

// execStatements executes multiple SQL statements separated by semicolons.
// libsql driver doesn't support multi-statement Exec, so we split and execute individually.
func execStatements(db *sql.DB, sqlScript string, args ...interface{}) error {
	statements := splitStatements(sqlScript)
	argIdx := 0
	for _, stmt := range statements {
		if stmt == "" {
			continue
		}
		// Count placeholders in this statement
		placeholders := strings.Count(stmt, "?")
		stmtArgs := args[argIdx : argIdx+placeholders]
		argIdx += placeholders
		if _, err := db.Exec(stmt, stmtArgs...); err != nil {
			return err
		}
	}
	return nil
}

// splitStatements splits a SQL script into individual statements
func splitStatements(script string) []string {
	var statements []string
	var current strings.Builder

	lines := strings.Split(script, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Skip comments and empty lines
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		current.WriteString(line)
		current.WriteString("\n")
		if strings.HasSuffix(trimmed, ";") {
			statements = append(statements, strings.TrimSpace(current.String()))
			current.Reset()
		}
	}
	// Handle any remaining content
	if current.Len() > 0 {
		stmt := strings.TrimSpace(current.String())
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}
	return statements
}
