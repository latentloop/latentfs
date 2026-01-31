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
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/tursodatabase/go-libsql"
	"github.com/uptrace/bun"

	"latentfs/internal/cache"
	"latentfs/internal/common"
)

// lookupCacheKey is a cache key for Lookup results.
type lookupCacheKey struct {
	parentIno int64
	name      string
}

// DataFile represents a SQLite-backed latentfs data file
type DataFile struct {
	path              string
	db                *sql.DB
	bunDB             *BunDB
	currentWriteEpoch int64 // Current epoch for writes
	readableEpoch     int64 // Max ready epoch for reads
	lastDataVersion   int64 // Last known PRAGMA data_version for external change detection

	// O1: Rate-limit CheckForExternalChanges to avoid per-VFS-call PRAGMA overhead.
	lastExternalCheck time.Time

	// lookupCache caches Lookup(parentIno, name) → Dentry results at readableEpoch.
	// Invalidated by: RefreshEpochs (epoch change), and targeted invalidation in
	// dentry mutation functions (CreateDentry, DeleteDentry, RenameDentry, etc.).
	// This eliminates repeated parent-directory resolutions during rm -rf.
	lookupCache map[lookupCacheKey]*Dentry

	// O3: Single-entry cache for parent inode, used by pre-WCC in UnlinkByPathWithAttrs.
	// For rm -rf of siblings in the same directory, the parent inode is fetched once
	// and reused for all subsequent siblings. Invalidated on epoch change and cache clear.
	cachedParentIno   int64
	cachedParentInode *Inode
}

// execPragma runs a PRAGMA statement using Query (not Exec) because libsql
// returns rows for PRAGMA statements. The result rows are drained and closed.
func execPragma(db *sql.DB, pragma string) error {
	rows, err := db.Query(pragma)
	if err != nil {
		return err
	}
	rows.Close()
	return nil
}

// applyPragmas sets essential PRAGMAs after opening a libsql connection.
// libsql ignores DSN-based _pragma=value parameters, so all PRAGMAs must be
// set explicitly via SQL statements after the connection is opened.
func applyPragmas(db *sql.DB, ctx DBContext) error {
	// Busy timeout MUST be set first — all subsequent PRAGMAs (especially
	// journal_mode=WAL which needs exclusive access) will wait for locks
	// instead of failing immediately with "database is locked".
	busyTimeout := GetBusyTimeout(ctx)
	if err := execPragma(db, fmt.Sprintf("PRAGMA busy_timeout = %d", busyTimeout)); err != nil {
		return fmt.Errorf("failed to set busy_timeout: %w", err)
	}

	// WAL mode: enables concurrent readers during writes, reduces lock contention.
	// Must be set via explicit PRAGMA — libsql ignores _journal_mode in DSN.
	// journal_mode conversion requires exclusive file access; with busy_timeout
	// set above, this will wait rather than fail on transient locks.
	if err := execPragma(db, "PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("failed to set journal_mode=WAL: %w", err)
	}

	// synchronous=NORMAL: WAL mode with NORMAL sync is safe against process crashes
	// (only vulnerable to OS crash / power loss). Avoids fsync on every commit.
	if err := execPragma(db, "PRAGMA synchronous=NORMAL"); err != nil {
		return fmt.Errorf("failed to set synchronous=NORMAL: %w", err)
	}

	// Foreign keys
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	// Larger cache for better read performance (default is ~2MB, set to 8MB).
	if err := execPragma(db, "PRAGMA cache_size = -8000"); err != nil {
		return fmt.Errorf("failed to set cache_size: %w", err)
	}

	// Memory-map I/O for reads (256MB). Reduces read() syscalls for hot data.
	// Failure is non-fatal (may not be supported on all platforms).
	_ = execPragma(db, "PRAGMA mmap_size = 268435456")

	return nil
}

// Create creates a new .latentfs data file with default context
func Create(path string) (*DataFile, error) {
	return CreateWithContext(path, DBContextDefault)
}

// CreateWithContext creates a new .latentfs data file with the specified context.
func CreateWithContext(path string, ctx DBContext) (*DataFile, error) {
	// Check if file already exists
	if _, err := os.Stat(path); err == nil {
		return nil, fmt.Errorf("file already exists: %s", path)
	}

	// Create SQLite database
	db, err := sql.Open("libsql", BuildDSN(path, ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	// Apply all PRAGMAs (WAL, synchronous, foreign keys, busy_timeout, cache, mmap).
	// Must be explicit — libsql ignores DSN-based _pragma=value parameters.
	if err := applyPragmas(db, ctx); err != nil {
		db.Close()
		os.Remove(path)
		return nil, err
	}

	// Create schema (execute statements individually for libsql compatibility)
	if err := execStatements(db, dataFileSchema); err != nil {
		db.Close()
		os.Remove(path)
		return nil, fmt.Errorf("failed to create schema: %w", err)
	}

	// Initialize root directory
	if err := execStatements(db, initRootDir, SchemaVersion, DefaultDirMode); err != nil {
		db.Close()
		os.Remove(path)
		return nil, fmt.Errorf("failed to initialize root: %w", err)
	}

	df := &DataFile{
		path:  path,
		db:    db,
		bunDB: NewBunDB(db),
	}
	// Initialize lastDataVersion for external change detection
	df.lastDataVersion, _ = df.getDataVersion()
	return df, nil
}

// Open opens an existing .latentfs data file with default context
func Open(path string) (*DataFile, error) {
	return OpenWithContext(path, DBContextDefault)
}

// OpenWithContext opens an existing .latentfs data file with the specified context.
func OpenWithContext(path string, ctx DBContext) (*DataFile, error) {
	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	// Open SQLite database
	db, err := sql.Open("libsql", BuildDSN(path, ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Apply all PRAGMAs (WAL, synchronous, foreign keys, busy_timeout, cache, mmap).
	// Must be explicit — libsql ignores DSN-based _pragma=value parameters.
	if err := applyPragmas(db, ctx); err != nil {
		db.Close()
		return nil, err
	}

	bunDB := NewBunDB(db)
	bgCtx := context.Background()

	// Verify it's a data file
	fileType, err := bunDB.GetSchemaInfo(bgCtx, "type")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to read schema info: %w", err)
	}
	if fileType != "data" {
		db.Close()
		return nil, fmt.Errorf("not a data file (type=%s)", fileType)
	}

	df := &DataFile{
		path:  path,
		db:    db,
		bunDB: bunDB,
	}

	// Initialize epoch fields
	if err := df.RefreshEpochs(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize epochs: %w", err)
	}

	// Initialize lastDataVersion for external change detection
	df.lastDataVersion, _ = df.getDataVersion()

	return df, nil
}

// Close closes the database connection and cleans up WAL files.
// It performs a TRUNCATE checkpoint to merge WAL data into the main database,
// then removes the -wal and -shm files.
func (df *DataFile) Close() error {
	if df.db == nil {
		return nil
	}

	// Checkpoint WAL to merge all transactions into main database and truncate WAL
	// TRUNCATE mode: checkpoint and then truncate the WAL file to zero bytes
	// Note: PRAGMA wal_checkpoint returns rows, so we must use Query() not Exec()
	rows, err := df.db.Query("PRAGMA wal_checkpoint(TRUNCATE)")
	if err != nil {
		// Log but don't fail - the close is more important
		log.Printf("warning: WAL checkpoint failed: %v", err)
	} else {
		rows.Close()
	}

	// Close the database connection
	if err := df.db.Close(); err != nil {
		return err
	}

	// Remove WAL and SHM files if they exist
	walPath := df.path + "-wal"
	shmPath := df.path + "-shm"
	os.Remove(walPath) // Ignore errors - files may not exist
	os.Remove(shmPath)

	return nil
}

// Path returns the file path
func (df *DataFile) Path() string {
	return df.path
}

// DB returns the underlying *sql.DB for use with Bun or other wrappers.
func (df *DataFile) DB() *sql.DB {
	return df.db
}

// BunDB returns the Bun database wrapper.
func (df *DataFile) BunDB() *BunDB {
	return df.bunDB
}

// --- Epoch Operations ---

// RefreshEpochs reloads the epoch values from the database.
// Clears the lookup cache since readableEpoch may have changed.
func (df *DataFile) RefreshEpochs() error {
	ctx := context.Background()

	// Get max ready epoch for reads
	readableEpoch, err := df.bunDB.GetMaxReadyEpoch(ctx)
	if err != nil {
		return fmt.Errorf("failed to get readable epoch: %w", err)
	}
	log.Printf("[DataFile] RefreshEpochs: old=%d new=%d", df.readableEpoch, readableEpoch)
	df.readableEpoch = readableEpoch

	// Get current write epoch from config
	currentEpochStr, err := df.bunDB.GetConfigValue(ctx, "current_write_epoch")
	if err != nil {
		return fmt.Errorf("failed to get current write epoch: %w", err)
	}
	if currentEpochStr == "" {
		df.currentWriteEpoch = 0
	} else {
		fmt.Sscanf(currentEpochStr, "%d", &df.currentWriteEpoch)
	}

	// Invalidate caches — readableEpoch may have changed,
	// making all cached entries potentially stale.
	df.lookupCache = nil
	df.InvalidateParentInodeCache()

	return nil
}

// GetCurrentWriteEpoch returns the current write epoch
func (df *DataFile) GetCurrentWriteEpoch() int64 {
	return df.currentWriteEpoch
}

// getDataVersion returns the current PRAGMA data_version value.
// data_version increments whenever another connection commits changes to the database.
// This is used to detect external modifications (e.g., CLI restore while daemon is running).
func (df *DataFile) getDataVersion() (int64, error) {
	var version int64
	err := df.db.QueryRow("PRAGMA data_version").Scan(&version)
	return version, err
}

// CheckForExternalChanges checks if another process has modified the database.
// If changes are detected (via PRAGMA data_version), it refreshes the epoch cache.
// Returns true if external changes were detected and epochs were refreshed.
// This provides a self-healing mechanism for cache coherency without requiring IPC.
//
// DISABLED in fs.go: All DataFile modifications now go through daemon (NFS or IPC).
// IPC handlers (e.g., handleRestore in daemon.go) call InvalidateCacheForDataFile
// for immediate cache invalidation, making this polling mechanism redundant.
// The function is kept for potential debugging/edge cases but is not called.
//
// O1: Rate-limited to at most once per 50ms. During bulk operations (rm -rf, cp -r),
// the daemon is the sole writer, so external changes are rare. This saves ~1 PRAGMA
// query per VFS call within the rate-limit window. CLI-initiated changes (e.g.,
// `data restore`) use IPC cache invalidation for immediate detection, so the rate
// limit only needs to be short enough for defensive polling.
func (df *DataFile) CheckForExternalChanges() bool {
	now := time.Now()
	if now.Sub(df.lastExternalCheck) < 50*time.Millisecond {
		return false
	}
	df.lastExternalCheck = now

	version, err := df.getDataVersion()
	if err != nil {
		return false
	}

	if version != df.lastDataVersion {
		df.lastDataVersion = version
		df.RefreshEpochs() // also clears lookupCache
		return true
	}
	return false
}

// GetReadableEpoch returns the max ready epoch for reads
func (df *DataFile) GetReadableEpoch() int64 {
	return df.readableEpoch
}

// CreateWriteEpoch creates a new epoch with given status and returns its epoch number
func (df *DataFile) CreateWriteEpoch(status, createdBy string) (int64, error) {
	ctx := context.Background()
	return df.bunDB.CreateWriteEpoch(ctx, status, createdBy)
}

// SetEpochStatus updates an epoch's status
func (df *DataFile) SetEpochStatus(epoch int64, status string) error {
	ctx := context.Background()
	return df.bunDB.UpdateEpochStatus(ctx, epoch, status)
}

// SetCurrentWriteEpoch updates the current write epoch in config and refreshes local cache
func (df *DataFile) SetCurrentWriteEpoch(epoch int64) error {
	ctx := context.Background()
	err := df.bunDB.SetConfigValue(ctx, "current_write_epoch", fmt.Sprintf("%d", epoch))
	if err != nil {
		return fmt.Errorf("failed to set current write epoch: %w", err)
	}
	df.currentWriteEpoch = epoch
	return nil
}

// CleanupDeprecatedEpochs deletes data from deprecated epochs
func (df *DataFile) CleanupDeprecatedEpochs() (int, error) {
	ctx := context.Background()

	epochs, err := df.bunDB.GetDeprecatedEpochs(ctx)
	if err != nil {
		return 0, err
	}

	cleanedCount := 0
	for _, epoch := range epochs {
		if err := df.bunDB.DeleteEpochData(ctx, epoch); err != nil {
			return cleanedCount, err
		}
		cleanedCount++
	}

	return cleanedCount, nil
}

// GarbageCollectResult holds statistics from a garbage collection run
type GarbageCollectResult struct {
	OrphanedContentBlocks int   // content_blocks deleted
	OrphanedBlocksBytes   int64 // bytes freed from content_blocks
	DeprecatedEpochs      int   // deprecated epochs cleaned
}

// GarbageCollect performs comprehensive storage cleanup.
// Returns statistics about what was cleaned.
func (df *DataFile) GarbageCollect() (*GarbageCollectResult, error) {
	ctx := context.Background()
	result := &GarbageCollectResult{}

	// 1. Clean orphaned content blocks (snapshot data)
	count, bytes, err := df.bunDB.GarbageCollectContentBlocks(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to clean content blocks: %w", err)
	}
	result.OrphanedContentBlocks = count
	result.OrphanedBlocksBytes = bytes

	// 2. Clean deprecated epochs (live data MVCC cleanup)
	epochCount, err := df.CleanupDeprecatedEpochs()
	if err != nil {
		return nil, fmt.Errorf("failed to clean deprecated epochs: %w", err)
	}
	result.DeprecatedEpochs = epochCount

	return result, nil
}

// GetStorageStats returns storage statistics for the data file.
func (df *DataFile) GetStorageStats() (*StorageStats, error) {
	ctx := context.Background()
	return df.bunDB.GetStorageStats(ctx)
}

// --- Inode Operations ---

// GetInode retrieves an inode by number (latest version at or before readable epoch)
// Returns ErrNotFound if inode doesn't exist or is a tombstone (mode=0)
func (df *DataFile) GetInode(ino int64) (*Inode, error) {
	ctx := context.Background()
	model, err := df.bunDB.GetInode(ctx, ino, df.readableEpoch)
	if err != nil {
		return nil, err
	}
	return model.ToInode(), nil
}

// GetInodeCachedForParent retrieves an inode, using a single-entry cache optimized
// for the parent-inode pre-WCC pattern during rm -rf. When deleting siblings in
// the same directory, the parent inode is the same for all files — this avoids
// re-fetching it from the DB for each sibling. (O3 optimization)
// Bypasses cache when LATENTFS_CACHE=0.
func (df *DataFile) GetInodeCachedForParent(ino int64) (*Inode, error) {
	// Bypass cache entirely when disabled
	if cache.Disabled {
		return df.GetInode(ino)
	}

	if df.cachedParentIno == ino && df.cachedParentInode != nil {
		return df.cachedParentInode, nil
	}
	inode, err := df.GetInode(ino)
	if err != nil {
		return nil, err
	}
	df.cachedParentIno = ino
	df.cachedParentInode = inode
	return inode, nil
}

// SetCachedParentInode updates the single-entry parent inode cache with a
// known-good value. Used after synthetic post-WCC computation to propagate
// updated mtime/ctime/nlink to the next sibling's pre-WCC lookup. (O3)
func (df *DataFile) SetCachedParentInode(ino int64, inode *Inode) {
	df.cachedParentIno = ino
	df.cachedParentInode = inode
}

// InvalidateParentInodeCache clears the single-entry parent inode cache.
// Called after mutations that change the parent inode (e.g., delete child dir).
func (df *DataFile) InvalidateParentInodeCache() {
	df.cachedParentIno = 0
	df.cachedParentInode = nil
}

// InvalidateCache clears all internal caches (lookupCache and parentInodeCache).
// Called by LatentFS.InvalidateCache() to ensure complete cache invalidation.
func (df *DataFile) InvalidateCache() {
	df.lookupCache = nil
	df.InvalidateParentInodeCache()
}

// CreateInode creates a new inode with the given mode at current write epoch
func (df *DataFile) CreateInode(mode uint32) (int64, error) {
	ctx := context.Background()
	now := time.Now().Unix()
	uid := os.Getuid()
	gid := os.Getgid()

	// Get next ino value
	maxIno, _ := df.bunDB.GetMaxIno(ctx)
	ino := maxIno + 1
	if ino < 2 {
		ino = 2 // Start at 2 (1 is root)
	}

	err := df.bunDB.InsertInode(ctx, &InodeModel{
		Ino:        ino,
		WriteEpoch: df.currentWriteEpoch,
		Mode:       int64(mode),
		UID:        int64(uid),
		GID:        int64(gid),
		Size:       0,
		Atime:      now,
		Mtime:      now,
		Ctime:      now,
		Nlink:      1,
	})
	if err != nil {
		return 0, err
	}

	return ino, nil
}

// UpdateInode updates inode fields by creating a new version at current epoch
func (df *DataFile) UpdateInode(ino int64, updates *InodeUpdate) error {
	if updates == nil {
		return nil
	}

	ctx := context.Background()

	// Get current inode state
	current, err := df.GetInode(ino)
	if err != nil {
		return err
	}

	// Apply updates to create new version
	mode := current.Mode
	uid := current.Uid
	gid := current.Gid
	size := current.Size
	atime := current.Atime.Unix()
	mtime := current.Mtime.Unix()
	nlink := current.Nlink

	if updates.Mode != nil {
		mode = *updates.Mode
	}
	if updates.Uid != nil {
		uid = *updates.Uid
	}
	if updates.Gid != nil {
		gid = *updates.Gid
	}
	if updates.Size != nil {
		size = *updates.Size
	}
	if updates.Atime != nil {
		atime = updates.Atime.Unix()
	}
	if updates.Mtime != nil {
		mtime = updates.Mtime.Unix()
	}

	ctime := time.Now().Unix()

	return df.bunDB.UpsertInode(ctx, &InodeModel{
		Ino:        ino,
		WriteEpoch: df.currentWriteEpoch,
		Mode:       int64(mode),
		UID:        int64(uid),
		GID:        int64(gid),
		Size:       size,
		Atime:      atime,
		Mtime:      mtime,
		Ctime:      ctime,
		Nlink:      int64(nlink),
	})
}

// DeleteInode creates a tombstone (mode=0) for the inode at current epoch
func (df *DataFile) DeleteInode(ino int64) error {
	ctx := context.Background()
	return df.bunDB.InsertInodeTombstone(ctx, ino, df.currentWriteEpoch)
}

// invalidateLookupCache removes a specific entry from the lookup cache.
// Must be called whenever a dentry is created, deleted, or renamed.
func (df *DataFile) invalidateLookupCache(parentIno int64, name string) {
	if df.lookupCache != nil {
		delete(df.lookupCache, lookupCacheKey{parentIno, name})
	}
}

// LookupWithInode combines Lookup + GetInode into a single SQL query (O2 optimization).
// Returns both the dentry and the child inode in one DB round-trip.
func (df *DataFile) LookupWithInode(parentIno int64, name string) (*Dentry, *Inode, error) {
	ctx := context.Background()
	dentryModel, inodeModel, err := df.bunDB.LookupWithInode(ctx, parentIno, name, df.readableEpoch)
	if err != nil {
		return nil, nil, err
	}
	d := dentryModel.ToDentry()
	// Populate lookup cache with the dentry (reusable by subsequent Lookup calls).
	key := lookupCacheKey{parentIno, name}
	if df.lookupCache == nil {
		df.lookupCache = make(map[lookupCacheKey]*Dentry)
	}
	df.lookupCache[key] = d
	return d, inodeModel.ToInode(), nil
}

// ResolvePathWithParentAndInode resolves a path and returns the child inode,
// parent inode number, and child name — combining ResolvePathWithParent + GetInode(childIno)
// into one fewer DB round-trip for the final path component. (O2 optimization)
func (df *DataFile) ResolvePathWithParentAndInode(path string) (childInode *Inode, parentIno int64, name string, err error) {
	parts := common.SplitPath(path)
	if len(parts) == 0 {
		// Root inode — no parent, need to fetch inode normally.
		inode, err := df.GetInode(RootIno)
		return inode, 0, "", err
	}

	parentIno = int64(RootIno)
	for i, part := range parts {
		if i == len(parts)-1 {
			// Last component: use LookupWithInode to get both dentry and inode.
			_, inode, err := df.LookupWithInode(parentIno, part)
			if err != nil {
				return nil, 0, "", err
			}
			return inode, parentIno, part, nil
		}
		// Intermediate components: normal Lookup (uses lookupCache).
		dentry, err := df.Lookup(parentIno, part)
		if err != nil {
			return nil, 0, "", err
		}
		parentIno = dentry.Ino
	}
	// unreachable
	return nil, 0, "", common.ErrNotFound
}

// --- Directory Operations ---

// Lookup finds a directory entry by name in a parent directory (latest version at readable epoch)
// Returns ErrNotFound if the entry doesn't exist or is a tombstone (ino=0)
func (df *DataFile) Lookup(parentIno int64, name string) (*Dentry, error) {
	// Bypass cache entirely when disabled (LATENTFS_CACHE=0)
	if cache.Disabled {
		ctx := context.Background()
		model, err := df.bunDB.GetDentry(ctx, parentIno, name, df.readableEpoch)
		if err != nil {
			return nil, err
		}
		return model.ToDentry(), nil
	}

	key := lookupCacheKey{parentIno, name}
	if df.lookupCache != nil {
		if d, ok := df.lookupCache[key]; ok {
			return d, nil
		}
	}

	ctx := context.Background()
	model, err := df.bunDB.GetDentry(ctx, parentIno, name, df.readableEpoch)
	if err != nil {
		return nil, err
	}
	d := model.ToDentry()

	if df.lookupCache == nil {
		df.lookupCache = make(map[lookupCacheKey]*Dentry)
	}
	df.lookupCache[key] = d
	return d, nil
}

// ListDir lists all entries in a directory (latest version at readable epoch)
func (df *DataFile) ListDir(parentIno int64) ([]DirEntry, error) {
	ctx := context.Background()
	return df.bunDB.ListDirEntries(ctx, parentIno, df.readableEpoch)
}

// HasLiveChildren checks if a directory has any non-tombstoned children (O6 optimization).
// Uses EXISTS + LIMIT 1 instead of materializing all entries via ListDir.
func (df *DataFile) HasLiveChildren(parentIno int64) (bool, error) {
	ctx := context.Background()
	return df.bunDB.HasLiveChildren(ctx, parentIno, df.readableEpoch)
}

// IsDentryTombstoned checks if a dentry has been explicitly deleted (tombstone ino=0).
// Used to block source-folder fallback for deleted entries in soft-forks.
func (df *DataFile) IsDentryTombstoned(parentIno int64, name string) bool {
	ctx := context.Background()
	return df.bunDB.IsDentryTombstoned(ctx, parentIno, name, df.readableEpoch)
}

// ListDentryTombstoneNames returns names of explicitly deleted entries under parentIno.
// Used by ReadDir to block source folder entries that have overlay dentry tombstones.
func (df *DataFile) ListDentryTombstoneNames(parentIno int64) ([]string, error) {
	ctx := context.Background()
	return df.bunDB.ListDentryTombstoneNames(ctx, parentIno, df.readableEpoch)
}

// CreateDentry creates a directory entry at current write epoch
func (df *DataFile) CreateDentry(parentIno int64, name string, ino int64) error {
	df.invalidateLookupCache(parentIno, name)
	ctx := context.Background()

	err := df.bunDB.UpsertDentry(ctx, &DentryModel{
		ParentIno:  parentIno,
		Name:       name,
		Ino:        ino,
		WriteEpoch: df.currentWriteEpoch,
	})
	if err != nil {
		return err
	}

	// Increment parent's nlink for directories
	inode, err := df.GetInode(ino)
	if err != nil {
		// If inode not found, it's likely a tombstone (ModeDeleted)
		// GetInode returns ErrNotFound for tombstones by design.
		// Tombstones are not directories, so no nlink update needed.
		if err == common.ErrNotFound {
			return nil
		}
		return err
	}
	if inode.IsDir() {
		parent, err := df.GetInode(parentIno)
		if err != nil {
			return err
		}
		now := time.Now().Unix()
		return df.bunDB.UpsertInode(ctx, &InodeModel{
			Ino:        parentIno,
			WriteEpoch: df.currentWriteEpoch,
			Mode:       int64(parent.Mode),
			UID:        int64(parent.Uid),
			GID:        int64(parent.Gid),
			Size:       parent.Size,
			Atime:      parent.Atime.Unix(),
			Mtime:      parent.Mtime.Unix(),
			Ctime:      now,
			Nlink:      int64(parent.Nlink + 1),
		})
	}
	return nil
}

// DeleteDentry marks a directory entry as deleted by creating a tombstone at current epoch
func (df *DataFile) DeleteDentry(parentIno int64, name string) error {
	ctx := context.Background()

	// Get the inode first to check if it's a directory
	dentry, err := df.Lookup(parentIno, name)
	if err != nil {
		return err
	}

	// Get inode to check if it's a directory (for nlink handling)
	// GetInode returns ErrNotFound for tombstone inodes (mode=0) by design,
	// in which case we skip nlink handling (tombstones aren't directories)
	inode, err := df.GetInode(dentry.Ino)
	isTombstoneInode := err == common.ErrNotFound
	if err != nil && !isTombstoneInode {
		return err
	}

	// Create tombstone dentry at current epoch
	err = df.bunDB.InsertDentryTombstone(ctx, parentIno, name, df.currentWriteEpoch)
	if err != nil {
		return err
	}

	// Invalidate cache AFTER mutation (Lookup above may have re-populated cache)
	df.invalidateLookupCache(parentIno, name)

	// Decrement parent's nlink for directories (skip for tombstone inodes)
	if !isTombstoneInode && inode.IsDir() {
		parent, err := df.GetInode(parentIno)
		if err != nil {
			return err
		}
		now := time.Now().Unix()
		return df.bunDB.UpsertInode(ctx, &InodeModel{
			Ino:        parentIno,
			WriteEpoch: df.currentWriteEpoch,
			Mode:       int64(parent.Mode),
			UID:        int64(parent.Uid),
			GID:        int64(parent.Gid),
			Size:       parent.Size,
			Atime:      parent.Atime.Unix(),
			Mtime:      parent.Mtime.Unix(),
			Ctime:      now,
			Nlink:      int64(parent.Nlink - 1),
		})
	}
	return nil
}

// --- Transaction-aware operations (for batching multiple DB ops in one SQLite transaction) ---

// RunInTx wraps the given function in a single SQLite transaction.
// All tx-aware methods called within fn share the same transaction.
func (df *DataFile) RunInTx(ctx context.Context, fn func(ctx context.Context, tx bun.Tx) error) error {
	return df.bunDB.RunInTx(ctx, nil, fn)
}

// DeleteDentryAndInodeTx deletes a dentry and its inode within a transaction.
// Unlike DeleteDentry + DeleteInode, the caller passes childIno and isDir
// to avoid redundant Lookup + GetInode reads (M2 optimization).
func (df *DataFile) DeleteDentryAndInodeTx(ctx context.Context, tx bun.Tx, parentIno int64, name string, childIno int64, isDir bool) error {
	df.invalidateLookupCache(parentIno, name)
	// Insert dentry tombstone
	if err := df.bunDB.InsertDentryTombstoneWith(tx, ctx, parentIno, name, df.currentWriteEpoch); err != nil {
		return err
	}

	// Decrement parent's nlink for directories
	if isDir {
		parent, err := df.bunDB.GetInodeWith(tx, ctx, parentIno, df.readableEpoch)
		if err != nil {
			return err
		}
		now := time.Now().Unix()
		if err := df.bunDB.UpsertInodeWith(tx, ctx, &InodeModel{
			Ino:        parentIno,
			WriteEpoch: df.currentWriteEpoch,
			Mode:       parent.Mode,
			UID:        parent.UID,
			GID:        parent.GID,
			Size:       parent.Size,
			Atime:      parent.Atime,
			Mtime:      parent.Mtime,
			Ctime:      now,
			Nlink:      parent.Nlink - 1,
		}); err != nil {
			return err
		}
	}

	// Insert inode tombstone
	return df.bunDB.InsertInodeTombstoneWith(tx, ctx, childIno, df.currentWriteEpoch)
}

// CreateInodeTx creates a new inode within a transaction.
func (df *DataFile) CreateInodeTx(ctx context.Context, tx bun.Tx, mode uint32) (int64, error) {
	now := time.Now().Unix()
	uid := os.Getuid()
	gid := os.Getgid()

	maxIno, _ := df.bunDB.GetMaxInoWith(tx, ctx)
	ino := maxIno + 1
	if ino < 2 {
		ino = 2
	}

	err := df.bunDB.InsertInodeWith(tx, ctx, &InodeModel{
		Ino:        ino,
		WriteEpoch: df.currentWriteEpoch,
		Mode:       int64(mode),
		UID:        int64(uid),
		GID:        int64(gid),
		Size:       0,
		Atime:      now,
		Mtime:      now,
		Ctime:      now,
		Nlink:      1,
	})
	if err != nil {
		return 0, err
	}
	return ino, nil
}

// CreateDentryTx creates a directory entry within a transaction.
func (df *DataFile) CreateDentryTx(ctx context.Context, tx bun.Tx, parentIno int64, name string, ino int64) error {
	df.invalidateLookupCache(parentIno, name)
	if err := df.bunDB.UpsertDentryWith(tx, ctx, &DentryModel{
		ParentIno:  parentIno,
		Name:       name,
		Ino:        ino,
		WriteEpoch: df.currentWriteEpoch,
	}); err != nil {
		return err
	}

	// Increment parent's nlink for directories
	inode, err := df.bunDB.GetInodeWith(tx, ctx, ino, df.readableEpoch)
	if err != nil {
		if err == common.ErrNotFound {
			return nil // tombstone, no nlink update
		}
		return err
	}
	if uint32(inode.Mode)&ModeMask == ModeDir {
		parent, err := df.bunDB.GetInodeWith(tx, ctx, parentIno, df.readableEpoch)
		if err != nil {
			return err
		}
		now := time.Now().Unix()
		return df.bunDB.UpsertInodeWith(tx, ctx, &InodeModel{
			Ino:        parentIno,
			WriteEpoch: df.currentWriteEpoch,
			Mode:       parent.Mode,
			UID:        parent.UID,
			GID:        parent.GID,
			Size:       parent.Size,
			Atime:      parent.Atime,
			Mtime:      parent.Mtime,
			Ctime:      now,
			Nlink:      parent.Nlink + 1,
		})
	}
	return nil
}

// DeleteInodeTx creates a tombstone for the inode within a transaction.
func (df *DataFile) DeleteInodeTx(ctx context.Context, tx bun.Tx, ino int64) error {
	return df.bunDB.InsertInodeTombstoneWith(tx, ctx, ino, df.currentWriteEpoch)
}

// InsertDentryTombstoneTx creates a dentry tombstone within a transaction.
func (df *DataFile) InsertDentryTombstoneTx(ctx context.Context, tx bun.Tx, parentIno int64, name string) error {
	df.invalidateLookupCache(parentIno, name)
	return df.bunDB.InsertDentryTombstoneWith(tx, ctx, parentIno, name, df.currentWriteEpoch)
}

// LookupTx finds a directory entry within a transaction.
func (df *DataFile) LookupTx(ctx context.Context, tx bun.Tx, parentIno int64, name string) (*Dentry, error) {
	model, err := df.bunDB.GetDentryWith(tx, ctx, parentIno, name, df.readableEpoch)
	if err != nil {
		return nil, err
	}
	return model.ToDentry(), nil
}

// ResolvePathTx resolves a path to an inode number within a transaction.
func (df *DataFile) ResolvePathTx(ctx context.Context, tx bun.Tx, path string) (int64, error) {
	parts := common.SplitPath(path)
	if len(parts) == 0 {
		return RootIno, nil
	}

	currentIno := int64(RootIno)
	for _, part := range parts {
		dentry, err := df.LookupTx(ctx, tx, currentIno, part)
		if err != nil {
			return 0, err
		}
		currentIno = dentry.Ino
	}
	return currentIno, nil
}

// ResolvePathWithParent resolves a path and returns both the child inode and
// the parent inode. For root path, parentIno is 0.
// This avoids a separate ResolvePath(parentPath) call when the caller needs both.
func (df *DataFile) ResolvePathWithParent(path string) (childIno int64, parentIno int64, name string, err error) {
	parts := common.SplitPath(path)
	if len(parts) == 0 {
		return RootIno, 0, "", nil
	}

	parentIno = int64(RootIno)
	for i, part := range parts {
		dentry, err := df.Lookup(parentIno, part)
		if err != nil {
			return 0, 0, "", err
		}
		if i == len(parts)-1 {
			return dentry.Ino, parentIno, part, nil
		}
		parentIno = dentry.Ino
	}
	// unreachable
	return 0, 0, "", common.ErrNotFound
}

// RenameDentry renames/moves a directory entry at current write epoch
func (df *DataFile) RenameDentry(srcParent int64, srcName string, dstParent int64, dstName string) error {
	ctx := context.Background()

	// Get source dentry
	srcDentry, err := df.Lookup(srcParent, srcName)
	if err != nil {
		return err
	}
	ino := srcDentry.Ino

	// Create tombstone for source
	err = df.bunDB.InsertDentryTombstone(ctx, srcParent, srcName, df.currentWriteEpoch)
	if err != nil {
		return err
	}

	// Create dentry at destination
	err = df.bunDB.UpsertDentry(ctx, &DentryModel{
		ParentIno:  dstParent,
		Name:       dstName,
		Ino:        ino,
		WriteEpoch: df.currentWriteEpoch,
	})
	if err != nil {
		return err
	}

	// Invalidate cache AFTER mutations (Lookup above may have re-populated cache)
	df.invalidateLookupCache(srcParent, srcName)
	df.invalidateLookupCache(dstParent, dstName)
	return nil
}

// --- Content Operations ---

// ReadContent reads file content at the given offset (latest version at readable epoch)
func (df *DataFile) ReadContent(ino int64, offset int64, length int) ([]byte, error) {
	if length <= 0 {
		return nil, nil
	}

	ctx := context.Background()

	// Calculate chunk range
	startChunk := int(offset / ChunkSize)
	endChunk := int((offset + int64(length) - 1) / ChunkSize)

	chunks, err := df.bunDB.ReadContentChunks(ctx, ino, startChunk, endChunk, df.readableEpoch)
	if err != nil {
		return nil, err
	}

	// Assemble data
	var result []byte
	for _, chunk := range chunks {
		chunkIdx := int(chunk.ChunkIdx)
		data := chunk.Data

		// Handle partial reads
		chunkStart := int64(chunkIdx) * ChunkSize
		readStart := offset - chunkStart
		if readStart < 0 {
			readStart = 0
		}
		readEnd := int64(len(data))
		if chunkStart+readEnd > offset+int64(length) {
			readEnd = offset + int64(length) - chunkStart
		}

		if readStart < int64(len(data)) && readStart < readEnd {
			result = append(result, data[readStart:readEnd]...)
		}
	}

	return result, nil
}

// ReadContentAtEpoch reads file content at the given offset using a specific epoch
// This is used for read consistency - the epoch is pinned when the file is opened
// and all subsequent reads use that pinned epoch, even if readableEpoch changes
func (df *DataFile) ReadContentAtEpoch(ino int64, offset int64, length int, epoch int64) ([]byte, error) {
	if length <= 0 {
		return nil, nil
	}

	ctx := context.Background()

	// Calculate chunk range
	startChunk := int(offset / ChunkSize)
	endChunk := int((offset + int64(length) - 1) / ChunkSize)

	chunks, err := df.bunDB.ReadContentChunks(ctx, ino, startChunk, endChunk, epoch)
	if err != nil {
		return nil, err
	}

	// Assemble data
	var result []byte
	for _, chunk := range chunks {
		chunkIdx := int(chunk.ChunkIdx)
		data := chunk.Data

		// Handle partial reads
		chunkStart := int64(chunkIdx) * ChunkSize
		readStart := offset - chunkStart
		if readStart < 0 {
			readStart = 0
		}
		readEnd := int64(len(data))
		if chunkStart+readEnd > offset+int64(length) {
			readEnd = offset + int64(length) - chunkStart
		}

		if readStart < int64(len(data)) && readStart < readEnd {
			result = append(result, data[readStart:readEnd]...)
		}
	}

	return result, nil
}

// traceEnabled controls whether WriteContent logs debug information.
// Set via LATENTFS_TRACE=1 environment variable at daemon startup.
var traceEnabled = os.Getenv("LATENTFS_TRACE") == "1"

// WriteContent writes file content at the given offset at current write epoch
func (df *DataFile) WriteContent(ino int64, offset int64, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if traceEnabled {
		log.Printf("[WriteContent] START ino=%d offset=%d len=%d readEpoch=%d writeEpoch=%d",
			ino, offset, len(data), df.readableEpoch, df.currentWriteEpoch)
	}

	ctx := context.Background()

	// Use a transaction for atomicity
	err := df.bunDB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if traceEnabled {
			log.Printf("[WriteContent] TX started ino=%d", ino)
		}

		// Write data chunk by chunk
		pos := int64(0)
		chunksWritten := 0
		for pos < int64(len(data)) {
			chunkIdx := int((offset + pos) / ChunkSize)
			chunkOffset := int((offset + pos) % ChunkSize)

			// Read existing chunk if partial write
			var existing []byte
			if chunkOffset > 0 || int64(len(data))-pos < ChunkSize {
				existing, _ = df.bunDB.GetContentChunk(ctx, ino, chunkIdx, df.readableEpoch)
			}

			// Prepare new chunk
			var newChunk []byte
			if existing != nil {
				newChunk = make([]byte, max(len(existing), chunkOffset+min(ChunkSize-chunkOffset, len(data)-int(pos))))
				copy(newChunk, existing)
			} else {
				newChunk = make([]byte, chunkOffset+min(ChunkSize-chunkOffset, len(data)-int(pos)))
			}

			// Copy new data
			writeLen := min(ChunkSize-chunkOffset, len(data)-int(pos))
			copy(newChunk[chunkOffset:], data[pos:pos+int64(writeLen)])

			// Insert/replace chunk at current epoch
			_, err := tx.NewInsert().
				Model(&ContentModel{
					Ino:        ino,
					ChunkIdx:   int64(chunkIdx),
					WriteEpoch: df.currentWriteEpoch,
					Data:       newChunk,
				}).
				On("CONFLICT (ino, chunk_idx, write_epoch) DO UPDATE").
				Set("data = EXCLUDED.data").
				Exec(ctx)
			if err != nil {
				if traceEnabled {
					log.Printf("[WriteContent] chunk INSERT failed ino=%d chunk=%d: %v", ino, chunkIdx, err)
				}
				return err
			}

			pos += int64(writeLen)
			chunksWritten++
		}

		if traceEnabled {
			log.Printf("[WriteContent] chunks written=%d, now fetching inode via TX handle", chunksWritten)
		}

		// Update file size
		// CRITICAL: Use transaction handle for GetInode to avoid connection state issues.
		// Querying df.bunDB (main connection) while tx is active can fail with "disk I/O error"
		// after external modifications (like CLI 'data save') that checkpoint the WAL.
		// This was the root cause of TestEpoch/SnapshotRestoreUsesEpochSwitch failures.
		inodeModel, err := df.bunDB.GetInodeWith(tx, ctx, ino, df.readableEpoch)
		if err != nil {
			if traceEnabled {
				log.Printf("[WriteContent] GetInodeWith FAILED ino=%d readEpoch=%d: %v", ino, df.readableEpoch, err)
			}
			return err
		}

		if traceEnabled {
			log.Printf("[WriteContent] GetInodeWith OK ino=%d size=%d", ino, inodeModel.Size)
		}

		inode := inodeModel.ToInode()
		newSize := offset + int64(len(data))
		if newSize < inode.Size {
			newSize = inode.Size
		}
		now := time.Now().Unix()

		// Use raw SQL to avoid alias issues with MAX() in ON CONFLICT
		_, err = tx.NewRaw(`
			INSERT INTO inodes (ino, write_epoch, mode, uid, gid, size, atime, mtime, ctime, nlink)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (ino, write_epoch) DO UPDATE SET
				size = MAX(inodes.size, excluded.size),
				mtime = excluded.mtime,
				ctime = excluded.ctime
		`, ino, df.currentWriteEpoch, int64(inode.Mode), int64(inode.Uid), int64(inode.Gid),
			newSize, inode.Atime.Unix(), now, now, int64(inode.Nlink)).Exec(ctx)

		if err != nil {
			if traceEnabled {
				log.Printf("[WriteContent] inode UPDATE failed ino=%d: %v", ino, err)
			}
			return err
		}

		if traceEnabled {
			log.Printf("[WriteContent] TX commit ino=%d newSize=%d", ino, newSize)
		}
		return nil
	})

	if err != nil {
		if traceEnabled {
			log.Printf("[WriteContent] FAILED ino=%d: %v", ino, err)
		}
	} else if traceEnabled {
		log.Printf("[WriteContent] OK ino=%d", ino)
	}

	return err
}

// TruncateContent truncates file content to the given size at current write epoch
func (df *DataFile) TruncateContent(ino int64, size int64) error {
	ctx := context.Background()

	// Get current inode
	inode, err := df.GetInode(ino)
	if err != nil {
		return err
	}

	// If truncating to smaller size, we need to truncate the last chunk at current epoch
	if size > 0 {
		lastChunk := int(size / ChunkSize)
		chunkOffset := int(size % ChunkSize)
		if chunkOffset > 0 {
			// Read existing last chunk
			data, _ := df.bunDB.GetContentChunk(ctx, ino, lastChunk, df.readableEpoch)
			if len(data) > chunkOffset {
				// Write truncated chunk at current epoch
				err = df.bunDB.UpsertContentChunk(ctx, ino, lastChunk, data[:chunkOffset], df.currentWriteEpoch)
				if err != nil {
					return err
				}
			}
		}
	}

	// Update inode size at current epoch
	now := time.Now().Unix()
	return df.bunDB.UpsertInode(ctx, &InodeModel{
		Ino:        ino,
		WriteEpoch: df.currentWriteEpoch,
		Mode:       int64(inode.Mode),
		UID:        int64(inode.Uid),
		GID:        int64(inode.Gid),
		Size:       size,
		Atime:      inode.Atime.Unix(),
		Mtime:      now,
		Ctime:      now,
		Nlink:      int64(inode.Nlink),
	})
}

// --- Path Resolution ---

// ResolvePath resolves a path to an inode number
func (df *DataFile) ResolvePath(path string) (int64, error) {
	parts := common.SplitPath(path)
	if len(parts) == 0 {
		return RootIno, nil
	}

	currentIno := int64(RootIno)
	for _, part := range parts {
		dentry, err := df.Lookup(currentIno, part)
		if err != nil {
			return 0, err
		}
		currentIno = dentry.Ino
	}

	return currentIno, nil
}

// --- Symlink Operations ---

// CreateSymlink stores the target path for a symbolic link at current write epoch
func (df *DataFile) CreateSymlink(ino int64, target string) error {
	ctx := context.Background()
	return df.bunDB.UpsertSymlink(ctx, ino, target, df.currentWriteEpoch)
}

// ReadSymlink retrieves the target path for a symbolic link (latest version at readable epoch)
func (df *DataFile) ReadSymlink(ino int64) (string, error) {
	ctx := context.Background()
	return df.bunDB.GetSymlink(ctx, ino, df.readableEpoch)
}

// --- Source Folder (for soft-fork) ---

// SetSourceFolder stores the source folder path for this data file (soft-fork)
func (df *DataFile) SetSourceFolder(sourceFolder string) error {
	ctx := context.Background()
	return df.bunDB.SetSchemaInfo(ctx, "source_folder", sourceFolder)
}

// GetSourceFolder retrieves the source folder path if this is a soft-fork
// Returns empty string if not a soft-fork
func (df *DataFile) GetSourceFolder() string {
	ctx := context.Background()
	sourceFolder, _ := df.bunDB.GetSchemaInfo(ctx, "source_folder")
	return sourceFolder
}

// --- Fork Type ---

// Fork type constants
const (
	ForkTypeNone = ""     // Not a fork (regular mount or empty data file)
	ForkTypeSoft = "soft" // Soft-fork: copy-on-write with source folder fallback
	ForkTypeHard = "hard" // Hard-fork: independent copy, source recorded for history only
)

// SetForkType stores the fork type for this data file
func (df *DataFile) SetForkType(forkType string) error {
	ctx := context.Background()
	return df.bunDB.SetSchemaInfo(ctx, "fork_type", forkType)
}

// GetForkType retrieves the fork type
// Returns empty string if not a fork
func (df *DataFile) GetForkType() string {
	ctx := context.Background()
	forkType, _ := df.bunDB.GetSchemaInfo(ctx, "fork_type")
	return forkType
}

// IsSoftFork returns true if this is a soft-fork (copy-on-write enabled)
func (df *DataFile) IsSoftFork() bool {
	return df.GetForkType() == ForkTypeSoft
}
