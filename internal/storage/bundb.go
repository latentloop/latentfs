package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"

	"latentfs/internal/util"
	"latentfs/internal/common"
)

// BunDB wraps a Bun database instance for type-safe queries.
type BunDB struct {
	*bun.DB
}

// NewBunDB wraps an existing *sql.DB with Bun's type-safe query builder.
func NewBunDB(sqlDB *sql.DB) *BunDB {
	bunDB := bun.NewDB(sqlDB, sqlitedialect.New())
	return &BunDB{DB: bunDB}
}

// --- Epoch Operations ---

// GetMaxReadyEpoch returns the maximum epoch with "ready" status.
func (db *BunDB) GetMaxReadyEpoch(ctx context.Context) (int64, error) {
	var epoch sql.NullInt64
	err := db.NewRaw(`SELECT MAX(epoch) FROM write_epochs WHERE status = 'ready'`).Scan(ctx, &epoch)
	if err != nil {
		return 0, err
	}
	if epoch.Valid {
		return epoch.Int64, nil
	}
	return 0, nil
}

// GetConfigValue retrieves a config value by key.
func (db *BunDB) GetConfigValue(ctx context.Context, key string) (string, error) {
	var config ConfigModel
	err := db.NewSelect().
		Model(&config).
		Where("key = ?", key).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return config.Value, nil
}

// SetConfigValue sets a config value (upserts).
func (db *BunDB) SetConfigValue(ctx context.Context, key, value string) error {
	_, err := db.NewInsert().
		Model(&ConfigModel{Key: key, Value: value}).
		On("CONFLICT (key) DO UPDATE").
		Set("value = EXCLUDED.value").
		Exec(ctx)
	return err
}

// CreateWriteEpoch creates a new write epoch and returns its ID.
// Uses retry logic to handle transient "database is locked" errors that can occur
// when the daemon and CLI both have the database open (WAL checkpoint contention).
func (db *BunDB) CreateWriteEpoch(ctx context.Context, status, createdBy string) (int64, error) {
	return util.RetryWithResult(ctx,
		func() (int64, error) {
			return db.createWriteEpochInternal(ctx, status, createdBy)
		},
		util.DatabaseRetryOptions(ctx)...)
}

// createWriteEpochInternal is the actual implementation of CreateWriteEpoch.
func (db *BunDB) createWriteEpochInternal(ctx context.Context, status, createdBy string) (int64, error) {
	now := time.Now().Unix()
	model := &WriteEpochModel{
		Status:    status,
		CreatedBy: createdBy,
		CreatedAt: now,
		UpdatedAt: now,
	}
	// Use RETURNING clause to get the epoch ID (libsql doesn't support LastInsertId)
	_, err := db.NewInsert().
		Model(model).
		Returning("epoch").
		Exec(ctx)
	if err != nil {
		return 0, err
	}
	return model.Epoch, nil
}

// UpdateEpochStatus updates an epoch's status.
func (db *BunDB) UpdateEpochStatus(ctx context.Context, epoch int64, status string) error {
	now := time.Now().Unix()
	_, err := db.NewUpdate().
		Model((*WriteEpochModel)(nil)).
		Set("status = ?", status).
		Set("updated_at = ?", now).
		Where("epoch = ?", epoch).
		Exec(ctx)
	return err
}

// GetDraftEpochs returns all epochs with "draft" status.
func (db *BunDB) GetDraftEpochs(ctx context.Context) ([]int64, error) {
	var epochs []int64
	err := db.NewRaw(`SELECT epoch FROM write_epochs WHERE status = 'draft'`).Scan(ctx, &epochs)
	return epochs, err
}

// GetDeprecatedEpochs returns all epochs with "deprecated" status.
func (db *BunDB) GetDeprecatedEpochs(ctx context.Context) ([]int64, error) {
	var epochs []int64
	err := db.NewRaw(`SELECT epoch FROM write_epochs WHERE status = 'deprecated'`).Scan(ctx, &epochs)
	return epochs, err
}

// DeleteEpochData deletes all data associated with an epoch.
func (db *BunDB) DeleteEpochData(ctx context.Context, epoch int64) error {
	if _, err := db.NewDelete().Model((*ContentModel)(nil)).Where("write_epoch = ?", epoch).Exec(ctx); err != nil {
		return err
	}
	if _, err := db.NewDelete().Model((*SymlinkModel)(nil)).Where("write_epoch = ?", epoch).Exec(ctx); err != nil {
		return err
	}
	if _, err := db.NewDelete().Model((*DentryModel)(nil)).Where("write_epoch = ?", epoch).Exec(ctx); err != nil {
		return err
	}
	if _, err := db.NewDelete().Model((*InodeModel)(nil)).Where("write_epoch = ?", epoch).Exec(ctx); err != nil {
		return err
	}
	if _, err := db.NewDelete().Model((*WriteEpochModel)(nil)).Where("epoch = ?", epoch).Exec(ctx); err != nil {
		return err
	}
	return nil
}

// --- Inode Operations ---

// GetInode retrieves an inode at or before the specified epoch.
// Returns ErrNotFound if inode doesn't exist or is a tombstone (mode=0).
func (db *BunDB) GetInode(ctx context.Context, ino int64, maxEpoch int64) (*InodeModel, error) {
	return db.getInodeWith(db.DB, ctx, ino, maxEpoch)
}

// GetInodeWith is like GetInode but uses the provided bun.IDB (for transaction support).
func (db *BunDB) GetInodeWith(idb bun.IDB, ctx context.Context, ino int64, maxEpoch int64) (*InodeModel, error) {
	return db.getInodeWith(idb, ctx, ino, maxEpoch)
}

func (db *BunDB) getInodeWith(idb bun.IDB, ctx context.Context, ino int64, maxEpoch int64) (*InodeModel, error) {
	var inode InodeModel
	err := idb.NewSelect().
		Model(&inode).
		Where("ino = ?", ino).
		Where("write_epoch <= ?", maxEpoch).
		Order("write_epoch DESC").
		Limit(1).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, common.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	// Check for tombstone
	if inode.Mode == ModeDeleted {
		return nil, common.ErrNotFound
	}
	return &inode, nil
}

// GetMaxIno returns the maximum inode number.
func (db *BunDB) GetMaxIno(ctx context.Context) (int64, error) {
	return db.getMaxInoWith(db.DB, ctx)
}

// GetMaxInoWith is like GetMaxIno but uses the provided bun.IDB (for transaction support).
func (db *BunDB) GetMaxInoWith(idb bun.IDB, ctx context.Context) (int64, error) {
	return db.getMaxInoWith(idb, ctx)
}

func (db *BunDB) getMaxInoWith(idb bun.IDB, ctx context.Context) (int64, error) {
	var maxIno sql.NullInt64
	err := idb.NewRaw(`SELECT MAX(ino) FROM inodes`).Scan(ctx, &maxIno)
	if err != nil {
		return 1, err
	}
	if maxIno.Valid {
		return maxIno.Int64, nil
	}
	return 1, nil
}

// InsertInode inserts a new inode version.
func (db *BunDB) InsertInode(ctx context.Context, inode *InodeModel) error {
	return db.insertInodeWith(db.DB, ctx, inode)
}

// InsertInodeWith is like InsertInode but uses the provided bun.IDB (for transaction support).
func (db *BunDB) InsertInodeWith(idb bun.IDB, ctx context.Context, inode *InodeModel) error {
	return db.insertInodeWith(idb, ctx, inode)
}

func (db *BunDB) insertInodeWith(idb bun.IDB, ctx context.Context, inode *InodeModel) error {
	_, err := idb.NewInsert().Model(inode).Exec(ctx)
	return err
}

// UpsertInode inserts or updates an inode at the specified epoch.
func (db *BunDB) UpsertInode(ctx context.Context, inode *InodeModel) error {
	return db.upsertInodeWith(db.DB, ctx, inode)
}

// UpsertInodeWith is like UpsertInode but uses the provided bun.IDB (for transaction support).
func (db *BunDB) UpsertInodeWith(idb bun.IDB, ctx context.Context, inode *InodeModel) error {
	return db.upsertInodeWith(idb, ctx, inode)
}

func (db *BunDB) upsertInodeWith(idb bun.IDB, ctx context.Context, inode *InodeModel) error {
	_, err := idb.NewInsert().
		Model(inode).
		On("CONFLICT (ino, write_epoch) DO UPDATE").
		Set("mode = EXCLUDED.mode").
		Set("uid = EXCLUDED.uid").
		Set("gid = EXCLUDED.gid").
		Set("size = EXCLUDED.size").
		Set("atime = EXCLUDED.atime").
		Set("mtime = EXCLUDED.mtime").
		Set("ctime = EXCLUDED.ctime").
		Set("nlink = EXCLUDED.nlink").
		Exec(ctx)
	return err
}

// InsertInodeTombstone creates a tombstone (mode=0) for the inode at the specified epoch.
func (db *BunDB) InsertInodeTombstone(ctx context.Context, ino, epoch int64) error {
	return db.insertInodeTombstoneWith(db.DB, ctx, ino, epoch)
}

// InsertInodeTombstoneWith is like InsertInodeTombstone but uses the provided bun.IDB.
func (db *BunDB) InsertInodeTombstoneWith(idb bun.IDB, ctx context.Context, ino, epoch int64) error {
	return db.insertInodeTombstoneWith(idb, ctx, ino, epoch)
}

func (db *BunDB) insertInodeTombstoneWith(idb bun.IDB, ctx context.Context, ino, epoch int64) error {
	now := time.Now().Unix()
	_, err := idb.NewInsert().
		Model(&InodeModel{
			Ino:        ino,
			WriteEpoch: epoch,
			Mode:       0,
			UID:        0,
			GID:        0,
			Size:       0,
			Atime:      now,
			Mtime:      now,
			Ctime:      now,
			Nlink:      0,
		}).
		On("CONFLICT (ino, write_epoch) DO UPDATE").
		Set("mode = 0").
		Set("ctime = EXCLUDED.ctime").
		Exec(ctx)
	return err
}

// --- Dentry Operations ---

// GetDentry retrieves a directory entry at or before the specified epoch.
// Returns ErrNotFound if the entry doesn't exist or is a tombstone (ino=0).
func (db *BunDB) GetDentry(ctx context.Context, parentIno int64, name string, maxEpoch int64) (*DentryModel, error) {
	return db.getDentryWith(db.DB, ctx, parentIno, name, maxEpoch)
}

// GetDentryWith is like GetDentry but uses the provided bun.IDB (for transaction support).
func (db *BunDB) GetDentryWith(idb bun.IDB, ctx context.Context, parentIno int64, name string, maxEpoch int64) (*DentryModel, error) {
	return db.getDentryWith(idb, ctx, parentIno, name, maxEpoch)
}

func (db *BunDB) getDentryWith(idb bun.IDB, ctx context.Context, parentIno int64, name string, maxEpoch int64) (*DentryModel, error) {
	var dentry DentryModel
	err := idb.NewSelect().
		Model(&dentry).
		Where("parent_ino = ?", parentIno).
		Where("name = ?", name).
		Where("write_epoch <= ?", maxEpoch).
		Order("write_epoch DESC").
		Limit(1).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, common.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	// Debug logging for anti-tombstone investigation
	if name == "cli" && parentIno == 1 {
		log.Printf("[BunDB] getDentry: parentIno=%d name=%q maxEpoch=%d → ino=%d epoch=%d", parentIno, name, maxEpoch, dentry.Ino, dentry.WriteEpoch)
	}
	// Check for tombstone (ino=0) or anti-tombstone (ino=-1)
	// Both should return ErrNotFound so VFS falls through to source folder
	if dentry.Ino <= 0 {
		return nil, common.ErrNotFound
	}
	return &dentry, nil
}

// LookupWithInode combines GetDentry + GetInode into a single SQL query (O2 optimization).
// Returns both the dentry and the child inode in one round-trip.
func (db *BunDB) LookupWithInode(ctx context.Context, parentIno int64, name string, maxEpoch int64) (*DentryModel, *InodeModel, error) {
	type dentryInodeResult struct {
		// Dentry fields
		ParentIno  int64  `bun:"parent_ino"`
		Name       string `bun:"name"`
		Ino        int64  `bun:"ino"`
		WriteEpoch int64  `bun:"write_epoch"`
		// Inode fields
		IMode  int64 `bun:"i_mode"`
		IUID   int64 `bun:"i_uid"`
		IGID   int64 `bun:"i_gid"`
		ISize  int64 `bun:"i_size"`
		IAtime int64 `bun:"i_atime"`
		IMtime int64 `bun:"i_mtime"`
		ICtime int64 `bun:"i_ctime"`
		INlink int64 `bun:"i_nlink"`
		IEpoch int64 `bun:"i_epoch"`
	}
	var result dentryInodeResult
	err := db.NewRaw(`
		SELECT d.parent_ino, d.name, d.ino, d.write_epoch,
		       i.mode AS i_mode, i.uid AS i_uid, i.gid AS i_gid,
		       i.size AS i_size, i.atime AS i_atime, i.mtime AS i_mtime,
		       i.ctime AS i_ctime, i.nlink AS i_nlink, i.write_epoch AS i_epoch
		FROM dentries d
		INNER JOIN inodes i ON d.ino = i.ino
		  AND i.write_epoch = (
		    SELECT MAX(write_epoch) FROM inodes WHERE ino = d.ino AND write_epoch <= ?
		  )
		WHERE d.parent_ino = ? AND d.name = ?
		  AND d.write_epoch = (
		    SELECT MAX(write_epoch) FROM dentries
		    WHERE parent_ino = ? AND name = ? AND write_epoch <= ?
		  )
		  AND d.ino != 0
	`, maxEpoch, parentIno, name, parentIno, name, maxEpoch).Scan(ctx, &result)
	if err == sql.ErrNoRows {
		return nil, nil, common.ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	// Check for inode tombstone
	if result.IMode == ModeDeleted {
		return nil, nil, common.ErrNotFound
	}
	dentry := &DentryModel{
		ParentIno:  result.ParentIno,
		Name:       result.Name,
		Ino:        result.Ino,
		WriteEpoch: result.WriteEpoch,
	}
	inode := &InodeModel{
		Ino:        result.Ino,
		WriteEpoch: result.IEpoch,
		Mode:       result.IMode,
		UID:        result.IUID,
		GID:        result.IGID,
		Size:       result.ISize,
		Atime:      result.IAtime,
		Mtime:      result.IMtime,
		Ctime:      result.ICtime,
		Nlink:      result.INlink,
	}
	return dentry, inode, nil
}

// HasLiveChildren checks if a directory has any non-tombstoned children (O6 optimization).
// Uses EXISTS + LIMIT 1 to short-circuit instead of materializing all entries via ListDir.
func (db *BunDB) HasLiveChildren(ctx context.Context, parentIno int64, maxEpoch int64) (bool, error) {
	var exists int
	err := db.NewRaw(`
		SELECT EXISTS(
			SELECT 1 FROM dentries d
			INNER JOIN (
				SELECT parent_ino, name, MAX(write_epoch) as max_epoch
				FROM dentries
				WHERE parent_ino = ? AND write_epoch <= ?
				GROUP BY parent_ino, name
			) latest ON d.parent_ino = latest.parent_ino
			         AND d.name = latest.name
			         AND d.write_epoch = latest.max_epoch
			INNER JOIN (
				SELECT ino, mode FROM inodes i2
				WHERE (i2.ino, i2.write_epoch) IN (
					SELECT ino, MAX(write_epoch) FROM inodes
					WHERE write_epoch <= ? GROUP BY ino
				)
			) i ON d.ino = i.ino
			WHERE d.ino != 0 AND i.mode != 0
			LIMIT 1
		)
	`, parentIno, maxEpoch, maxEpoch).Scan(ctx, &exists)
	if err != nil {
		return false, err
	}
	return exists == 1, nil
}

// =============================================================================
// TOMBSTONE SYSTEM DOCUMENTATION
// =============================================================================
//
// LatentFS uses a tombstone system to track deleted files in soft-forks (overlays).
// Since soft-forks show a merged view of the overlay database and source folder,
// tombstones are needed to "hide" deleted files that still exist in the source.
//
// FOUR DENTRY STATES
// ------------------
// A dentry can be in one of four states based on (ino, inode.mode):
//
//   1. NORMAL ENTRY: ino > 0, mode > 0
//      A live file/directory with valid inode. Visible to VFS.
//
//   2. INODE TOMBSTONE: ino > 0, mode = 0
//      The dentry points to a valid inode number, but the inode itself is
//      marked as deleted (mode=0). Used when deleting SOURCE-ONLY files
//      (files that exist only in the source folder, not yet copied to overlay).
//      VFS returns ENOENT, blocking fallback to source folder.
//
//   3. DENTRY TOMBSTONE: ino = 0
//      The dentry entry itself is a tombstone. Used when deleting OVERLAY files
//      (files that were created or copied to the overlay).
//      VFS returns ENOENT, blocking fallback to source folder.
//
//   4. ANTI-TOMBSTONE: ino = -1
//      A special marker that tells VFS to "passthrough" to source folder.
//      Created during overlay snapshot restore to unhide files that were
//      deleted AFTER the snapshot was taken.
//      VFS returns "not found in overlay" allowing source folder fallback.
//
// TOMBSTONE INVARIANTS
// --------------------
//   1. Tombstones are only created for entries that existed at some point.
//      Never create a tombstone for a path that never existed.
//
//   2. For source-only files: Use INODE TOMBSTONE (create dentry with ino pointing
//      to new inode with mode=0). This allocates an inode number to track the deletion.
//
//   3. For overlay files: Use DENTRY TOMBSTONE (update dentry to ino=0).
//      The file's inode already exists in overlay, just mark dentry as deleted.
//
//   4. Anti-tombstones (ino=-1) should ONLY be created during snapshot restore
//      to override tombstones that were created after the snapshot.
//
//   5. Tombstones persist across daemon restarts (stored in SQLite database).
//
//   6. Tombstones are epoch-versioned like all other data. To see tombstones,
//      query at the appropriate epoch.
//
// TOMBSTONE QUERIES
// -----------------
//   - IsDentryTombstoned: Checks for dentry tombstones only (ino=0)
//   - IsTombstoned: Checks for BOTH tombstone types (ino=0 OR mode=0)
//   - ListDentryTombstoneNames: Lists names with dentry tombstones only
//   - ListAllTombstoneNames: Lists names with any tombstone type
//
// =============================================================================

// IsDentryTombstoned checks if the latest dentry for (parentIno, name) at maxEpoch is a tombstone (ino=0).
// Returns true if a tombstone dentry exists, false if no dentry exists or it's live.
// This is used to block source-folder fallback for explicitly deleted entries in soft-forks.
func (db *BunDB) IsDentryTombstoned(ctx context.Context, parentIno int64, name string, maxEpoch int64) bool {
	var dentry DentryModel
	err := db.NewSelect().
		Model(&dentry).
		Where("parent_ino = ?", parentIno).
		Where("name = ?", name).
		Where("write_epoch <= ?", maxEpoch).
		Order("write_epoch DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		return false
	}
	return dentry.Ino == 0
}

// IsTombstoned checks if the entry at (parentIno, name) is effectively tombstoned at maxEpoch.
// Returns true if either:
//   - Dentry tombstone: dentry has ino=0
//   - Inode tombstone: dentry has ino>0 but points to inode with mode=0
//
// Returns false if entry doesn't exist or is live.
// This is the unified tombstone check that handles both tombstone types in a single query.
func (db *BunDB) IsTombstoned(ctx context.Context, parentIno int64, name string, maxEpoch int64) (bool, error) {
	var isTombstone int
	err := db.NewRaw(`
		SELECT CASE
			WHEN d.ino = 0 THEN 1  -- dentry tombstone
			WHEN d.ino > 0 AND i.mode = 0 THEN 1  -- inode tombstone
			ELSE 0
		END as is_tombstone
		FROM dentries d
		LEFT JOIN (
			SELECT ino, mode FROM inodes i2
			WHERE (i2.ino, i2.write_epoch) IN (
				SELECT ino, MAX(write_epoch) FROM inodes
				WHERE write_epoch <= ?
				GROUP BY ino
			)
		) i ON d.ino = i.ino
		WHERE d.parent_ino = ? AND d.name = ?
		  AND d.write_epoch = (
			SELECT MAX(write_epoch) FROM dentries
			WHERE parent_ino = ? AND name = ? AND write_epoch <= ?
		  )
	`, maxEpoch, parentIno, name, parentIno, name, maxEpoch).Scan(ctx, &isTombstone)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return isTombstone == 1, nil
}

// ListAllTombstoneNames returns the names of all tombstoned entries under parentIno at maxEpoch.
// This includes both tombstone types:
//   - Dentry tombstones: entries with ino=0
//   - Inode tombstones: entries with ino>0 but pointing to inodes with mode=0
//
// Used by ReadDir to block source folder entries that have been deleted in any way.
func (db *BunDB) ListAllTombstoneNames(ctx context.Context, parentIno int64, maxEpoch int64) ([]string, error) {
	var names []string
	err := db.NewRaw(`
		SELECT d.name
		FROM dentries d
		INNER JOIN (
			SELECT parent_ino, name, MAX(write_epoch) as max_epoch
			FROM dentries
			WHERE parent_ino = ? AND write_epoch <= ?
			GROUP BY parent_ino, name
		) latest ON d.parent_ino = latest.parent_ino
		         AND d.name = latest.name
		         AND d.write_epoch = latest.max_epoch
		LEFT JOIN (
			SELECT ino, mode FROM inodes i2
			WHERE (i2.ino, i2.write_epoch) IN (
				SELECT ino, MAX(write_epoch) FROM inodes
				WHERE write_epoch <= ?
				GROUP BY ino
			)
		) i ON d.ino = i.ino
		WHERE d.ino = 0 OR (d.ino > 0 AND i.mode = 0)
	`, parentIno, maxEpoch, maxEpoch).Scan(ctx, &names)
	if err != nil {
		return nil, err
	}
	return names, nil
}

// ListDentryTombstoneNames returns the names of all dentry tombstones under parentIno at maxEpoch.
// Used by ReadDir to block source folder entries that have been explicitly deleted.
func (db *BunDB) ListDentryTombstoneNames(ctx context.Context, parentIno int64, maxEpoch int64) ([]string, error) {
	var names []string
	err := db.NewRaw(`
		SELECT d.name
		FROM dentries d
		INNER JOIN (
			SELECT parent_ino, name, MAX(write_epoch) as max_epoch
			FROM dentries
			WHERE parent_ino = ? AND write_epoch <= ?
			GROUP BY parent_ino, name
		) latest ON d.parent_ino = latest.parent_ino
		         AND d.name = latest.name
		         AND d.write_epoch = latest.max_epoch
		WHERE d.ino = 0
	`, parentIno, maxEpoch).Scan(ctx, &names)
	if err != nil {
		return nil, err
	}
	return names, nil
}

// ListDentries retrieves all directory entries for a parent at or before the specified epoch.
// Filters out dentry tombstones (ino=0) but includes inode tombstones (mode=0) for VFS filtering.
func (db *BunDB) ListDentries(ctx context.Context, parentIno int64, maxEpoch int64) ([]DirEntry, error) {
	type rawEntry struct {
		Name  string
		Ino   int64
		Mode  uint32
		Size  int64
		Mtime int64
	}
	var rawEntries []rawEntry
	err := db.NewRaw(`
		SELECT d.name, d.ino, i.mode, i.size, i.mtime
		FROM dentries d
		INNER JOIN (
			SELECT parent_ino, name, MAX(write_epoch) as max_epoch
			FROM dentries
			WHERE parent_ino = ? AND write_epoch <= ?
			GROUP BY parent_ino, name
		) latest_d ON d.parent_ino = latest_d.parent_ino
		         AND d.name = latest_d.name
		         AND d.write_epoch = latest_d.max_epoch
		INNER JOIN (
			SELECT ino, mode, size, mtime, write_epoch
			FROM inodes i2
			WHERE (i2.ino, i2.write_epoch) IN (
				SELECT ino, MAX(write_epoch)
				FROM inodes
				WHERE write_epoch <= ?
				GROUP BY ino
			)
		) i ON d.ino = i.ino
		WHERE d.ino != 0
		ORDER BY d.name
	`, parentIno, maxEpoch, maxEpoch).Scan(ctx, &rawEntries)
	if err != nil {
		return nil, err
	}

	entries := make([]DirEntry, len(rawEntries))
	for i, r := range rawEntries {
		entries[i] = DirEntry{
			Name:  r.Name,
			Ino:   r.Ino,
			Mode:  r.Mode,
			Size:  r.Size,
			Mtime: time.Unix(r.Mtime, 0),
		}
	}
	return entries, nil
}

// ListDirEntries is like ListDentries but returns simpler DirEntry structs with time already converted.
func (db *BunDB) ListDirEntries(ctx context.Context, parentIno int64, maxEpoch int64) ([]DirEntry, error) {
	type rawEntry struct {
		Name  string
		Ino   int64
		Mode  uint32
		Size  int64
		Mtime int64
	}
	var rawEntries []rawEntry
	err := db.NewRaw(`
		SELECT d.name, d.ino, i.mode, i.size, i.mtime
		FROM dentries d
		INNER JOIN (
			SELECT parent_ino, name, MAX(write_epoch) as max_epoch
			FROM dentries
			WHERE parent_ino = ? AND write_epoch <= ?
			GROUP BY parent_ino, name
		) latest_d ON d.parent_ino = latest_d.parent_ino
		         AND d.name = latest_d.name
		         AND d.write_epoch = latest_d.max_epoch
		INNER JOIN (
			SELECT ino, mode, size, mtime, write_epoch
			FROM inodes i2
			WHERE (i2.ino, i2.write_epoch) IN (
				SELECT ino, MAX(write_epoch)
				FROM inodes
				WHERE write_epoch <= ?
				GROUP BY ino
			)
		) i ON d.ino = i.ino
		WHERE d.ino != 0
		ORDER BY d.name
	`, parentIno, maxEpoch, maxEpoch).Scan(ctx, &rawEntries)
	if err != nil {
		return nil, err
	}

	entries := make([]DirEntry, len(rawEntries))
	for i, r := range rawEntries {
		entries[i] = DirEntry{
			Name:  r.Name,
			Ino:   r.Ino,
			Mode:  r.Mode,
			Size:  r.Size,
			Mtime: time.Unix(r.Mtime, 0),
		}
	}
	return entries, nil
}

// InsertDentry inserts a new directory entry version.
func (db *BunDB) InsertDentry(ctx context.Context, dentry *DentryModel) error {
	_, err := db.NewInsert().Model(dentry).Exec(ctx)
	return err
}

// UpsertDentry inserts or updates a directory entry at the specified epoch.
func (db *BunDB) UpsertDentry(ctx context.Context, dentry *DentryModel) error {
	return db.upsertDentryWith(db.DB, ctx, dentry)
}

// UpsertDentryWith is like UpsertDentry but uses the provided bun.IDB (for transaction support).
func (db *BunDB) UpsertDentryWith(idb bun.IDB, ctx context.Context, dentry *DentryModel) error {
	return db.upsertDentryWith(idb, ctx, dentry)
}

func (db *BunDB) upsertDentryWith(idb bun.IDB, ctx context.Context, dentry *DentryModel) error {
	_, err := idb.NewInsert().
		Model(dentry).
		On("CONFLICT (parent_ino, name, write_epoch) DO UPDATE").
		Set("ino = EXCLUDED.ino").
		Exec(ctx)
	return err
}

// InsertDentryTombstone creates a tombstone (ino=0) for the dentry at the specified epoch.
func (db *BunDB) InsertDentryTombstone(ctx context.Context, parentIno int64, name string, epoch int64) error {
	return db.insertDentryTombstoneWith(db.DB, ctx, parentIno, name, epoch)
}

// InsertDentryTombstoneWith is like InsertDentryTombstone but uses the provided bun.IDB.
func (db *BunDB) InsertDentryTombstoneWith(idb bun.IDB, ctx context.Context, parentIno int64, name string, epoch int64) error {
	return db.insertDentryTombstoneWith(idb, ctx, parentIno, name, epoch)
}

func (db *BunDB) insertDentryTombstoneWith(idb bun.IDB, ctx context.Context, parentIno int64, name string, epoch int64) error {
	_, err := idb.NewInsert().
		Model(&DentryModel{
			ParentIno:  parentIno,
			Name:       name,
			WriteEpoch: epoch,
			Ino:        0,
		}).
		On("CONFLICT (parent_ino, name, write_epoch) DO UPDATE").
		Set("ino = 0").
		Exec(ctx)
	return err
}

// --- Content Operations ---

// ReadContentChunks retrieves content chunks for a file at or before the specified epoch.
func (db *BunDB) ReadContentChunks(ctx context.Context, ino int64, startChunk, endChunk int, maxEpoch int64) ([]ContentModel, error) {
	var chunks []ContentModel
	err := db.NewRaw(`
		SELECT c.ino, c.chunk_idx, c.data, c.write_epoch
		FROM content c
		INNER JOIN (
			SELECT ino, chunk_idx, MAX(write_epoch) as max_epoch
			FROM content
			WHERE ino = ? AND chunk_idx >= ? AND chunk_idx <= ? AND write_epoch <= ?
			GROUP BY ino, chunk_idx
		) latest ON c.ino = latest.ino AND c.chunk_idx = latest.chunk_idx AND c.write_epoch = latest.max_epoch
		ORDER BY c.chunk_idx
	`, ino, startChunk, endChunk, maxEpoch).Scan(ctx, &chunks)
	return chunks, err
}

// GetContentChunk retrieves a single content chunk at or before the specified epoch.
func (db *BunDB) GetContentChunk(ctx context.Context, ino int64, chunkIdx int, maxEpoch int64) ([]byte, error) {
	var data []byte
	err := db.NewRaw(`
		SELECT data FROM content
		WHERE ino = ? AND chunk_idx = ? AND write_epoch <= ?
		ORDER BY write_epoch DESC LIMIT 1
	`, ino, chunkIdx, maxEpoch).Scan(ctx, &data)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return data, err
}

// UpsertContentChunk inserts or updates a content chunk at the specified epoch.
func (db *BunDB) UpsertContentChunk(ctx context.Context, ino int64, chunkIdx int, data []byte, epoch int64) error {
	_, err := db.NewInsert().
		Model(&ContentModel{
			Ino:        ino,
			ChunkIdx:   int64(chunkIdx),
			WriteEpoch: epoch,
			Data:       data,
		}).
		On("CONFLICT (ino, chunk_idx, write_epoch) DO UPDATE").
		Set("data = EXCLUDED.data").
		Exec(ctx)
	return err
}

// --- Symlink Operations ---

// GetSymlink retrieves a symlink target at or before the specified epoch.
func (db *BunDB) GetSymlink(ctx context.Context, ino int64, maxEpoch int64) (string, error) {
	var target string
	err := db.NewRaw(`
		SELECT target FROM symlinks
		WHERE ino = ? AND write_epoch <= ?
		ORDER BY write_epoch DESC LIMIT 1
	`, ino, maxEpoch).Scan(ctx, &target)
	if err == sql.ErrNoRows {
		return "", common.ErrNotFound
	}
	return target, err
}

// UpsertSymlink inserts or updates a symlink at the specified epoch.
func (db *BunDB) UpsertSymlink(ctx context.Context, ino int64, target string, epoch int64) error {
	_, err := db.NewInsert().
		Model(&SymlinkModel{
			Ino:        ino,
			WriteEpoch: epoch,
			Target:     target,
		}).
		On("CONFLICT (ino, write_epoch) DO UPDATE").
		Set("target = EXCLUDED.target").
		Exec(ctx)
	return err
}

// --- Schema Info Operations ---

// GetSchemaInfo retrieves a schema info value by key.
func (db *BunDB) GetSchemaInfo(ctx context.Context, key string) (string, error) {
	var info SchemaInfoModel
	err := db.NewSelect().
		Model(&info).
		Where("key = ?", key).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return info.Value, nil
}

// SetSchemaInfo sets a schema info value (upserts).
func (db *BunDB) SetSchemaInfo(ctx context.Context, key, value string) error {
	_, err := db.NewInsert().
		Model(&SchemaInfoModel{Key: key, Value: value}).
		On("CONFLICT (key) DO UPDATE").
		Set("value = EXCLUDED.value").
		Exec(ctx)
	return err
}

// --- Mount Entry Operations ---

// GetMountEntry retrieves a mount entry by sub_path.
func (db *BunDB) GetMountEntry(ctx context.Context, subPath string) (*MountEntryModel, error) {
	var entry MountEntryModel
	err := db.NewSelect().
		Model(&entry).
		Where("sub_path = ?", subPath).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

// ListMountEntries retrieves all mount entries.
func (db *BunDB) ListMountEntries(ctx context.Context) ([]MountEntryModel, error) {
	var entries []MountEntryModel
	err := db.NewSelect().
		Model(&entries).
		Order("sub_path").
		Scan(ctx)
	return entries, err
}

// UpsertMountEntry inserts or updates a mount entry.
func (db *BunDB) UpsertMountEntry(ctx context.Context, entry *MountEntryModel) error {
	_, err := db.NewInsert().
		Model(entry).
		On("CONFLICT (sub_path) DO UPDATE").
		Set("data_file = EXCLUDED.data_file").
		Set("source_folder = EXCLUDED.source_folder").
		Set("fork_type = EXCLUDED.fork_type").
		Set("symlink_target = EXCLUDED.symlink_target").
		Exec(ctx)
	return err
}

// InsertMountEntry inserts a new mount entry.
func (db *BunDB) InsertMountEntry(ctx context.Context, entry *MountEntryModel) error {
	_, err := db.NewInsert().Model(entry).Exec(ctx)
	return err
}

// DeleteMountEntry deletes a mount entry by sub_path.
func (db *BunDB) DeleteMountEntry(ctx context.Context, subPath string) (int64, error) {
	result, err := db.NewDelete().
		Model((*MountEntryModel)(nil)).
		Where("sub_path = ?", subPath).
		Exec(ctx)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// LookupMountByPath finds the mount entry for a given path (longest matching sub_path).
func (db *BunDB) LookupMountByPath(ctx context.Context, path string) (*MountEntryModel, error) {
	var entry MountEntryModel
	err := db.NewRaw(`
		SELECT id, sub_path, data_file, source_folder, fork_type, symlink_target, created_at
		FROM mounts
		WHERE ? = sub_path OR ? LIKE sub_path || '/%'
		ORDER BY LENGTH(sub_path) DESC LIMIT 1
	`, path, path).Scan(ctx, &entry)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

// GetMountBySymlinkTarget finds a mount entry by symlink target path.
func (db *BunDB) GetMountBySymlinkTarget(ctx context.Context, symlinkTarget string) (*MountEntryModel, error) {
	var entry MountEntryModel
	err := db.NewSelect().
		Model(&entry).
		Where("symlink_target = ?", symlinkTarget).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

// GetMountByDataFile finds a mount entry by data file path.
func (db *BunDB) GetMountByDataFile(ctx context.Context, dataFile string) (*MountEntryModel, error) {
	var entry MountEntryModel
	err := db.NewSelect().
		Model(&entry).
		Where("data_file = ?", dataFile).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

// --- Snapshot Operations ---

// GetSnapshot retrieves a snapshot by ID.
func (db *BunDB) GetSnapshot(ctx context.Context, id string) (*SnapshotModel, error) {
	var snapshot SnapshotModel
	err := db.NewSelect().
		Model(&snapshot).
		Where("id = ?", id).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("snapshot %q not found", id)
	}
	if err != nil {
		return nil, err
	}
	return &snapshot, nil
}

// GetSnapshotByTag retrieves a snapshot by tag.
func (db *BunDB) GetSnapshotByTag(ctx context.Context, tag string) (*SnapshotModel, error) {
	var snapshot SnapshotModel
	err := db.NewSelect().
		Model(&snapshot).
		Where("tag = ?", tag).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &snapshot, nil
}

// GetSnapshotByPrefix retrieves a snapshot by ID prefix.
func (db *BunDB) GetSnapshotByPrefix(ctx context.Context, prefix string) (*SnapshotModel, error) {
	var snapshot SnapshotModel
	err := db.NewSelect().
		Model(&snapshot).
		Where("id LIKE ?", prefix+"%").
		Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &snapshot, nil
}

// ListSnapshots retrieves all snapshots ordered by creation time (newest first).
func (db *BunDB) ListSnapshots(ctx context.Context) ([]SnapshotModel, error) {
	var snapshots []SnapshotModel
	err := db.NewSelect().
		Model(&snapshots).
		Order("created_at DESC").
		Scan(ctx)
	return snapshots, err
}

// InsertSnapshot inserts a new snapshot.
func (db *BunDB) InsertSnapshot(ctx context.Context, snapshot *SnapshotModel) error {
	_, err := db.NewInsert().Model(snapshot).Exec(ctx)
	return err
}

// DeleteSnapshotData deletes a snapshot and all its associated data.
func (db *BunDB) DeleteSnapshotData(ctx context.Context, id string) error {
	return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := tx.NewDelete().Model((*SnapshotContentModel)(nil)).Where("snapshot_id = ?", id).Exec(ctx); err != nil {
			return err
		}
		if _, err := tx.NewDelete().Model((*SnapshotSymlinkModel)(nil)).Where("snapshot_id = ?", id).Exec(ctx); err != nil {
			return err
		}
		if _, err := tx.NewDelete().Model((*SnapshotDentryModel)(nil)).Where("snapshot_id = ?", id).Exec(ctx); err != nil {
			return err
		}
		if _, err := tx.NewDelete().Model((*SnapshotInodeModel)(nil)).Where("snapshot_id = ?", id).Exec(ctx); err != nil {
			return err
		}
		if _, err := tx.NewDelete().Model((*SnapshotModel)(nil)).Where("id = ?", id).Exec(ctx); err != nil {
			return err
		}
		return nil
	})
}

// TagExists checks if a snapshot tag already exists.
func (db *BunDB) TagExists(ctx context.Context, tag string) (bool, error) {
	exists, err := db.NewSelect().
		Model((*SnapshotModel)(nil)).
		Where("tag = ?", tag).
		Exists(ctx)
	return exists, err
}

// --- Snapshot Data Copy Operations ---

// CopyInodesToSnapshot copies latest inodes at the specified epoch to snapshot tables.
// Excludes tombstones (mode=0).
func (db *BunDB) CopyInodesToSnapshot(ctx context.Context, snapshotID string, maxEpoch int64) error {
	_, err := db.NewRaw(`
		INSERT INTO snapshot_inodes (snapshot_id, ino, mode, uid, gid, size, atime, mtime, ctime, nlink)
		SELECT ?, i.ino, i.mode, i.uid, i.gid, i.size, i.atime, i.mtime, i.ctime, i.nlink
		FROM inodes i
		INNER JOIN (
			SELECT ino, MAX(write_epoch) as max_epoch
			FROM inodes WHERE write_epoch <= ?
			GROUP BY ino
		) latest ON i.ino = latest.ino AND i.write_epoch = latest.max_epoch
		WHERE i.mode != 0
	`, snapshotID, maxEpoch).Exec(ctx)
	return err
}

// CopyDentriesToSnapshot copies latest dentries at the specified epoch to snapshot tables.
// Excludes tombstones (ino=0).
func (db *BunDB) CopyDentriesToSnapshot(ctx context.Context, snapshotID string, maxEpoch int64) error {
	_, err := db.NewRaw(`
		INSERT INTO snapshot_dentries (snapshot_id, parent_ino, name, ino)
		SELECT ?, d.parent_ino, d.name, d.ino
		FROM dentries d
		INNER JOIN (
			SELECT parent_ino, name, MAX(write_epoch) as max_epoch
			FROM dentries WHERE write_epoch <= ?
			GROUP BY parent_ino, name
		) latest ON d.parent_ino = latest.parent_ino AND d.name = latest.name AND d.write_epoch = latest.max_epoch
		WHERE d.ino != 0
	`, snapshotID, maxEpoch).Exec(ctx)
	return err
}

// CopySymlinksToSnapshot copies latest symlinks at the specified epoch to snapshot tables.
func (db *BunDB) CopySymlinksToSnapshot(ctx context.Context, snapshotID string, maxEpoch int64) error {
	_, err := db.NewRaw(`
		INSERT INTO snapshot_symlinks (snapshot_id, ino, target)
		SELECT ?, s.ino, s.target
		FROM symlinks s
		INNER JOIN (
			SELECT ino, MAX(write_epoch) as max_epoch
			FROM symlinks WHERE write_epoch <= ?
			GROUP BY ino
		) latest ON s.ino = latest.ino AND s.write_epoch = latest.max_epoch
	`, snapshotID, maxEpoch).Exec(ctx)
	return err
}

// ContentChunkForSnapshot represents a content chunk to be saved in a snapshot.
type ContentChunkForSnapshot struct {
	Ino      int64
	ChunkIdx int
	Data     []byte
}

// GetLatestContentAtEpoch retrieves all latest content chunks at or before the specified epoch.
func (db *BunDB) GetLatestContentAtEpoch(ctx context.Context, maxEpoch int64) ([]ContentChunkForSnapshot, error) {
	var chunks []ContentChunkForSnapshot
	err := db.NewRaw(`
		SELECT c.ino, c.chunk_idx, c.data
		FROM content c
		INNER JOIN (
			SELECT ino, chunk_idx, MAX(write_epoch) as max_epoch
			FROM content WHERE write_epoch <= ?
			GROUP BY ino, chunk_idx
		) latest ON c.ino = latest.ino AND c.chunk_idx = latest.chunk_idx AND c.write_epoch = latest.max_epoch
	`, maxEpoch).Scan(ctx, &chunks)
	return chunks, err
}

// InsertContentBlock inserts a content block for deduplication (ignores duplicates).
func (db *BunDB) InsertContentBlock(ctx context.Context, hash string, data []byte) error {
	_, err := db.NewRaw(`INSERT OR IGNORE INTO content_blocks (hash, data) VALUES (?, ?)`, hash, data).Exec(ctx)
	return err
}

// InsertSnapshotContent inserts a snapshot content reference.
func (db *BunDB) InsertSnapshotContent(ctx context.Context, snapshotID string, ino int64, chunkIdx int, hash string) error {
	_, err := db.NewInsert().Model(&SnapshotContentModel{
		SnapshotID: snapshotID,
		Ino:        ino,
		ChunkIdx:   int64(chunkIdx),
		Hash:       hash,
	}).Exec(ctx)
	return err
}

// InsertSnapshotInode inserts a snapshot inode.
func (db *BunDB) InsertSnapshotInode(ctx context.Context, snapshotID string, ino, mode, uid, gid, size, atime, mtime, ctime, nlink int64) error {
	_, err := db.NewInsert().Model(&SnapshotInodeModel{
		SnapshotID: snapshotID,
		Ino:        ino,
		Mode:       mode,
		UID:        uid,
		GID:        gid,
		Size:       size,
		Atime:      atime,
		Mtime:      mtime,
		Ctime:      ctime,
		Nlink:      nlink,
	}).Exec(ctx)
	return err
}

// InsertSnapshotDentry inserts a snapshot dentry.
func (db *BunDB) InsertSnapshotDentry(ctx context.Context, snapshotID string, parentIno int64, name string, ino int64) error {
	_, err := db.NewInsert().Model(&SnapshotDentryModel{
		SnapshotID: snapshotID,
		ParentIno:  parentIno,
		Name:       name,
		Ino:        ino,
	}).Exec(ctx)
	return err
}

// InsertSnapshotSymlink inserts a snapshot symlink.
func (db *BunDB) InsertSnapshotSymlink(ctx context.Context, snapshotID string, ino int64, target string) error {
	_, err := db.NewInsert().Model(&SnapshotSymlinkModel{
		SnapshotID: snapshotID,
		Ino:        ino,
		Target:     target,
	}).Exec(ctx)
	return err
}

// GetMaxSnapshotIno returns the maximum inode number in a snapshot.
func (db *BunDB) GetMaxSnapshotIno(ctx context.Context, snapshotID string) (int64, error) {
	var maxIno sql.NullInt64
	err := db.NewRaw(`SELECT COALESCE(MAX(ino), 1) FROM snapshot_inodes WHERE snapshot_id = ?`, snapshotID).Scan(ctx, &maxIno)
	if err != nil {
		return 1, err
	}
	if maxIno.Valid {
		return maxIno.Int64, nil
	}
	return 1, nil
}

// ListSnapshotDentriesForParent lists snapshot dentries for a given parent inode.
func (db *BunDB) ListSnapshotDentriesForParent(ctx context.Context, snapshotID string, parentIno int64) ([]SnapshotDentryModel, error) {
	var entries []SnapshotDentryModel
	err := db.NewSelect().
		Model(&entries).
		Where("snapshot_id = ?", snapshotID).
		Where("parent_ino = ?", parentIno).
		Scan(ctx)
	return entries, err
}

// GetSnapshotInodeMode returns the mode of a snapshot inode.
func (db *BunDB) GetSnapshotInodeMode(ctx context.Context, snapshotID string, ino int64) (int64, error) {
	var mode int64
	err := db.NewRaw(`SELECT mode FROM snapshot_inodes WHERE snapshot_id = ? AND ino = ?`, snapshotID, ino).Scan(ctx, &mode)
	return mode, err
}

// GetSnapshotInodeByPath resolves a path to an inode in a snapshot.
func (db *BunDB) GetSnapshotInodeByPath(ctx context.Context, snapshotID string, path string) (int64, error) {
	parts := splitPathForDB(path)
	currentIno := int64(RootIno)

	for _, part := range parts {
		if part == "" {
			continue
		}
		var ino int64
		err := db.NewRaw(`SELECT ino FROM snapshot_dentries WHERE snapshot_id = ? AND parent_ino = ? AND name = ?`,
			snapshotID, currentIno, part).Scan(ctx, &ino)
		if err != nil {
			return 0, err
		}
		currentIno = ino
	}
	return currentIno, nil
}

// splitPathForDB splits a path into components for database operations.
func splitPathForDB(path string) []string {
	if path == "" || path == "." {
		return nil
	}
	// Remove leading/trailing slashes
	for len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	for len(path) > 0 && path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}
	if path == "" {
		return nil
	}
	// Split by slash
	var parts []string
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			if i > start {
				parts = append(parts, path[start:i])
			}
			start = i + 1
		}
	}
	if start < len(path) {
		parts = append(parts, path[start:])
	}
	return parts
}

// GetSnapshotInode retrieves a single inode from a snapshot.
func (db *BunDB) GetSnapshotInode(ctx context.Context, snapshotID string, ino int64) (*SnapshotInodeModel, error) {
	var inode SnapshotInodeModel
	err := db.NewSelect().
		Model(&inode).
		Where("snapshot_id = ?", snapshotID).
		Where("ino = ?", ino).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, common.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &inode, nil
}

// GetSnapshotSymlink retrieves a symlink target from a snapshot.
func (db *BunDB) GetSnapshotSymlink(ctx context.Context, snapshotID string, ino int64) (string, error) {
	var target string
	err := db.NewRaw(`SELECT target FROM snapshot_symlinks WHERE snapshot_id = ? AND ino = ?`, snapshotID, ino).Scan(ctx, &target)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return target, err
}

// GetSnapshotContentChunks retrieves content chunks from a snapshot.
func (db *BunDB) GetSnapshotContentChunks(ctx context.Context, snapshotID string, ino int64) ([]ContentChunkForSnapshot, error) {
	var chunks []ContentChunkForSnapshot
	err := db.NewRaw(`
		SELECT sc.ino, sc.chunk_idx, cb.data
		FROM snapshot_content sc
		JOIN content_blocks cb ON sc.hash = cb.hash
		WHERE sc.snapshot_id = ? AND sc.ino = ?
	`, snapshotID, ino).Scan(ctx, &chunks)
	return chunks, err
}

// --- Garbage Collection Operations ---

// StorageStats holds statistics about storage usage.
type StorageStats struct {
	TotalContentBlocks      int
	TotalContentBlocksBytes int64
	UsedContentBlocks       int   // referenced by at least one snapshot
	UsedContentBlocksBytes  int64
	OrphanedContentBlocks   int
	OrphanedBlocksBytes     int64
	SnapshotCount           int
	DeprecatedEpochCount    int
}

// GarbageCollectContentBlocks removes content blocks not referenced by any snapshot.
// Returns count and total bytes freed.
func (db *BunDB) GarbageCollectContentBlocks(ctx context.Context) (int, int64, error) {
	// First, get size of orphaned blocks
	var orphanedBytes sql.NullInt64
	err := db.NewRaw(`
		SELECT COALESCE(SUM(LENGTH(data)), 0)
		FROM content_blocks
		WHERE hash NOT IN (SELECT DISTINCT hash FROM snapshot_content)
	`).Scan(ctx, &orphanedBytes)
	if err != nil {
		return 0, 0, err
	}

	// Delete orphaned blocks
	result, err := db.NewRaw(`
		DELETE FROM content_blocks
		WHERE hash NOT IN (SELECT DISTINCT hash FROM snapshot_content)
	`).Exec(ctx)
	if err != nil {
		return 0, 0, err
	}
	count, _ := result.RowsAffected()

	bytes := int64(0)
	if orphanedBytes.Valid {
		bytes = orphanedBytes.Int64
	}
	return int(count), bytes, nil
}

// GetOrphanedContentBlocksStats returns stats about orphaned content blocks without deleting.
func (db *BunDB) GetOrphanedContentBlocksStats(ctx context.Context) (count int, bytes int64, err error) {
	type stats struct {
		Count int   `bun:"count"`
		Bytes int64 `bun:"bytes"`
	}
	var s stats
	err = db.NewRaw(`
		SELECT COUNT(*) as count, COALESCE(SUM(LENGTH(data)), 0) as bytes
		FROM content_blocks
		WHERE hash NOT IN (SELECT DISTINCT hash FROM snapshot_content)
	`).Scan(ctx, &s)
	return s.Count, s.Bytes, err
}

// GetStorageStats returns overall storage statistics.
func (db *BunDB) GetStorageStats(ctx context.Context) (*StorageStats, error) {
	stats := &StorageStats{}

	// Total content blocks
	err := db.NewRaw(`SELECT COUNT(*), COALESCE(SUM(LENGTH(data)), 0) FROM content_blocks`).
		Scan(ctx, &stats.TotalContentBlocks, &stats.TotalContentBlocksBytes)
	if err != nil {
		return nil, err
	}

	// Orphaned content blocks
	stats.OrphanedContentBlocks, stats.OrphanedBlocksBytes, err = db.GetOrphanedContentBlocksStats(ctx)
	if err != nil {
		return nil, err
	}

	// Used = Total - Orphaned
	stats.UsedContentBlocks = stats.TotalContentBlocks - stats.OrphanedContentBlocks
	stats.UsedContentBlocksBytes = stats.TotalContentBlocksBytes - stats.OrphanedBlocksBytes

	// Snapshot count
	err = db.NewRaw(`SELECT COUNT(*) FROM snapshots`).Scan(ctx, &stats.SnapshotCount)
	if err != nil {
		return nil, err
	}

	// Deprecated epochs
	err = db.NewRaw(`SELECT COUNT(*) FROM write_epochs WHERE status = 'deprecated'`).
		Scan(ctx, &stats.DeprecatedEpochCount)
	if err != nil {
		return nil, err
	}

	return stats, nil
}

// --- Protected File Operations ---

// AddProtectedFile inserts a new protected file entry.
func (db *BunDB) AddProtectedFile(ctx context.Context, dataFile string) error {
	_, err := db.NewInsert().
		Model(&ProtectedFileModel{
			DataFile:  dataFile,
			CreatedAt: time.Now().Unix(),
		}).
		On("CONFLICT (data_file) DO NOTHING").
		Exec(ctx)
	return err
}

// RemoveProtectedFile deletes a protected file entry by data file path.
func (db *BunDB) RemoveProtectedFile(ctx context.Context, dataFile string) (int64, error) {
	result, err := db.NewDelete().
		Model((*ProtectedFileModel)(nil)).
		Where("data_file = ?", dataFile).
		Exec(ctx)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// GetProtectedFile retrieves a protected file entry by data file path.
func (db *BunDB) GetProtectedFile(ctx context.Context, dataFile string) (*ProtectedFileModel, error) {
	var entry ProtectedFileModel
	err := db.NewSelect().
		Model(&entry).
		Where("data_file = ?", dataFile).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

// ListProtectedFiles retrieves all protected file entries.
func (db *BunDB) ListProtectedFiles(ctx context.Context) ([]ProtectedFileModel, error) {
	var entries []ProtectedFileModel
	err := db.NewSelect().
		Model(&entries).
		Order("created_at DESC").
		Scan(ctx)
	return entries, err
}

// --- Overlay Snapshot Restore Helpers ---

// TombstoneEntry represents a dentry tombstone (parent_ino, name).
type TombstoneEntry struct {
	ParentIno int64
	Name      string
}

// FindTombstonesToOverride finds tombstones created after snapshot that need anti-tombstones.
// These are entries that are currently tombstoned but weren't tombstoned at snapshot time,
// AND don't exist as live entries in the snapshot (those are restored directly).
// Returns entries that should have anti-tombstones (ino=-1) created to unhide source folder files.
//
// Note: LatentFS has TWO types of tombstones:
// 1. Inode tombstones: dentry with ino > 0 pointing to inode with mode=0 (for source-only files)
// 2. Dentry tombstones: dentry with ino=0 (for overlay files that were deleted)
func (db *BunDB) FindTombstonesToOverride(ctx context.Context, sourceEpoch int64, snapshotID string) ([]TombstoneEntry, error) {
	log.Debugf("[BunDB] FindTombstonesToOverride: sourceEpoch=%d snapshotID=%s", sourceEpoch, snapshotID)

	// Debug: count current tombstones (both types)
	var inodeTombstoneCount, dentryTombstoneCount int
	_ = db.NewRaw(`SELECT COUNT(*) FROM dentries d INNER JOIN inodes i ON d.ino = i.ino WHERE d.ino > 0 AND i.mode = 0`).Scan(ctx, &inodeTombstoneCount)
	_ = db.NewRaw(`SELECT COUNT(*) FROM dentries d WHERE d.ino = 0`).Scan(ctx, &dentryTombstoneCount)
	log.Debugf("[BunDB] FindTombstonesToOverride: inode tombstones=%d, dentry tombstones=%d", inodeTombstoneCount, dentryTombstoneCount)

	var result []TombstoneEntry
	err := db.NewRaw(`
		WITH
		current_tombstones AS (
			-- All current tombstones from ready epochs (both dentry and inode tombstones)
			SELECT d.parent_ino, d.name
			FROM dentries d
			INNER JOIN (
				SELECT parent_ino, name, MAX(write_epoch) as max_epoch
				FROM dentries
				WHERE write_epoch IN (SELECT epoch FROM write_epochs WHERE status = 'ready')
				GROUP BY parent_ino, name
			) latest_d ON d.parent_ino = latest_d.parent_ino AND d.name = latest_d.name AND d.write_epoch = latest_d.max_epoch
			LEFT JOIN (
				SELECT ino, mode, write_epoch
				FROM inodes i2
				WHERE (i2.ino, i2.write_epoch) IN (
					SELECT ino, MAX(write_epoch)
					FROM inodes
					WHERE write_epoch IN (SELECT epoch FROM write_epochs WHERE status = 'ready')
					GROUP BY ino
				)
			) i ON d.ino = i.ino
			WHERE d.ino = 0 OR (d.ino > 0 AND i.mode = 0)  -- dentry tombstone OR inode tombstone
		),
		snapshot_tombstones AS (
			-- Tombstones at snapshot time (both types)
			SELECT d.parent_ino, d.name
			FROM dentries d
			INNER JOIN (
				SELECT parent_ino, name, MAX(write_epoch) as max_epoch
				FROM dentries
				WHERE write_epoch <= ?
				GROUP BY parent_ino, name
			) snapshot_d ON d.parent_ino = snapshot_d.parent_ino AND d.name = snapshot_d.name AND d.write_epoch = snapshot_d.max_epoch
			LEFT JOIN (
				SELECT ino, mode, write_epoch
				FROM inodes i2
				WHERE (i2.ino, i2.write_epoch) IN (
					SELECT ino, MAX(write_epoch)
					FROM inodes
					WHERE write_epoch <= ?
					GROUP BY ino
				)
			) i ON d.ino = i.ino
			WHERE d.ino = 0 OR (d.ino > 0 AND i.mode = 0)  -- dentry tombstone OR inode tombstone at snapshot time
		),
		snapshot_live_entries AS (
			-- Live entries in snapshot (these will be restored directly, no anti-tombstone needed)
			SELECT parent_ino, name FROM snapshot_dentries WHERE snapshot_id = ?
		)
		-- Find tombstones to override (current tombstones that weren't tombstones at snapshot time
		-- AND don't exist as live entries in the snapshot)
		-- Using NOT EXISTS instead of NOT IN for better performance
		SELECT parent_ino, name
		FROM current_tombstones ct
		WHERE NOT EXISTS (
			SELECT 1 FROM snapshot_tombstones st
			WHERE st.parent_ino = ct.parent_ino AND st.name = ct.name
		)
		AND NOT EXISTS (
			SELECT 1 FROM snapshot_live_entries sle
			WHERE sle.parent_ino = ct.parent_ino AND sle.name = ct.name
		)
	`, sourceEpoch, sourceEpoch, snapshotID).Scan(ctx, &result)
	if err != nil {
		return nil, fmt.Errorf("failed to find tombstones to override: %w", err)
	}
	log.Debugf("[BunDB] FindTombstonesToOverride: found %d tombstones to override", len(result))
	return result, nil
}

// FindEntriesToHide finds entries created after snapshot that need tombstones.
// These are TRULY live entries (ino > 0 AND inode mode != 0) that exist in the current
// state but don't exist in the snapshot. Entries pointing to inode tombstones (mode=0)
// are excluded since they're already effectively deleted.
// Returns entries that should have tombstones (ino=0) created to hide them.
func (db *BunDB) FindEntriesToHide(ctx context.Context, snapshotID string) ([]TombstoneEntry, error) {
	var result []TombstoneEntry
	err := db.NewRaw(`
		WITH
		current_live AS (
			-- All current TRULY live entries (ino > 0 AND inode not tombstoned) from ready epochs
			SELECT d.parent_ino, d.name
			FROM dentries d
			INNER JOIN (
				SELECT parent_ino, name, MAX(write_epoch) as max_epoch
				FROM dentries
				WHERE write_epoch IN (SELECT epoch FROM write_epochs WHERE status = 'ready')
				GROUP BY parent_ino, name
			) latest ON d.parent_ino = latest.parent_ino AND d.name = latest.name AND d.write_epoch = latest.max_epoch
			INNER JOIN (
				-- Get latest inode version for each ino
				SELECT ino, mode
				FROM inodes i2
				WHERE (i2.ino, i2.write_epoch) IN (
					SELECT ino, MAX(write_epoch)
					FROM inodes
					WHERE write_epoch IN (SELECT epoch FROM write_epochs WHERE status = 'ready')
					GROUP BY ino
				)
			) i ON d.ino = i.ino
			WHERE d.ino > 0 AND i.mode != 0  -- Exclude dentry tombstones (ino=0) AND inode tombstones (mode=0)
		),
		snapshot_entries AS (
			-- Entries in snapshot (from snapshot_dentries)
			SELECT parent_ino, name FROM snapshot_dentries WHERE snapshot_id = ?
		)
		-- Find entries to hide (current live that don't exist in snapshot)
		-- Using NOT EXISTS instead of NOT IN for better performance
		SELECT parent_ino, name
		FROM current_live cl
		WHERE NOT EXISTS (
			SELECT 1 FROM snapshot_entries se
			WHERE se.parent_ino = cl.parent_ino AND se.name = cl.name
		)
	`, snapshotID).Scan(ctx, &result)
	if err != nil {
		return nil, fmt.Errorf("failed to find entries to hide: %w", err)
	}
	return result, nil
}

// FindAntiTombstonesToOverride finds anti-tombstones that should be tombstones at snapshot time.
// This handles the case when restoring to a snapshot where files were already deleted.
// Example: Restore snap1 (A exists) → creates anti-tombstone for A. Then restore snap3 (A deleted) →
// needs to create tombstone to override the anti-tombstone.
func (db *BunDB) FindAntiTombstonesToOverride(ctx context.Context, sourceEpoch int64) ([]TombstoneEntry, error) {
	var result []TombstoneEntry
	err := db.NewRaw(`
		WITH
		current_anti_tombstones AS (
			-- All current anti-tombstones (ino=-1) from ready epochs
			SELECT d.parent_ino, d.name
			FROM dentries d
			INNER JOIN (
				SELECT parent_ino, name, MAX(write_epoch) as max_epoch
				FROM dentries
				WHERE write_epoch IN (SELECT epoch FROM write_epochs WHERE status = 'ready')
				GROUP BY parent_ino, name
			) latest_d ON d.parent_ino = latest_d.parent_ino AND d.name = latest_d.name AND d.write_epoch = latest_d.max_epoch
			WHERE d.ino = -1  -- anti-tombstone
		),
		snapshot_tombstones AS (
			-- Tombstones at snapshot time (both dentry and inode tombstones)
			SELECT d.parent_ino, d.name
			FROM dentries d
			INNER JOIN (
				SELECT parent_ino, name, MAX(write_epoch) as max_epoch
				FROM dentries
				WHERE write_epoch <= ?
				GROUP BY parent_ino, name
			) snapshot_d ON d.parent_ino = snapshot_d.parent_ino AND d.name = snapshot_d.name AND d.write_epoch = snapshot_d.max_epoch
			LEFT JOIN (
				SELECT ino, mode
				FROM inodes i2
				WHERE (i2.ino, i2.write_epoch) IN (
					SELECT ino, MAX(write_epoch)
					FROM inodes
					WHERE write_epoch <= ?
					GROUP BY ino
				)
			) i ON d.ino = i.ino
			WHERE d.ino = 0 OR (d.ino > 0 AND i.mode = 0)  -- dentry tombstone OR inode tombstone
		)
		-- Find anti-tombstones that should be tombstones at snapshot time
		-- Using EXISTS instead of IN for better performance
		SELECT parent_ino, name
		FROM current_anti_tombstones cat
		WHERE EXISTS (
			SELECT 1 FROM snapshot_tombstones st
			WHERE st.parent_ino = cat.parent_ino AND st.name = cat.name
		)
	`, sourceEpoch, sourceEpoch).Scan(ctx, &result)
	if err != nil {
		return nil, fmt.Errorf("failed to find anti-tombstones to override: %w", err)
	}
	log.Debugf("[BunDB] FindAntiTombstonesToOverride: found %d anti-tombstones to override", len(result))
	return result, nil
}

// InsertTombstoneWith creates a tombstone dentry (ino=0) that blocks access to the path.
// This is used during overlay snapshot restore to re-hide files that have anti-tombstones
// but should be tombstoned at snapshot time.
func (db *BunDB) InsertTombstoneWith(idb bun.IDB, ctx context.Context, parentIno int64, name string, epoch int64) error {
	_, err := idb.NewInsert().
		Model(&DentryModel{
			ParentIno:  parentIno,
			Name:       name,
			WriteEpoch: epoch,
			Ino:        0, // tombstone marker: blocks access
		}).
		On("CONFLICT (parent_ino, name, write_epoch) DO UPDATE").
		Set("ino = 0").
		Exec(ctx)
	return err
}

// InsertAntiTombstone creates an anti-tombstone dentry (ino=-1) that tells VFS to passthrough to source folder.
// This is used during overlay snapshot restore to unhide files that were tombstoned after the snapshot.
func (db *BunDB) InsertAntiTombstone(ctx context.Context, parentIno int64, name string, epoch int64) error {
	return db.InsertAntiTombstoneWith(db.DB, ctx, parentIno, name, epoch)
}

// InsertAntiTombstoneWith creates an anti-tombstone using the provided transaction/connection.
func (db *BunDB) InsertAntiTombstoneWith(idb bun.IDB, ctx context.Context, parentIno int64, name string, epoch int64) error {
	_, err := idb.NewInsert().
		Model(&DentryModel{
			ParentIno:  parentIno,
			Name:       name,
			WriteEpoch: epoch,
			Ino:        -1, // anti-tombstone marker: passthrough to source folder
		}).
		On("CONFLICT (parent_ino, name, write_epoch) DO UPDATE").
		Set("ino = -1").
		Exec(ctx)
	return err
}
