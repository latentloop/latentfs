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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/uptrace/bun"
)

// Snapshot represents a point-in-time snapshot of the filesystem
type Snapshot struct {
	ID          string
	Message     string
	Tag         string
	CreatedAt   time.Time
	FileCount   int64 // Number of files in snapshot
	TotalSize   int64 // Total size of all files in bytes
	SourceEpoch int64 // Epoch at which snapshot was taken (for overlay restore)
}

// hashContent computes SHA-256 hash of content data
func hashContent(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// computeHiddenPath checks if target is inside source folder and returns the relative path to hide
// This is used to prevent storing the symlink target in snapshots when target is inside source
func computeHiddenPath(sourceFolder, symlinkTarget string) string {
	if sourceFolder == "" || symlinkTarget == "" {
		return ""
	}

	sourceAbs, err := filepath.Abs(sourceFolder)
	if err != nil {
		return ""
	}
	targetAbs, err := filepath.Abs(symlinkTarget)
	if err != nil {
		return ""
	}

	// Ensure source ends with separator for proper prefix matching
	sourcePrefix := sourceAbs
	if !strings.HasSuffix(sourcePrefix, string(filepath.Separator)) {
		sourcePrefix += string(filepath.Separator)
	}

	// Check if target is inside source
	if !strings.HasPrefix(targetAbs, sourcePrefix) {
		return ""
	}

	// Get relative path from source to target
	relPath, err := filepath.Rel(sourceAbs, targetAbs)
	if err != nil {
		return ""
	}

	// Don't hide if it's the source itself or parent
	if relPath == "." || relPath == ".." || strings.HasPrefix(relPath, ".."+string(filepath.Separator)) {
		return ""
	}

	return relPath
}

// CreateSnapshot creates a new snapshot of the current filesystem state using Write Epoch
// This creates a new epoch for daemon writes and collects data from the old epoch
func (df *DataFile) CreateSnapshot(message, tag string) (*Snapshot, error) {
	ctx := context.Background()

	// Check if tag already exists
	if tag != "" {
		exists, err := df.bunDB.TagExists(ctx, tag)
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, fmt.Errorf("tag %q already exists", tag)
		}
	}

	// Pre-save cleanup: deprecate and GC any orphaned draft epochs from prior failed restores.
	// A draft epoch's data becomes visible once a higher-numbered ready epoch is created
	// (reads use WHERE write_epoch <= readableEpoch, a numeric range, not status-aware).
	// Safe because save is mutex across CLIs — no restore can be in progress.
	if draftEpochs, err := df.bunDB.GetDraftEpochs(ctx); err == nil && len(draftEpochs) > 0 {
		for _, de := range draftEpochs {
			fmt.Fprintf(os.Stderr, "cleaning up orphaned draft epoch %d\n", de)
			if err := df.SetEpochStatus(de, "deprecated"); err != nil {
				return nil, fmt.Errorf("failed to deprecate orphaned draft epoch %d: %w", de, err)
			}
		}
		if _, err := df.CleanupDeprecatedEpochs(); err != nil {
			return nil, fmt.Errorf("failed to GC deprecated epochs: %w", err)
		}
	}

	// Generate snapshot ID
	snapshotID := uuid.New().String()
	now := time.Now().Unix()

	// Step 1: Create new 'ready' epoch for daemon writes and get old epoch for collection
	snapshotEpoch := df.currentWriteEpoch
	log.Debugf("[Snapshot] CreateSnapshot: snapshotEpoch=%d (currentWriteEpoch before save)", snapshotEpoch)
	createdBy := fmt.Sprintf("snapshot_save:%s", snapshotID)
	newEpoch, err := df.CreateWriteEpoch("ready", createdBy)
	if err != nil {
		return nil, fmt.Errorf("failed to create new epoch: %w", err)
	}
	log.Debugf("[Snapshot] CreateSnapshot: newEpoch=%d (created for future writes)", newEpoch)

	// Update current write epoch
	if err := df.SetCurrentWriteEpoch(newEpoch); err != nil {
		return nil, fmt.Errorf("failed to set current epoch: %w", err)
	}

	// Refresh epochs so daemon writes go to new epoch
	if err := df.RefreshEpochs(); err != nil {
		return nil, fmt.Errorf("failed to refresh epochs: %w", err)
	}

	// Step 2: Use Bun transaction for snapshot data collection
	err = df.bunDB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// Insert snapshot metadata (including source_epoch for overlay restore)
		var tagPtr, msgPtr string
		if tag != "" {
			tagPtr = tag
		}
		if message != "" {
			msgPtr = message
		}
		if _, err := tx.NewInsert().Model(&SnapshotModel{
			ID:          snapshotID,
			Message:     msgPtr,
			Tag:         tagPtr,
			CreatedAt:   now,
			SourceEpoch: snapshotEpoch, // Store epoch for overlay restore
		}).Exec(ctx); err != nil {
			return fmt.Errorf("failed to create snapshot: %w", err)
		}

		// Copy latest inodes at snapshot epoch (excluding tombstones with mode=0)
		if _, err := tx.NewRaw(`
			INSERT INTO snapshot_inodes (snapshot_id, ino, mode, uid, gid, size, atime, mtime, ctime, nlink)
			SELECT ?, i.ino, i.mode, i.uid, i.gid, i.size, i.atime, i.mtime, i.ctime, i.nlink
			FROM inodes i
			INNER JOIN (
				SELECT ino, MAX(write_epoch) as max_epoch
				FROM inodes WHERE write_epoch <= ?
				GROUP BY ino
			) latest ON i.ino = latest.ino AND i.write_epoch = latest.max_epoch
			WHERE i.mode != 0
		`, snapshotID, snapshotEpoch).Exec(ctx); err != nil {
			return fmt.Errorf("failed to copy inodes: %w", err)
		}

		// Copy latest dentries at snapshot epoch (excluding tombstones with ino=0)
		if _, err := tx.NewRaw(`
			INSERT INTO snapshot_dentries (snapshot_id, parent_ino, name, ino)
			SELECT ?, d.parent_ino, d.name, d.ino
			FROM dentries d
			INNER JOIN (
				SELECT parent_ino, name, MAX(write_epoch) as max_epoch
				FROM dentries WHERE write_epoch <= ?
				GROUP BY parent_ino, name
			) latest ON d.parent_ino = latest.parent_ino AND d.name = latest.name AND d.write_epoch = latest.max_epoch
			WHERE d.ino != 0
		`, snapshotID, snapshotEpoch).Exec(ctx); err != nil {
			return fmt.Errorf("failed to copy dentries: %w", err)
		}

		// Get latest content chunks at snapshot epoch
		var chunks []ContentChunkForSnapshot
		err := tx.NewRaw(`
			SELECT c.ino, c.chunk_idx, c.data
			FROM content c
			INNER JOIN (
				SELECT ino, chunk_idx, MAX(write_epoch) as max_epoch
				FROM content WHERE write_epoch <= ?
				GROUP BY ino, chunk_idx
			) latest ON c.ino = latest.ino AND c.chunk_idx = latest.chunk_idx AND c.write_epoch = latest.max_epoch
		`, snapshotEpoch).Scan(ctx, &chunks)
		if err != nil {
			return fmt.Errorf("failed to read content: %w", err)
		}

		// Insert content blocks and snapshot references
		for _, c := range chunks {
			hash := hashContent(c.Data)

			// Insert into content_blocks (ignore if already exists)
			if _, err := tx.NewRaw(`INSERT OR IGNORE INTO content_blocks (hash, data) VALUES (?, ?)`,
				hash, c.Data).Exec(ctx); err != nil {
				return fmt.Errorf("failed to insert content block: %w", err)
			}

			// Insert reference in snapshot_content
			if _, err := tx.NewInsert().Model(&SnapshotContentModel{
				SnapshotID: snapshotID,
				Ino:        c.Ino,
				ChunkIdx:   int64(c.ChunkIdx),
				Hash:       hash,
			}).Exec(ctx); err != nil {
				return fmt.Errorf("failed to insert snapshot content reference: %w", err)
			}
		}

		// Copy latest symlinks at snapshot epoch
		if _, err := tx.NewRaw(`
			INSERT INTO snapshot_symlinks (snapshot_id, ino, target)
			SELECT ?, s.ino, s.target
			FROM symlinks s
			INNER JOIN (
				SELECT ino, MAX(write_epoch) as max_epoch
				FROM symlinks WHERE write_epoch <= ?
				GROUP BY ino
			) latest ON s.ino = latest.ino AND s.write_epoch = latest.max_epoch
		`, snapshotID, snapshotEpoch).Exec(ctx); err != nil {
			return fmt.Errorf("failed to copy symlinks: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Note: We intentionally do NOT deprecate the old epoch here.
	// The data at the old epoch is still needed for reads (MVCC).
	// Epochs are only deprecated when data is truly superseded,
	// such as after a restore operation replaces the filesystem state.

	return &Snapshot{
		ID:        snapshotID,
		Message:   message,
		Tag:       tag,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

// SnapshotResult contains the result of a snapshot operation
type SnapshotResult struct {
	Snapshot     *Snapshot
	SkippedFiles []string // Files that couldn't be read (when allowPartial is true)
}

// CreateSnapshotWithSource creates a snapshot including files from source folder (for soft-fork)
// Files in the overlay take precedence over source files
// If allowPartial is true, continues on read errors and reports skipped files
// symlinkTarget is the mount symlink target path - if it's inside sourceFolder, it will be excluded from the snapshot
// Pass empty string for symlinkTarget if the daemon is not running or this info is not available
func (df *DataFile) CreateSnapshotWithSource(message, tag, sourceFolder, symlinkTarget string, allowPartial bool) (*SnapshotResult, error) {
	ctx := context.Background()

	// Check if tag already exists
	if tag != "" {
		exists, err := df.bunDB.TagExists(ctx, tag)
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, fmt.Errorf("tag %q already exists", tag)
		}
	}

	// Pre-save cleanup: deprecate and GC any orphaned draft epochs from prior failed restores.
	// A draft epoch's data becomes visible once a higher-numbered ready epoch is created
	// (reads use WHERE write_epoch <= readableEpoch, a numeric range, not status-aware).
	// Safe because save is mutex across CLIs — no restore can be in progress.
	if draftEpochs, err := df.bunDB.GetDraftEpochs(ctx); err == nil && len(draftEpochs) > 0 {
		for _, de := range draftEpochs {
			fmt.Fprintf(os.Stderr, "cleaning up orphaned draft epoch %d\n", de)
			if err := df.SetEpochStatus(de, "deprecated"); err != nil {
				return nil, fmt.Errorf("failed to deprecate orphaned draft epoch %d: %w", de, err)
			}
		}
		if _, err := df.CleanupDeprecatedEpochs(); err != nil {
			return nil, fmt.Errorf("failed to GC deprecated epochs: %w", err)
		}
	}

	// Generate snapshot ID
	snapshotID := uuid.New().String()
	now := time.Now().Unix()

	// Step 1: Create new 'ready' epoch for daemon writes and get old epoch for collection
	snapshotEpoch := df.currentWriteEpoch
	log.Debugf("[Snapshot] SaveOverlaySnapshot: snapshotEpoch=%d (currentWriteEpoch before save)", snapshotEpoch)
	createdBy := fmt.Sprintf("snapshot_save:%s", snapshotID)
	newEpoch, err := df.CreateWriteEpoch("ready", createdBy)
	if err != nil {
		return nil, fmt.Errorf("failed to create new epoch: %w", err)
	}
	log.Debugf("[Snapshot] SaveOverlaySnapshot: newEpoch=%d (created for future writes)", newEpoch)

	// Update current write epoch
	if err := df.SetCurrentWriteEpoch(newEpoch); err != nil {
		return nil, fmt.Errorf("failed to set current epoch: %w", err)
	}

	// Refresh epochs so daemon writes go to new epoch
	if err := df.RefreshEpochs(); err != nil {
		return nil, fmt.Errorf("failed to refresh epochs: %w", err)
	}

	var skippedFiles []string

	// Step 2: Use Bun transaction for snapshot data collection
	err = df.bunDB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// Insert snapshot metadata (including source_epoch for overlay restore)
		var tagPtr, msgPtr string
		if tag != "" {
			tagPtr = tag
		}
		if message != "" {
			msgPtr = message
		}
		if _, err := tx.NewInsert().Model(&SnapshotModel{
			ID:          snapshotID,
			Message:     msgPtr,
			Tag:         tagPtr,
			CreatedAt:   now,
			SourceEpoch: snapshotEpoch, // Store epoch for overlay restore
		}).Exec(ctx); err != nil {
			return fmt.Errorf("failed to create snapshot: %w", err)
		}

		// Copy latest overlay inodes at snapshot epoch (including tombstones with mode=0 for restore)
		if _, err := tx.NewRaw(`
			INSERT INTO snapshot_inodes (snapshot_id, ino, mode, uid, gid, size, atime, mtime, ctime, nlink)
			SELECT ?, i.ino, i.mode, i.uid, i.gid, i.size, i.atime, i.mtime, i.ctime, i.nlink
			FROM inodes i
			INNER JOIN (
				SELECT ino, MAX(write_epoch) as max_epoch
				FROM inodes WHERE write_epoch <= ?
				GROUP BY ino
			) latest ON i.ino = latest.ino AND i.write_epoch = latest.max_epoch
		`, snapshotID, snapshotEpoch).Exec(ctx); err != nil {
			return fmt.Errorf("failed to copy inodes: %w", err)
		}

		// Copy latest overlay dentries at snapshot epoch (excluding dentry tombstones ino=0, but including inode tombstones mode=0)
		// Inode tombstones (mode=0) represent deleted source files and need to be preserved in snapshots for restore
		if _, err := tx.NewRaw(`
			INSERT INTO snapshot_dentries (snapshot_id, parent_ino, name, ino)
			SELECT ?, d.parent_ino, d.name, d.ino
			FROM dentries d
			INNER JOIN (
				SELECT parent_ino, name, MAX(write_epoch) as max_epoch
				FROM dentries WHERE write_epoch <= ?
				GROUP BY parent_ino, name
			) latest ON d.parent_ino = latest.parent_ino AND d.name = latest.name AND d.write_epoch = latest.max_epoch
			WHERE d.ino != 0
		`, snapshotID, snapshotEpoch).Exec(ctx); err != nil {
			return fmt.Errorf("failed to copy dentries: %w", err)
		}

		// Get latest overlay content at snapshot epoch
		var chunks []ContentChunkForSnapshot
		err := tx.NewRaw(`
			SELECT c.ino, c.chunk_idx, c.data
			FROM content c
			INNER JOIN (
				SELECT ino, chunk_idx, MAX(write_epoch) as max_epoch
				FROM content WHERE write_epoch <= ?
				GROUP BY ino, chunk_idx
			) latest ON c.ino = latest.ino AND c.chunk_idx = latest.chunk_idx AND c.write_epoch = latest.max_epoch
		`, snapshotEpoch).Scan(ctx, &chunks)
		if err != nil {
			return fmt.Errorf("failed to read content: %w", err)
		}

		for _, c := range chunks {
			hash := hashContent(c.Data)
			if _, err := tx.NewRaw(`INSERT OR IGNORE INTO content_blocks (hash, data) VALUES (?, ?)`, hash, c.Data).Exec(ctx); err != nil {
				return fmt.Errorf("failed to insert content block: %w", err)
			}
			if _, err := tx.NewInsert().Model(&SnapshotContentModel{
				SnapshotID: snapshotID,
				Ino:        c.Ino,
				ChunkIdx:   int64(c.ChunkIdx),
				Hash:       hash,
			}).Exec(ctx); err != nil {
				return fmt.Errorf("failed to insert snapshot content reference: %w", err)
			}
		}

		// Copy latest overlay symlinks at snapshot epoch
		if _, err := tx.NewRaw(`
			INSERT INTO snapshot_symlinks (snapshot_id, ino, target)
			SELECT ?, s.ino, s.target
			FROM symlinks s
			INNER JOIN (
				SELECT ino, MAX(write_epoch) as max_epoch
				FROM symlinks WHERE write_epoch <= ?
				GROUP BY ino
			) latest ON s.ino = latest.ino AND s.write_epoch = latest.max_epoch
		`, snapshotID, snapshotEpoch).Exec(ctx); err != nil {
			return fmt.Errorf("failed to copy symlinks: %w", err)
		}

		// Track the next available inode number for source files
		var maxIno int64
		err = tx.NewRaw(`SELECT COALESCE(MAX(ino), 1) FROM snapshot_inodes WHERE snapshot_id = ?`, snapshotID).Scan(ctx, &maxIno)
		if err != nil {
			return err
		}
		nextIno := maxIno + 1

		// Build a map of paths that exist in the overlay
		overlayPaths := make(map[string]bool)
		if err := df.buildOverlayPathMapTx(ctx, tx, snapshotID, overlayPaths); err != nil {
			return fmt.Errorf("failed to build overlay path map: %w", err)
		}

		// Compute hidden path (symlink target inside source folder should be excluded)
		hiddenPath := computeHiddenPath(sourceFolder, symlinkTarget)

		// Walk source folder and add files not in overlay
		sourceInoMap := make(map[string]int64) // path -> ino mapping for source dirs

		walkErr := filepath.Walk(sourceFolder, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				if allowPartial {
					skippedFiles = append(skippedFiles, path+": "+walkErr.Error())
					if info != nil && info.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
				return walkErr
			}

			// Get relative path from source
			relPath, err := filepath.Rel(sourceFolder, path)
			if err != nil {
				return err
			}

			// Skip root
			if relPath == "." {
				return nil
			}

			// Skip hidden files
			baseName := filepath.Base(path)
			if len(baseName) > 0 && baseName[0] == '.' {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if len(baseName) >= 2 && baseName[:2] == "._" {
				return nil
			}

			// Skip if already in overlay
			if overlayPaths[relPath] {
				return nil
			}

			// Skip hidden path (symlink target inside source folder)
			if hiddenPath != "" && (relPath == hiddenPath || strings.HasPrefix(relPath, hiddenPath+string(filepath.Separator))) {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}

			// Also skip the associated .latentfs data file and its SQLite auxiliary files
			if hiddenPath != "" {
				dataFileBase := hiddenPath + ".latentfs"
				if relPath == dataFileBase ||
					relPath == dataFileBase+"-journal" ||
					relPath == dataFileBase+"-shm" ||
					relPath == dataFileBase+"-wal" {
					return nil
				}
			}

			// Get or create parent inode
			parentPath := filepath.Dir(relPath)
			var parentIno int64 = RootIno
			if parentPath != "." {
				if ino, ok := sourceInoMap[parentPath]; ok {
					parentIno = ino
				} else {
					// Parent should have been created already (Walk visits in order)
					// Check if it's in overlay
					if overlayPaths[parentPath] {
						// Get overlay parent ino
						pIno, err := df.getSnapshotInodeByPathTx(ctx, tx, snapshotID, parentPath)
						if err != nil {
							return fmt.Errorf("failed to get overlay parent inode for %s: %w", parentPath, err)
						}
						parentIno = pIno
					}
				}
			}

			if info.IsDir() {
				// Create directory inode in snapshot
				dirMode := int64(ModeDir | 0755)
				mtime := info.ModTime().Unix()
				if _, err := tx.NewInsert().Model(&SnapshotInodeModel{
					SnapshotID: snapshotID,
					Ino:        nextIno,
					Mode:       dirMode,
					UID:        0,
					GID:        0,
					Size:       0,
					Atime:      mtime,
					Mtime:      mtime,
					Ctime:      mtime,
					Nlink:      2,
				}).Exec(ctx); err != nil {
					return fmt.Errorf("failed to create dir inode for %s: %w", relPath, err)
				}

				if _, err := tx.NewInsert().Model(&SnapshotDentryModel{
					SnapshotID: snapshotID,
					ParentIno:  parentIno,
					Name:       baseName,
					Ino:        nextIno,
				}).Exec(ctx); err != nil {
					return fmt.Errorf("failed to create dir dentry for %s: %w", relPath, err)
				}

				sourceInoMap[relPath] = nextIno
				nextIno++
				return nil
			}

			// Handle symlinks
			if info.Mode()&os.ModeSymlink != 0 {
				target, err := os.Readlink(path)
				if err != nil {
					if allowPartial {
						skippedFiles = append(skippedFiles, path+": "+err.Error())
						return nil
					}
					return err
				}

				symlinkMode := int64(ModeSymlink | 0777)
				mtime := info.ModTime().Unix()
				if _, err := tx.NewInsert().Model(&SnapshotInodeModel{
					SnapshotID: snapshotID,
					Ino:        nextIno,
					Mode:       symlinkMode,
					UID:        0,
					GID:        0,
					Size:       int64(len(target)),
					Atime:      mtime,
					Mtime:      mtime,
					Ctime:      mtime,
					Nlink:      1,
				}).Exec(ctx); err != nil {
					return fmt.Errorf("failed to create symlink inode for %s: %w", relPath, err)
				}

				if _, err := tx.NewInsert().Model(&SnapshotDentryModel{
					SnapshotID: snapshotID,
					ParentIno:  parentIno,
					Name:       baseName,
					Ino:        nextIno,
				}).Exec(ctx); err != nil {
					return fmt.Errorf("failed to create symlink dentry for %s: %w", relPath, err)
				}

				if _, err := tx.NewInsert().Model(&SnapshotSymlinkModel{
					SnapshotID: snapshotID,
					Ino:        nextIno,
					Target:     target,
				}).Exec(ctx); err != nil {
					return fmt.Errorf("failed to create symlink target for %s: %w", relPath, err)
				}

				nextIno++
				return nil
			}

			// Regular file - read content
			content, err := os.ReadFile(path)
			if err != nil {
				if allowPartial {
					skippedFiles = append(skippedFiles, path+": "+err.Error())
					return nil
				}
				return err
			}

			fileMode := int64(ModeFile | (int(info.Mode()) & 0777))
			mtime := info.ModTime().Unix()
			if _, err := tx.NewInsert().Model(&SnapshotInodeModel{
				SnapshotID: snapshotID,
				Ino:        nextIno,
				Mode:       fileMode,
				UID:        0,
				GID:        0,
				Size:       int64(len(content)),
				Atime:      mtime,
				Mtime:      mtime,
				Ctime:      mtime,
				Nlink:      1,
			}).Exec(ctx); err != nil {
				return fmt.Errorf("failed to create file inode for %s: %w", relPath, err)
			}

			if _, err := tx.NewInsert().Model(&SnapshotDentryModel{
				SnapshotID: snapshotID,
				ParentIno:  parentIno,
				Name:       baseName,
				Ino:        nextIno,
			}).Exec(ctx); err != nil {
				return fmt.Errorf("failed to create file dentry for %s: %w", relPath, err)
			}

			// Store content in chunks
			fileIno := nextIno
			nextIno++

			for i := 0; i < len(content); i += ChunkSize {
				end := i + ChunkSize
				if end > len(content) {
					end = len(content)
				}
				chunk := content[i:end]
				chunkIdx := i / ChunkSize

				hash := hashContent(chunk)
				if _, err := tx.NewRaw(`INSERT OR IGNORE INTO content_blocks (hash, data) VALUES (?, ?)`, hash, chunk).Exec(ctx); err != nil {
					return fmt.Errorf("failed to insert content block for %s: %w", relPath, err)
				}

				if _, err := tx.NewInsert().Model(&SnapshotContentModel{
					SnapshotID: snapshotID,
					Ino:        fileIno,
					ChunkIdx:   int64(chunkIdx),
					Hash:       hash,
				}).Exec(ctx); err != nil {
					return fmt.Errorf("failed to insert snapshot content for %s: %w", relPath, err)
				}
			}

			return nil
		})

		if walkErr != nil {
			return fmt.Errorf("failed to walk source folder: %w", walkErr)
		}

		// Compute and update file_count and total_size from snapshot_inodes
		// Only count regular files (mode & ModeFile != 0 and not tombstones)
		if _, err := tx.NewRaw(`
			UPDATE snapshots SET
				file_count = (SELECT COUNT(*) FROM snapshot_inodes WHERE snapshot_id = ? AND mode != 0 AND (mode & ?) = ?),
				total_size = COALESCE((SELECT SUM(size) FROM snapshot_inodes WHERE snapshot_id = ? AND mode != 0 AND (mode & ?) = ?), 0)
			WHERE id = ?
		`, snapshotID, ModeMask, ModeFile, snapshotID, ModeMask, ModeFile, snapshotID).Exec(ctx); err != nil {
			return fmt.Errorf("failed to update snapshot stats: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Query the computed stats
	var fileCount, totalSize int64
	_ = df.bunDB.DB.NewRaw(`SELECT file_count, total_size FROM snapshots WHERE id = ?`, snapshotID).
		Scan(ctx, &fileCount, &totalSize)

	// Note: We intentionally do NOT deprecate the old epoch here.
	// The data at the old epoch is still needed for reads (MVCC).
	// Epochs are only deprecated when data is truly superseded,
	// such as after a restore operation replaces the filesystem state.

	return &SnapshotResult{
		Snapshot: &Snapshot{
			ID:        snapshotID,
			Message:   message,
			Tag:       tag,
			CreatedAt: time.Unix(now, 0),
			FileCount: fileCount,
			TotalSize: totalSize,
		},
		SkippedFiles: skippedFiles,
	}, nil
}

// buildOverlayPathMapTx builds a map of all paths that exist in the snapshot (from overlay) within a transaction
func (df *DataFile) buildOverlayPathMapTx(ctx context.Context, tx bun.Tx, snapshotID string, paths map[string]bool) error {
	return df.buildPathsFromInodeTx(ctx, tx, snapshotID, RootIno, "", paths)
}

func (df *DataFile) buildPathsFromInodeTx(ctx context.Context, tx bun.Tx, snapshotID string, parentIno int64, parentPath string, paths map[string]bool) error {
	var entries []SnapshotDentryModel
	err := tx.NewSelect().
		Model(&entries).
		Where("snapshot_id = ?", snapshotID).
		Where("parent_ino = ?", parentIno).
		Scan(ctx)
	if err != nil {
		return err
	}

	for _, e := range entries {
		var path string
		if parentPath == "" {
			path = e.Name
		} else {
			path = parentPath + "/" + e.Name
		}

		// Get mode to check for tombstone or directory
		var mode int64
		err := tx.NewRaw(`SELECT mode FROM snapshot_inodes WHERE snapshot_id = ? AND ino = ?`,
			snapshotID, e.Ino).Scan(ctx, &mode)
		if err != nil {
			return err
		}

		// Add path to map (including tombstones - they need to block source files)
		paths[path] = true

		// Recurse into directories (skip tombstones - they're not directories)
		if mode != ModeDeleted && uint32(mode)&ModeMask == ModeDir {
			if err := df.buildPathsFromInodeTx(ctx, tx, snapshotID, e.Ino, path, paths); err != nil {
				return err
			}
		}
	}

	return nil
}

// getSnapshotInodeByPathTx finds an inode in the snapshot by its path within a transaction
func (df *DataFile) getSnapshotInodeByPathTx(ctx context.Context, tx bun.Tx, snapshotID, path string) (int64, error) {
	parts := strings.Split(path, "/")
	currentIno := int64(RootIno)

	for _, part := range parts {
		if part == "" {
			continue
		}
		var ino int64
		err := tx.NewRaw(`SELECT ino FROM snapshot_dentries WHERE snapshot_id = ? AND parent_ino = ? AND name = ?`,
			snapshotID, currentIno, part).Scan(ctx, &ino)
		if err != nil {
			return 0, err
		}
		currentIno = ino
	}

	return currentIno, nil
}

// ListSnapshots returns all snapshots ordered by creation time (newest first)
func (df *DataFile) ListSnapshots() ([]Snapshot, error) {
	ctx := context.Background()
	models, err := df.bunDB.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}

	snapshots := make([]Snapshot, len(models))
	for i, m := range models {
		snapshots[i] = Snapshot{
			ID:          m.ID,
			Message:     m.Message,
			Tag:         m.Tag,
			CreatedAt:   time.Unix(m.CreatedAt, 0),
			FileCount:   m.FileCount,
			TotalSize:   m.TotalSize,
			SourceEpoch: m.SourceEpoch,
		}
	}
	return snapshots, nil
}

// GetSnapshot returns a snapshot by ID or tag
func (df *DataFile) GetSnapshot(idOrTag string) (*Snapshot, error) {
	ctx := context.Background()

	// Try by exact ID first
	model, err := df.bunDB.GetSnapshot(ctx, idOrTag)
	if err == nil {
		return &Snapshot{
			ID:          model.ID,
			Message:     model.Message,
			Tag:         model.Tag,
			CreatedAt:   time.Unix(model.CreatedAt, 0),
			FileCount:   model.FileCount,
			TotalSize:   model.TotalSize,
			SourceEpoch: model.SourceEpoch,
		}, nil
	}

	// Try by tag
	model, err = df.bunDB.GetSnapshotByTag(ctx, idOrTag)
	if err == nil && model != nil {
		return &Snapshot{
			ID:          model.ID,
			Message:     model.Message,
			Tag:         model.Tag,
			CreatedAt:   time.Unix(model.CreatedAt, 0),
			FileCount:   model.FileCount,
			TotalSize:   model.TotalSize,
			SourceEpoch: model.SourceEpoch,
		}, nil
	}

	// Try by ID prefix
	model, err = df.bunDB.GetSnapshotByPrefix(ctx, idOrTag)
	if err != nil {
		return nil, err
	}
	if model == nil {
		return nil, fmt.Errorf("snapshot %q not found", idOrTag)
	}

	return &Snapshot{
		ID:          model.ID,
		Message:     model.Message,
		Tag:         model.Tag,
		CreatedAt:   time.Unix(model.CreatedAt, 0),
		FileCount:   model.FileCount,
		TotalSize:   model.TotalSize,
		SourceEpoch: model.SourceEpoch,
	}, nil
}

// RestoreResult contains the result of a restore operation
type RestoreResult struct {
	SnapshotID    string
	RestoredPaths []string
}

// RestoreFromSnapshot restores files from a snapshot using Write Epoch
// If paths is empty, all files are restored (full restore with epoch)
// If paths is specified, only those paths are restored (partial restore at current epoch)
func (df *DataFile) RestoreFromSnapshot(snapshotID string, paths []string) (*RestoreResult, error) {
	// Verify snapshot exists
	snapshot, err := df.GetSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}

	result := &RestoreResult{
		SnapshotID:    snapshot.ID,
		RestoredPaths: []string{},
	}

	if len(paths) == 0 {
		// Full restore - use Write Epoch for atomic visibility
		return df.restoreFullSnapshotWithEpoch(snapshot, result)
	}

	// Partial restore - restore specific paths at current epoch
	return df.restorePartialSnapshot(snapshot.ID, paths, result)
}

// restoreFullSnapshotWithEpoch restores entire filesystem using Write Epoch for atomic visibility.
// Phase 1: Create draft epoch (invisible to daemon)
// Phase 2+3: Single transaction — populate snapshot data and switch epoch to ready.
//
//	On failure the transaction rolls back (no partial data), and the defer
//	deprecates the draft epoch so the next save/restore/GC can clean it up.
//
// For overlay (soft-fork) restores with source_epoch, this also:
//   - Creates anti-tombstones (ino=-1) for tombstones created after snapshot
//   - Creates tombstones (ino=0) for entries created after snapshot
//   - Bumps parent mtime to invalidate NFS negative cache
func (df *DataFile) restoreFullSnapshotWithEpoch(snapshot *Snapshot, result *RestoreResult) (_ *RestoreResult, retErr error) {
	ctx := context.Background()
	now := time.Now().Unix()
	snapshotID := snapshot.ID

	// Pre-restore cleanup: deprecate and GC any orphaned draft epochs from prior failed restores.
	// Safe because restore is mutex across CLIs — no other restore can be in progress.
	if draftEpochs, err := df.bunDB.GetDraftEpochs(ctx); err == nil && len(draftEpochs) > 0 {
		for _, de := range draftEpochs {
			fmt.Fprintf(os.Stderr, "cleaning up orphaned draft epoch %d\n", de)
			if err := df.SetEpochStatus(de, "deprecated"); err != nil {
				return nil, fmt.Errorf("failed to deprecate orphaned draft epoch %d: %w", de, err)
			}
		}
		if _, err := df.CleanupDeprecatedEpochs(); err != nil {
			return nil, fmt.Errorf("failed to GC deprecated epochs: %w", err)
		}
	}

	// Find entries to hide (created after snapshot) - needed for both soft-fork and full-fork
	var tombstonesToOverride []TombstoneEntry
	var entriesToHide []TombstoneEntry
	var antiTombstonesToOverride []TombstoneEntry
	// Note: SourceEpoch can be 0 for snapshots taken at initial state (before any writes)
	isOverlayRestore := df.IsSoftFork()

	// Find entries created after snapshot that need to be hidden
	// For soft-fork: use anti-tombstones (fall through to source)
	// For full-fork: use tombstones (hide completely)
	var err error
	entriesToHide, err = df.bunDB.FindEntriesToHide(ctx, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to find entries to hide: %w", err)
	}
	log.Debugf("[Snapshot] restoreFullSnapshotWithEpoch: found %d entries to hide", len(entriesToHide))

	if isOverlayRestore {
		log.Debugf("[Snapshot] restoreFullSnapshotWithEpoch: overlay restore, snapshot.SourceEpoch=%d", snapshot.SourceEpoch)
		// Find tombstones created after snapshot that need anti-tombstones
		// (excludes entries that exist in the snapshot - those are restored directly)
		tombstonesToOverride, err = df.bunDB.FindTombstonesToOverride(ctx, snapshot.SourceEpoch, snapshotID)
		if err != nil {
			return nil, fmt.Errorf("failed to find tombstones to override: %w", err)
		}
		log.Debugf("[Snapshot] restoreFullSnapshotWithEpoch: found %d tombstones to override", len(tombstonesToOverride))
		// Find anti-tombstones that should be tombstones at snapshot time
		// (handles case when restoring to snapshot where files were already deleted)
		antiTombstonesToOverride, err = df.bunDB.FindAntiTombstonesToOverride(ctx, snapshot.SourceEpoch)
		if err != nil {
			return nil, fmt.Errorf("failed to find anti-tombstones to override: %w", err)
		}
		log.Debugf("[Snapshot] restoreFullSnapshotWithEpoch: found %d anti-tombstones to override", len(antiTombstonesToOverride))
	}

	// Phase 1: Create draft epoch (invisible to daemon reads)
	createdBy := fmt.Sprintf("snapshot_restore:%s", snapshotID)
	newEpoch, err := df.CreateWriteEpoch("draft", createdBy)
	if err != nil {
		return nil, fmt.Errorf("failed to create draft epoch: %w", err)
	}

	// On any failure after this point, deprecate the draft epoch for GC cleanup.
	// The transaction rollback handles data rows; this handles the epoch record itself.
	defer func() {
		if retErr != nil {
			df.abortRestoreEpoch(newEpoch)
		}
	}()

	// Phase 2+3: Single transaction — populate data and switch epoch atomically.
	// One fsync, full rollback on any failure (no partial garbage rows).
	err = df.bunDB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// Restore inodes (with +1s offset so NFS clients see mtime change)
		if _, err := tx.NewRaw(`
			INSERT INTO inodes (ino, write_epoch, mode, uid, gid, size, atime, mtime, ctime, nlink)
			SELECT ino, ?, mode, uid, gid, size, unixepoch()+1, unixepoch()+1, unixepoch()+1, nlink
			FROM snapshot_inodes WHERE snapshot_id = ?
		`, newEpoch, snapshotID).Exec(ctx); err != nil {
			return fmt.Errorf("failed to restore inodes: %w", err)
		}

		// Restore dentries
		if _, err := tx.NewRaw(`
			INSERT INTO dentries (parent_ino, name, ino, write_epoch)
			SELECT parent_ino, name, ino, ?
			FROM snapshot_dentries WHERE snapshot_id = ?
		`, newEpoch, snapshotID).Exec(ctx); err != nil {
			return fmt.Errorf("failed to restore dentries: %w", err)
		}

		// Restore content (via content_blocks)
		if _, err := tx.NewRaw(`
			INSERT INTO content (ino, chunk_idx, data, write_epoch)
			SELECT sc.ino, sc.chunk_idx, cb.data, ?
			FROM snapshot_content sc
			JOIN content_blocks cb ON sc.hash = cb.hash
			WHERE sc.snapshot_id = ?
		`, newEpoch, snapshotID).Exec(ctx); err != nil {
			return fmt.Errorf("failed to restore content: %w", err)
		}

		// Restore symlinks
		if _, err := tx.NewRaw(`
			INSERT INTO symlinks (ino, target, write_epoch)
			SELECT ino, target, ?
			FROM snapshot_symlinks WHERE snapshot_id = ?
		`, newEpoch, snapshotID).Exec(ctx); err != nil {
			return fmt.Errorf("failed to restore symlinks: %w", err)
		}

		// Track parent directories that need mtime bump for NFS cache invalidation
		parentDirsToUpdate := make(map[int64]struct{})

		// Hide entries created after snapshot
		if len(entriesToHide) > 0 {
			if isOverlayRestore {
				// For overlay restores: create anti-tombstones (ino=-1)
				// Anti-tombstones tell VFS to fall through to source folder:
				// - If source file exists → file is visible (correct for modified source files)
				// - If source file doesn't exist → not found (correct for new files)
				log.Debugf("[Snapshot] restoreFullSnapshotWithEpoch: creating %d anti-tombstones at newEpoch=%d", len(entriesToHide), newEpoch)
				for _, entry := range entriesToHide {
					if err := df.bunDB.InsertAntiTombstoneWith(tx, ctx, entry.ParentIno, entry.Name, newEpoch); err != nil {
						return fmt.Errorf("failed to create anti-tombstone for hiding %s: %w", entry.Name, err)
					}
					parentDirsToUpdate[entry.ParentIno] = struct{}{}
				}
			} else {
				// For full-fork restores: create tombstones (ino=0)
				// Tombstones completely hide the entries since there's no source folder to fall through to
				log.Debugf("[Snapshot] restoreFullSnapshotWithEpoch: creating %d tombstones at newEpoch=%d", len(entriesToHide), newEpoch)
				for _, entry := range entriesToHide {
					if err := df.bunDB.InsertTombstoneWith(tx, ctx, entry.ParentIno, entry.Name, newEpoch); err != nil {
						return fmt.Errorf("failed to create tombstone for hiding %s: %w", entry.Name, err)
					}
					parentDirsToUpdate[entry.ParentIno] = struct{}{}
				}
			}
		}

		// For overlay restores only: handle tombstones and anti-tombstones
		if isOverlayRestore {
			// Create anti-tombstones (ino=-1) for tombstones created after snapshot
			for _, tomb := range tombstonesToOverride {
				if err := df.bunDB.InsertAntiTombstoneWith(tx, ctx, tomb.ParentIno, tomb.Name, newEpoch); err != nil {
					return fmt.Errorf("failed to create anti-tombstone for %s: %w", tomb.Name, err)
				}
				parentDirsToUpdate[tomb.ParentIno] = struct{}{}
			}
			log.Debugf("[Snapshot] restoreFullSnapshotWithEpoch: created %d anti-tombstones for overriding tombstones", len(tombstonesToOverride))

			// Create tombstones (ino=0) for anti-tombstones that should be tombstones at snapshot time.
			// This handles the case when restoring to an earlier snapshot, then restoring to a later snapshot
			// where files were already deleted before the later snapshot was taken.
			for _, entry := range antiTombstonesToOverride {
				if err := df.bunDB.InsertTombstoneWith(tx, ctx, entry.ParentIno, entry.Name, newEpoch); err != nil {
					return fmt.Errorf("failed to create tombstone for %s: %w", entry.Name, err)
				}
				parentDirsToUpdate[entry.ParentIno] = struct{}{}
			}
			log.Debugf("[Snapshot] restoreFullSnapshotWithEpoch: created %d tombstones for anti-tombstones that should be deleted", len(antiTombstonesToOverride))
		}

		// Bump mtime for parent directories that have tombstones/anti-tombstones
		// This invalidates NFS cache so changes become visible
		for parentIno := range parentDirsToUpdate {
			if _, err := tx.NewRaw(`
				INSERT INTO inodes (ino, write_epoch, mode, uid, gid, size, atime, mtime, ctime, nlink)
				SELECT ino, ?, mode, uid, gid, size, atime, unixepoch()+1, unixepoch()+1, nlink
				FROM inodes i
				WHERE ino = ?
				AND write_epoch = (
					SELECT MAX(write_epoch) FROM inodes
					WHERE ino = ? AND write_epoch <= ?
				)
				ON CONFLICT (ino, write_epoch) DO UPDATE
				SET mtime = unixepoch()+1, ctime = unixepoch()+1
			`, newEpoch, parentIno, parentIno, newEpoch).Exec(ctx); err != nil {
				return fmt.Errorf("failed to bump mtime for parent ino %d: %w", parentIno, err)
			}
		}

		// Set new epoch to ready (makes it visible to daemon reads)
		if _, err := tx.NewUpdate().
			Model((*WriteEpochModel)(nil)).
			Set("status = 'ready'").
			Set("updated_at = ?", now).
			Where("epoch = ?", newEpoch).
			Exec(ctx); err != nil {
			return fmt.Errorf("failed to set new epoch ready: %w", err)
		}

		// Update current write epoch
		if _, err := tx.NewUpdate().
			Model((*ConfigModel)(nil)).
			Set("value = ?", fmt.Sprintf("%d", newEpoch)).
			Where("key = 'current_write_epoch'").
			Exec(ctx); err != nil {
			return fmt.Errorf("failed to update current epoch: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to restore snapshot: %w", err)
	}

	// Refresh epochs to update local cache and clear lookup caches
	// This is necessary because the restore created a new epoch
	if err := df.RefreshEpochs(); err != nil {
		return nil, fmt.Errorf("failed to refresh epochs after restore: %w", err)
	}

	// Collect restored paths
	result.RestoredPaths = df.collectAllPaths()
	return result, nil
}

// abortRestoreEpoch deprecates a draft epoch after a failed restore attempt.
// The actual data cleanup is handled by GC / startup cleanup.
func (df *DataFile) abortRestoreEpoch(epoch int64) {
	const maxRetries = 5
	for i := 0; i < maxRetries; i++ {
		if err := df.SetEpochStatus(epoch, "deprecated"); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to deprecate draft epoch %d (attempt %d/%d): %v\n", epoch, i+1, maxRetries, err)
			time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
			continue
		}
		return
	}
	fmt.Fprintf(os.Stderr, "error: exhausted retries deprecating draft epoch %d; GC/startup cleanup should handle it\n", epoch)
}

// restorePartialSnapshot restores only specific paths from a snapshot at current epoch
func (df *DataFile) restorePartialSnapshot(snapshotID string, paths []string, result *RestoreResult) (*RestoreResult, error) {
	for _, path := range paths {
		path = normalizePath(path)
		if path == "" || path == "/" {
			continue
		}

		// Find the inode for this path in the snapshot
		ino, err := df.resolvePathInSnapshot(snapshotID, path)
		if err != nil {
			// Path not found in snapshot - skip
			continue
		}

		// Restore this file/directory (each file is independent)
		if err := df.restoreInodeFromSnapshot(snapshotID, path, ino); err != nil {
			return nil, fmt.Errorf("failed to restore %s: %w", path, err)
		}

		result.RestoredPaths = append(result.RestoredPaths, path)
	}

	return result, nil
}

// resolvePathInSnapshot resolves a path to an inode in a snapshot
func (df *DataFile) resolvePathInSnapshot(snapshotID, path string) (int64, error) {
	ctx := context.Background()
	return df.bunDB.GetSnapshotInodeByPath(ctx, snapshotID, path)
}

// restoreInodeFromSnapshot restores a single inode and its content from a snapshot
func (df *DataFile) restoreInodeFromSnapshot(snapshotID, path string, snapshotIno int64) error {
	ctx := context.Background()

	// Get snapshot inode info
	snapshotInode, err := df.bunDB.GetSnapshotInode(ctx, snapshotID, snapshotIno)
	if err != nil {
		return err
	}

	// Parse path to get parent and name
	parts := splitPath(path)
	if len(parts) == 0 {
		return nil // Can't restore root
	}
	name := parts[len(parts)-1]
	parentPath := "/" + strings.Join(parts[:len(parts)-1], "/")

	// Get parent inode in current filesystem
	parentIno, err := df.ResolvePath(parentPath)
	if err != nil {
		// Parent doesn't exist - create it
		if err := df.ensureParentPath(parentPath); err != nil {
			return err
		}
		parentIno, _ = df.ResolvePath(parentPath)
	}

	// Check if file already exists
	existingDentry, _ := df.Lookup(parentIno, name)
	if existingDentry != nil {
		// Delete existing file
		df.DeleteInode(existingDentry.Ino)
		df.DeleteDentry(parentIno, name)
	}

	// Create new inode
	newIno, err := df.CreateInode(uint32(snapshotInode.Mode))
	if err != nil {
		return err
	}

	// Update inode attributes
	// Use current time + 1 second for mtime/atime instead of snapshot timestamps
	// This ensures NFS clients detect the change (NFS uses mtime for cache validation)
	// and build systems (make, etc.) correctly rebuild after restore
	// The +1 second offset ensures mtime is strictly newer even if restore happens in
	// the same second as the previous write
	sizeI := snapshotInode.Size
	now := time.Now().Add(time.Second)
	df.UpdateInode(newIno, &InodeUpdate{
		Size:  &sizeI,
		Atime: &now,
		Mtime: &now,
	})

	// Create dentry
	if err := df.CreateDentry(parentIno, name, newIno); err != nil {
		return err
	}

	// Copy content from snapshot
	chunks, err := df.bunDB.GetSnapshotContentChunks(ctx, snapshotID, snapshotIno)
	if err != nil {
		return err
	}

	// Insert the collected chunks using WriteContent (epoch-aware)
	for _, c := range chunks {
		if err := df.WriteContent(newIno, int64(c.ChunkIdx*ChunkSize), c.Data); err != nil {
			return err
		}
	}

	// Copy symlink if applicable
	target, err := df.bunDB.GetSnapshotSymlink(ctx, snapshotID, snapshotIno)
	if err == nil && target != "" {
		df.CreateSymlink(newIno, target)
	}

	return nil
}

// ensureParentPath creates parent directories if they don't exist
func (df *DataFile) ensureParentPath(path string) error {
	parts := splitPath(path)
	currentIno := int64(RootIno)

	for _, part := range parts {
		dentry, err := df.Lookup(currentIno, part)
		if err != nil {
			// Create directory
			ino, err := df.CreateInode(DefaultDirMode)
			if err != nil {
				return err
			}
			if err := df.CreateDentry(currentIno, part, ino); err != nil {
				return err
			}
			currentIno = ino
		} else {
			currentIno = dentry.Ino
		}
	}

	return nil
}

// collectAllPaths collects all file paths in the current filesystem
func (df *DataFile) collectAllPaths() []string {
	var paths []string
	df.collectPathsRecursive(RootIno, "", &paths)
	return paths
}

func (df *DataFile) collectPathsRecursive(ino int64, prefix string, paths *[]string) {
	entries, err := df.ListDir(ino)
	if err != nil {
		return
	}

	for _, e := range entries {
		path := prefix + "/" + e.Name
		*paths = append(*paths, path)

		inode, _ := df.GetInode(e.Ino)
		if inode != nil && inode.IsDir() {
			df.collectPathsRecursive(e.Ino, path, paths)
		}
	}
}

// normalizePath normalizes a file path
func normalizePath(path string) string {
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	// Remove trailing slash unless it's root
	if len(path) > 1 && strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
	}
	return path
}

// splitPath splits a path into components
func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

// CollectAllSnapshotPaths returns all file paths in the most recent snapshot.
// Useful for testing. Returns paths like "/src/main.go", "/README.md", etc.
func (df *DataFile) CollectAllSnapshotPaths() []string {
	snapshots, err := df.ListSnapshots()
	if err != nil || len(snapshots) == 0 {
		return nil
	}
	snapshotID := snapshots[0].ID // newest first
	var paths []string
	df.collectSnapshotPathsRecursive(snapshotID, RootIno, "", &paths)
	return paths
}

func (df *DataFile) collectSnapshotPathsRecursive(snapshotID string, parentIno int64, prefix string, paths *[]string) {
	ctx := context.Background()
	entries, err := df.bunDB.ListSnapshotDentriesForParent(ctx, snapshotID, parentIno)
	if err != nil {
		return
	}
	for _, e := range entries {
		path := prefix + "/" + e.Name
		*paths = append(*paths, path)

		mode, err := df.bunDB.GetSnapshotInodeMode(ctx, snapshotID, e.Ino)
		if err == nil && uint32(mode)&ModeMask == ModeDir {
			df.collectSnapshotPathsRecursive(snapshotID, e.Ino, path, paths)
		}
	}
}

// FileFilter returns true if the file/dir at relPath should be INCLUDED in the snapshot.
type FileFilter func(relPath string, isDir bool) bool

// CreateSnapshotFromDir creates a snapshot by walking a source directory,
// filtering through the provided filter. Unlike CreateSnapshotWithSource,
// this has no overlay/source split — it captures the directory directly.
// This is used by autosave to snapshot a project directory.
func (df *DataFile) CreateSnapshotFromDir(sourceDir string, filter FileFilter, message string) (*SnapshotResult, error) {
	ctx := context.Background()

	// Generate snapshot ID
	snapshotID := uuid.New().String()
	now := time.Now().Unix()

	// Create new epoch
	snapshotEpoch := df.currentWriteEpoch
	createdBy := fmt.Sprintf("snapshot_dir:%s", snapshotID)
	newEpoch, err := df.CreateWriteEpoch("ready", createdBy)
	if err != nil {
		return nil, fmt.Errorf("failed to create new epoch: %w", err)
	}
	if err := df.SetCurrentWriteEpoch(newEpoch); err != nil {
		return nil, fmt.Errorf("failed to set current epoch: %w", err)
	}
	if err := df.RefreshEpochs(); err != nil {
		return nil, fmt.Errorf("failed to refresh epochs: %w", err)
	}

	var skippedFiles []string
	var fileCount int64
	var totalSize int64

	err = df.bunDB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// Insert snapshot metadata (file_count and total_size will be updated at end)
		var msgPtr string
		if message != "" {
			msgPtr = message
		}
		if _, err := tx.NewInsert().Model(&SnapshotModel{
			ID:          snapshotID,
			Message:     msgPtr,
			CreatedAt:   now,
			SourceEpoch: snapshotEpoch, // Store epoch for overlay restore
		}).Exec(ctx); err != nil {
			return fmt.Errorf("failed to create snapshot: %w", err)
		}

		var nextIno int64 = RootIno + 1
		dirInoMap := make(map[string]int64) // relPath -> ino for directories

		walkErr := filepath.Walk(sourceDir, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				skippedFiles = append(skippedFiles, path+": "+walkErr.Error())
				if info != nil && info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}

			relPath, err := filepath.Rel(sourceDir, path)
			if err != nil {
				return err
			}
			if relPath == "." {
				return nil
			}

			// Apply filter
			if filter != nil && !filter(relPath, info.IsDir()) {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}

			// Determine parent inode
			parentPath := filepath.Dir(relPath)
			var parentIno int64 = RootIno
			if parentPath != "." {
				if ino, ok := dirInoMap[parentPath]; ok {
					parentIno = ino
				}
			}

			baseName := filepath.Base(relPath)
			mtime := info.ModTime().Unix()

			if info.IsDir() {
				dirMode := int64(ModeDir | 0755)
				if _, err := tx.NewInsert().Model(&SnapshotInodeModel{
					SnapshotID: snapshotID,
					Ino:        nextIno,
					Mode:       dirMode,
					Size:       0,
					Atime:      mtime,
					Mtime:      mtime,
					Ctime:      mtime,
					Nlink:      2,
				}).Exec(ctx); err != nil {
					return fmt.Errorf("failed to create dir inode for %s: %w", relPath, err)
				}
				if _, err := tx.NewInsert().Model(&SnapshotDentryModel{
					SnapshotID: snapshotID,
					ParentIno:  parentIno,
					Name:       baseName,
					Ino:        nextIno,
				}).Exec(ctx); err != nil {
					return fmt.Errorf("failed to create dir dentry for %s: %w", relPath, err)
				}
				dirInoMap[relPath] = nextIno
				nextIno++
				return nil
			}

			// Handle symlinks
			if info.Mode()&os.ModeSymlink != 0 {
				target, err := os.Readlink(path)
				if err != nil {
					skippedFiles = append(skippedFiles, path+": "+err.Error())
					return nil
				}
				symlinkMode := int64(ModeSymlink | 0777)
				if _, err := tx.NewInsert().Model(&SnapshotInodeModel{
					SnapshotID: snapshotID,
					Ino:        nextIno,
					Mode:       symlinkMode,
					Size:       int64(len(target)),
					Atime:      mtime,
					Mtime:      mtime,
					Ctime:      mtime,
					Nlink:      1,
				}).Exec(ctx); err != nil {
					return fmt.Errorf("failed to create symlink inode for %s: %w", relPath, err)
				}
				if _, err := tx.NewInsert().Model(&SnapshotDentryModel{
					SnapshotID: snapshotID,
					ParentIno:  parentIno,
					Name:       baseName,
					Ino:        nextIno,
				}).Exec(ctx); err != nil {
					return fmt.Errorf("failed to create symlink dentry for %s: %w", relPath, err)
				}
				if _, err := tx.NewInsert().Model(&SnapshotSymlinkModel{
					SnapshotID: snapshotID,
					Ino:        nextIno,
					Target:     target,
				}).Exec(ctx); err != nil {
					return fmt.Errorf("failed to create symlink target for %s: %w", relPath, err)
				}
				nextIno++
				return nil
			}

			// Regular file
			content, err := os.ReadFile(path)
			if err != nil {
				skippedFiles = append(skippedFiles, path+": "+err.Error())
				return nil
			}

			// Track file count and total size
			fileCount++
			totalSize += int64(len(content))

			fileMode := int64(ModeFile | (int(info.Mode()) & 0777))
			if _, err := tx.NewInsert().Model(&SnapshotInodeModel{
				SnapshotID: snapshotID,
				Ino:        nextIno,
				Mode:       fileMode,
				Size:       int64(len(content)),
				Atime:      mtime,
				Mtime:      mtime,
				Ctime:      mtime,
				Nlink:      1,
			}).Exec(ctx); err != nil {
				return fmt.Errorf("failed to create file inode for %s: %w", relPath, err)
			}
			if _, err := tx.NewInsert().Model(&SnapshotDentryModel{
				SnapshotID: snapshotID,
				ParentIno:  parentIno,
				Name:       baseName,
				Ino:        nextIno,
			}).Exec(ctx); err != nil {
				return fmt.Errorf("failed to create file dentry for %s: %w", relPath, err)
			}

			fileIno := nextIno
			nextIno++

			for i := 0; i < len(content); i += ChunkSize {
				end := i + ChunkSize
				if end > len(content) {
					end = len(content)
				}
				chunk := content[i:end]
				chunkIdx := i / ChunkSize

				hash := hashContent(chunk)
				if _, err := tx.NewRaw(`INSERT OR IGNORE INTO content_blocks (hash, data) VALUES (?, ?)`, hash, chunk).Exec(ctx); err != nil {
					return fmt.Errorf("failed to insert content block for %s: %w", relPath, err)
				}
				if _, err := tx.NewInsert().Model(&SnapshotContentModel{
					SnapshotID: snapshotID,
					Ino:        fileIno,
					ChunkIdx:   int64(chunkIdx),
					Hash:       hash,
				}).Exec(ctx); err != nil {
					return fmt.Errorf("failed to insert snapshot content for %s: %w", relPath, err)
				}
			}

			return nil
		})

		if walkErr != nil {
			return fmt.Errorf("failed to walk source dir: %w", walkErr)
		}

		// Update snapshot with file count and total size
		if _, err := tx.NewUpdate().Model(&SnapshotModel{}).
			Set("file_count = ?", fileCount).
			Set("total_size = ?", totalSize).
			Where("id = ?", snapshotID).
			Exec(ctx); err != nil {
			return fmt.Errorf("failed to update snapshot stats: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &SnapshotResult{
		Snapshot: &Snapshot{
			ID:        snapshotID,
			Message:   message,
			CreatedAt: time.Unix(now, 0),
			FileCount: fileCount,
			TotalSize: totalSize,
		},
		SkippedFiles: skippedFiles,
	}, nil
}

// DeleteSnapshot deletes a snapshot and all its data
func (df *DataFile) DeleteSnapshot(idOrTag string) error {
	snapshot, err := df.GetSnapshot(idOrTag)
	if err != nil {
		return err
	}

	ctx := context.Background()
	return df.bunDB.DeleteSnapshotData(ctx, snapshot.ID)
}
