package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/uptrace/bun"
)

// BulkCopyConfig configures the bulk copy behavior
type BulkCopyConfig struct {
	// BatchSize is the number of files to batch per transaction (default: 100)
	BatchSize int
	// ChunkBatchSize is the number of chunks to batch per multi-value INSERT (default: 50)
	ChunkBatchSize int
	// SkipHidden skips hidden files (starting with '.' or '._')
	SkipHidden bool
	// AllowPartial continues on read errors and collects skipped files
	AllowPartial bool
	// StreamLargeFiles streams files larger than this threshold (0 = load all in memory)
	StreamLargeFilesThreshold int64
	// Filter is an optional file filter function. If provided, only files for which
	// Filter(relPath, isDir) returns true will be included.
	// If nil, all files (respecting SkipHidden) are included.
	Filter FileFilter
}

// DefaultBulkCopyConfig returns the default configuration
func DefaultBulkCopyConfig() BulkCopyConfig {
	return BulkCopyConfig{
		BatchSize:                 100,
		ChunkBatchSize:            50,
		SkipHidden:                true,
		AllowPartial:              false,
		StreamLargeFilesThreshold: 0, // disabled by default for now
	}
}

// BulkCopyResult contains the result of a bulk copy operation
type BulkCopyResult struct {
	TotalFiles   int
	CopiedFiles  int
	TotalBytes   int64
	CopiedBytes  int64
	SkippedFiles []string
	Duration     time.Duration
}

// BulkCopier handles efficient bulk copying of files to a DataFile
type BulkCopier struct {
	df     *DataFile
	config BulkCopyConfig
	result *BulkCopyResult
}

// FileItem represents a file to be copied
type FileItem struct {
	RelPath    string      // Relative path from source root
	SrcPath    string      // Absolute source path
	Info       os.FileInfo // File info
	Content    []byte      // File content (nil for directories/symlinks)
	LinkTarget string      // Symlink target (only for symlinks)
}

// NewBulkCopier creates a new BulkCopier with the given configuration
func NewBulkCopier(df *DataFile, config BulkCopyConfig) *BulkCopier {
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.ChunkBatchSize <= 0 {
		config.ChunkBatchSize = 50
	}
	return &BulkCopier{
		df:     df,
		config: config,
		result: &BulkCopyResult{},
	}
}

// CopyFromDirectory copies all files from a source directory into the DataFile
func (bc *BulkCopier) CopyFromDirectory(sourcePath string) (*BulkCopyResult, error) {
	start := time.Now()

	// Resolve symlinks in source path
	resolvedSource, err := filepath.EvalSymlinks(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve source path: %w", err)
	}

	// Optimize PRAGMAs for bulk operations
	if err := bc.optimizePragmas(); err != nil {
		return nil, fmt.Errorf("failed to optimize PRAGMAs: %w", err)
	}
	defer bc.restorePragmas()

	// Collect all files first (allows for better batching)
	var files []FileItem
	err = filepath.Walk(resolvedSource, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			if bc.config.AllowPartial {
				bc.result.SkippedFiles = append(bc.result.SkippedFiles, path+": "+walkErr.Error())
				if info != nil && info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			return walkErr
		}

		// Get relative path from source
		relPath, err := filepath.Rel(resolvedSource, path)
		if err != nil {
			return err
		}

		// Skip root
		if relPath == "." {
			return nil
		}

		// Skip hidden files if configured
		if bc.config.SkipHidden {
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
		}

		// Apply custom filter if provided
		if bc.config.Filter != nil {
			if !bc.config.Filter(relPath, info.IsDir()) {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}

		item := FileItem{
			RelPath: "/" + relPath,
			SrcPath: path,
			Info:    info,
		}

		// For regular files, read content
		if info.Mode().IsRegular() {
			content, err := os.ReadFile(path)
			if err != nil {
				if bc.config.AllowPartial {
					bc.result.SkippedFiles = append(bc.result.SkippedFiles, path+": "+err.Error())
					return nil
				}
				return err
			}
			item.Content = content
			bc.result.TotalBytes += int64(len(content))
		}

		// For symlinks, read target
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				if bc.config.AllowPartial {
					bc.result.SkippedFiles = append(bc.result.SkippedFiles, path+": "+err.Error())
					return nil
				}
				return err
			}
			item.LinkTarget = target
		}

		files = append(files, item)
		bc.result.TotalFiles++
		return nil
	})

	if err != nil {
		return bc.result, err
	}

	// Copy files in batches
	if err := bc.copyFilesBatched(files); err != nil {
		return bc.result, err
	}

	bc.result.Duration = time.Since(start)
	return bc.result, nil
}

// optimizePragmas sets PRAGMA values optimized for bulk operations
func (bc *BulkCopier) optimizePragmas() error {
	ctx := context.Background()
	// These PRAGMAs are safe to set with libsql
	pragmas := []string{
		"PRAGMA cache_size = -65536", // 64MB cache
		"PRAGMA temp_store = MEMORY", // RAM for temp tables
	}
	for _, pragma := range pragmas {
		// Use ExecContext for PRAGMAs via Bun
		if _, err := bc.df.bunDB.ExecContext(ctx, pragma); err != nil {
			// Non-fatal: continue without this optimization
			continue
		}
	}
	return nil
}

// restorePragmas restores default PRAGMA values after bulk operations
func (bc *BulkCopier) restorePragmas() {
	ctx := context.Background()
	// Reset to more conservative values for normal operations
	bc.df.bunDB.ExecContext(ctx, "PRAGMA cache_size = -2000")
	bc.df.bunDB.ExecContext(ctx, "PRAGMA temp_store = DEFAULT")
}

// copyFilesBatched copies files in batches with transactions
func (bc *BulkCopier) copyFilesBatched(files []FileItem) error {
	// Process files in batches
	for i := 0; i < len(files); i += bc.config.BatchSize {
		end := min(i+bc.config.BatchSize, len(files))
		batch := files[i:end]

		if err := bc.processBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

// processBatch processes a batch of files in a single transaction
func (bc *BulkCopier) processBatch(batch []FileItem) error {
	ctx := context.Background()

	// Directory cache to avoid repeated lookups
	dirCache := make(map[string]int64)
	dirCache["/"] = RootIno

	// Use Bun transaction
	return bc.df.bunDB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// Process each file in the batch
		for _, item := range batch {
			if err := bc.processItem(ctx, tx, item, dirCache); err != nil {
				return err
			}
			bc.result.CopiedFiles++
			bc.result.CopiedBytes += int64(len(item.Content))
		}
		return nil
	})
}

// processItem processes a single file/directory/symlink
func (bc *BulkCopier) processItem(
	ctx context.Context,
	tx bun.Tx,
	item FileItem,
	dirCache map[string]int64,
) error {
	// Ensure parent directory exists
	parentPath := filepath.Dir(item.RelPath)
	parentIno, err := bc.ensureDir(ctx, tx, parentPath, dirCache)
	if err != nil {
		return err
	}

	if item.Info.IsDir() {
		return bc.processDir(ctx, tx, item, parentIno, dirCache)
	}

	if item.Info.Mode()&os.ModeSymlink != 0 {
		return bc.processSymlink(ctx, tx, item, parentIno)
	}

	return bc.processFile(ctx, tx, item, parentIno)
}

// ensureDir ensures a directory path exists, creating missing directories
func (bc *BulkCopier) ensureDir(
	ctx context.Context,
	tx bun.Tx,
	dirPath string,
	dirCache map[string]int64,
) (int64, error) {
	// Check cache first
	if ino, ok := dirCache[dirPath]; ok {
		return ino, nil
	}

	// Check if directory already exists in database
	ino, err := bc.df.ResolvePath(dirPath)
	if err == nil {
		dirCache[dirPath] = ino
		return ino, nil
	}

	// Need to create - ensure parent exists first
	parentPath := filepath.Dir(dirPath)
	if parentPath == dirPath {
		// We're at root
		return RootIno, nil
	}

	parentIno, err := bc.ensureDir(ctx, tx, parentPath, dirCache)
	if err != nil {
		return 0, err
	}

	// Create directory inode
	now := time.Now().Unix()
	uid := os.Getuid()
	gid := os.Getgid()
	mode := int64(ModeDir | 0755)

	// Get next ino
	var maxIno int64
	err = tx.NewRaw(`SELECT COALESCE(MAX(ino), 1) FROM inodes`).Scan(ctx, &maxIno)
	if err != nil {
		return 0, err
	}
	newIno := maxIno + 1

	// Insert inode using Bun
	if _, err := tx.NewInsert().Model(&InodeModel{
		Ino:        newIno,
		WriteEpoch: bc.df.currentWriteEpoch,
		Mode:       mode,
		UID:        int64(uid),
		GID:        int64(gid),
		Size:       0,
		Atime:      now,
		Mtime:      now,
		Ctime:      now,
		Nlink:      2,
	}).On("CONFLICT (ino, write_epoch) DO UPDATE").
		Set("mode = EXCLUDED.mode, size = EXCLUDED.size, mtime = EXCLUDED.mtime, ctime = EXCLUDED.ctime").
		Exec(ctx); err != nil {
		return 0, err
	}

	// Insert dentry using Bun
	name := filepath.Base(dirPath)
	if _, err := tx.NewInsert().Model(&DentryModel{
		ParentIno:  parentIno,
		Name:       name,
		Ino:        newIno,
		WriteEpoch: bc.df.currentWriteEpoch,
	}).On("CONFLICT (parent_ino, name, write_epoch) DO UPDATE").
		Set("ino = EXCLUDED.ino").
		Exec(ctx); err != nil {
		return 0, err
	}

	dirCache[dirPath] = newIno
	return newIno, nil
}

// processDir processes a directory item
func (bc *BulkCopier) processDir(
	ctx context.Context,
	tx bun.Tx,
	item FileItem,
	parentIno int64,
	dirCache map[string]int64,
) error {
	// Check if already exists
	if _, ok := dirCache[item.RelPath]; ok {
		return nil
	}

	ino, err := bc.df.ResolvePath(item.RelPath)
	if err == nil {
		dirCache[item.RelPath] = ino
		return nil
	}

	// Create directory
	now := time.Now().Unix()
	uid := os.Getuid()
	gid := os.Getgid()
	mode := int64(ModeDir | (int(item.Info.Mode()) & 0777))

	// Get next ino
	var maxIno int64
	err = tx.NewRaw(`SELECT COALESCE(MAX(ino), 1) FROM inodes`).Scan(ctx, &maxIno)
	if err != nil {
		return err
	}
	newIno := maxIno + 1

	// Insert inode using Bun
	if _, err := tx.NewInsert().Model(&InodeModel{
		Ino:        newIno,
		WriteEpoch: bc.df.currentWriteEpoch,
		Mode:       mode,
		UID:        int64(uid),
		GID:        int64(gid),
		Size:       0,
		Atime:      now,
		Mtime:      now,
		Ctime:      now,
		Nlink:      2,
	}).On("CONFLICT (ino, write_epoch) DO UPDATE").
		Set("mode = EXCLUDED.mode, size = EXCLUDED.size, mtime = EXCLUDED.mtime, ctime = EXCLUDED.ctime").
		Exec(ctx); err != nil {
		return err
	}

	// Insert dentry using Bun
	name := filepath.Base(item.RelPath)
	if _, err := tx.NewInsert().Model(&DentryModel{
		ParentIno:  parentIno,
		Name:       name,
		Ino:        newIno,
		WriteEpoch: bc.df.currentWriteEpoch,
	}).On("CONFLICT (parent_ino, name, write_epoch) DO UPDATE").
		Set("ino = EXCLUDED.ino").
		Exec(ctx); err != nil {
		return err
	}

	dirCache[item.RelPath] = newIno
	return nil
}

// processSymlink processes a symlink item
func (bc *BulkCopier) processSymlink(
	ctx context.Context,
	tx bun.Tx,
	item FileItem,
	parentIno int64,
) error {
	now := time.Now().Unix()
	uid := os.Getuid()
	gid := os.Getgid()
	mode := int64(ModeSymlink | 0777)

	// Get next ino
	var maxIno int64
	err := tx.NewRaw(`SELECT COALESCE(MAX(ino), 1) FROM inodes`).Scan(ctx, &maxIno)
	if err != nil {
		return err
	}
	newIno := maxIno + 1

	// Insert inode using Bun
	if _, err := tx.NewInsert().Model(&InodeModel{
		Ino:        newIno,
		WriteEpoch: bc.df.currentWriteEpoch,
		Mode:       mode,
		UID:        int64(uid),
		GID:        int64(gid),
		Size:       0,
		Atime:      now,
		Mtime:      now,
		Ctime:      now,
		Nlink:      1,
	}).On("CONFLICT (ino, write_epoch) DO UPDATE").
		Set("mode = EXCLUDED.mode, size = EXCLUDED.size, mtime = EXCLUDED.mtime, ctime = EXCLUDED.ctime").
		Exec(ctx); err != nil {
		return err
	}

	// Insert dentry using Bun
	name := filepath.Base(item.RelPath)
	if _, err := tx.NewInsert().Model(&DentryModel{
		ParentIno:  parentIno,
		Name:       name,
		Ino:        newIno,
		WriteEpoch: bc.df.currentWriteEpoch,
	}).On("CONFLICT (parent_ino, name, write_epoch) DO UPDATE").
		Set("ino = EXCLUDED.ino").
		Exec(ctx); err != nil {
		return err
	}

	// Insert symlink target using Bun
	if _, err := tx.NewInsert().Model(&SymlinkModel{
		Ino:        newIno,
		WriteEpoch: bc.df.currentWriteEpoch,
		Target:     item.LinkTarget,
	}).On("CONFLICT (ino, write_epoch) DO UPDATE").
		Set("target = EXCLUDED.target").
		Exec(ctx); err != nil {
		return err
	}

	return nil
}

// processFile processes a regular file item
func (bc *BulkCopier) processFile(
	ctx context.Context,
	tx bun.Tx,
	item FileItem,
	parentIno int64,
) error {
	now := time.Now().Unix()
	uid := os.Getuid()
	gid := os.Getgid()
	mode := int64(ModeFile | (int(item.Info.Mode()) & 0777))
	size := int64(len(item.Content))

	// Get next ino
	var maxIno int64
	err := tx.NewRaw(`SELECT COALESCE(MAX(ino), 1) FROM inodes`).Scan(ctx, &maxIno)
	if err != nil {
		return err
	}
	newIno := maxIno + 1

	// Insert inode using Bun
	if _, err := tx.NewInsert().Model(&InodeModel{
		Ino:        newIno,
		WriteEpoch: bc.df.currentWriteEpoch,
		Mode:       mode,
		UID:        int64(uid),
		GID:        int64(gid),
		Size:       size,
		Atime:      now,
		Mtime:      now,
		Ctime:      now,
		Nlink:      1,
	}).On("CONFLICT (ino, write_epoch) DO UPDATE").
		Set("mode = EXCLUDED.mode, size = EXCLUDED.size, mtime = EXCLUDED.mtime, ctime = EXCLUDED.ctime").
		Exec(ctx); err != nil {
		return err
	}

	// Insert dentry using Bun
	name := filepath.Base(item.RelPath)
	if _, err := tx.NewInsert().Model(&DentryModel{
		ParentIno:  parentIno,
		Name:       name,
		Ino:        newIno,
		WriteEpoch: bc.df.currentWriteEpoch,
	}).On("CONFLICT (parent_ino, name, write_epoch) DO UPDATE").
		Set("ino = EXCLUDED.ino").
		Exec(ctx); err != nil {
		return err
	}

	// Write content in chunks using Bun
	if len(item.Content) > 0 {
		if err := bc.writeContentChunked(ctx, tx, newIno, item.Content); err != nil {
			return err
		}
	}

	return nil
}

// writeContentChunked writes file content in chunks using Bun
func (bc *BulkCopier) writeContentChunked(ctx context.Context, tx bun.Tx, ino int64, data []byte) error {
	pos := int64(0)
	chunkIdx := 0
	for pos < int64(len(data)) {
		end := min(pos+ChunkSize, int64(len(data)))
		chunk := data[pos:end]

		if _, err := tx.NewInsert().Model(&ContentModel{
			Ino:        ino,
			ChunkIdx:   int64(chunkIdx),
			WriteEpoch: bc.df.currentWriteEpoch,
			Data:       chunk,
		}).On("CONFLICT (ino, chunk_idx, write_epoch) DO UPDATE").
			Set("data = EXCLUDED.data").
			Exec(ctx); err != nil {
			return err
		}

		pos = end
		chunkIdx++
	}
	return nil
}

