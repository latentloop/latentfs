package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/tursodatabase/go-libsql"
)

// MountEntry represents a mount mapping in the meta file
type MountEntry struct {
	ID            int64
	SubPath       string
	DataFile      string
	SourceFolder  string // Source folder (origin of fork, empty if not a fork)
	ForkType      string // Fork type: "soft", "hard", or empty (use ForkTypeSoft, ForkTypeHard constants)
	SymlinkTarget string // Where the symlink was created (the user-facing path)
	CreatedAt     time.Time
}

// MetaFile represents a SQLite-backed latentfs metadata file
type MetaFile struct {
	path  string
	db    *sql.DB
	bunDB *BunDB
}

// CreateMeta creates a new .latentfs metadata file with default context
func CreateMeta(path string) (*MetaFile, error) {
	return CreateMetaWithContext(path, DBContextDefault)
}

// CreateMetaWithContext creates a new .latentfs metadata file with the specified context.
func CreateMetaWithContext(path string, ctx DBContext) (*MetaFile, error) {
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
	if err := execStatements(db, metaFileSchema); err != nil {
		db.Close()
		os.Remove(path)
		return nil, fmt.Errorf("failed to create schema: %w", err)
	}

	// Initialize meta file
	if err := execStatements(db, initMetaFile, SchemaVersion); err != nil {
		db.Close()
		os.Remove(path)
		return nil, fmt.Errorf("failed to initialize meta file: %w", err)
	}

	return &MetaFile{
		path:  path,
		db:    db,
		bunDB: NewBunDB(db),
	}, nil
}

// OpenMeta opens an existing .latentfs metadata file with default context
func OpenMeta(path string) (*MetaFile, error) {
	return OpenMetaWithContext(path, DBContextDefault)
}

// OpenMetaWithContext opens an existing .latentfs metadata file with the specified context.
func OpenMetaWithContext(path string, ctx DBContext) (*MetaFile, error) {
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

	// Verify it's a meta file
	fileType, err := bunDB.GetSchemaInfo(bgCtx, "type")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to read schema info: %w", err)
	}
	if fileType != "meta" {
		db.Close()
		return nil, fmt.Errorf("not a meta file (type=%s)", fileType)
	}

	return &MetaFile{
		path:  path,
		db:    db,
		bunDB: bunDB,
	}, nil
}

// OpenOrCreateMeta opens an existing meta file or creates a new one with default context
func OpenOrCreateMeta(path string) (*MetaFile, error) {
	return OpenOrCreateMetaWithContext(path, DBContextDefault)
}

// OpenOrCreateMetaWithContext opens an existing meta file or creates a new one with the specified context
func OpenOrCreateMetaWithContext(path string, ctx DBContext) (*MetaFile, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return CreateMetaWithContext(path, ctx)
	}
	return OpenMetaWithContext(path, ctx)
}

// Close closes the database connection
func (mf *MetaFile) Close() error {
	if mf.db != nil {
		return mf.db.Close()
	}
	return nil
}

// Path returns the file path
func (mf *MetaFile) Path() string {
	return mf.path
}

// DB returns the underlying *sql.DB for use with Bun or other wrappers.
func (mf *MetaFile) DB() *sql.DB {
	return mf.db
}

// BunDB returns the Bun database wrapper.
func (mf *MetaFile) BunDB() *BunDB {
	return mf.bunDB
}

// AddMount adds a mount mapping
func (mf *MetaFile) AddMount(subPath, dataFile string) error {
	ctx := context.Background()
	return mf.bunDB.UpsertMountEntry(ctx, &MountEntryModel{
		SubPath:   subPath,
		DataFile:  dataFile,
		CreatedAt: time.Now().Unix(),
	})
}

// AddForkMount adds a fork mount mapping with source folder, fork type, and symlink target
func (mf *MetaFile) AddForkMount(subPath, dataFile, sourceFolder, forkType, symlinkTarget string) error {
	ctx := context.Background()
	return mf.bunDB.UpsertMountEntry(ctx, &MountEntryModel{
		SubPath:       subPath,
		DataFile:      dataFile,
		SourceFolder:  sourceFolder,
		ForkType:      forkType,
		SymlinkTarget: symlinkTarget,
		CreatedAt:     time.Now().Unix(),
	})
}

// RemoveMount removes a mount mapping by sub_path
func (mf *MetaFile) RemoveMount(subPath string) error {
	ctx := context.Background()
	rows, err := mf.bunDB.DeleteMountEntry(ctx, subPath)
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("mount not found: %s", subPath)
	}
	return nil
}

// ListMounts returns all mount mappings
func (mf *MetaFile) ListMounts() ([]MountEntry, error) {
	ctx := context.Background()
	models, err := mf.bunDB.ListMountEntries(ctx)
	if err != nil {
		return nil, err
	}

	entries := make([]MountEntry, len(models))
	for i, m := range models {
		entries[i] = *m.ToMountEntry()
	}
	return entries, nil
}

// LookupMount finds the mount entry for a given path
// Returns the mount entry and the remaining path within that mount
func (mf *MetaFile) LookupMount(path string) (*MountEntry, string, error) {
	ctx := context.Background()
	model, err := mf.bunDB.LookupMountByPath(ctx, path)
	if err != nil {
		return nil, "", err
	}
	if model == nil {
		return nil, "", nil // No mount found
	}

	entry := model.ToMountEntry()

	// Calculate remaining path
	remainingPath := ""
	if len(path) > len(entry.SubPath) {
		remainingPath = path[len(entry.SubPath):]
		if len(remainingPath) > 0 && remainingPath[0] == '/' {
			remainingPath = remainingPath[1:]
		}
	}

	return entry, remainingPath, nil
}

// GetMountBySymlinkTarget finds a mount entry by symlink target path
// Returns nil if no mount is found for the given symlink target
func (mf *MetaFile) GetMountBySymlinkTarget(symlinkTarget string) (*MountEntry, error) {
	ctx := context.Background()
	model, err := mf.bunDB.GetMountBySymlinkTarget(ctx, symlinkTarget)
	if err != nil {
		return nil, err
	}
	if model == nil {
		return nil, nil
	}
	return model.ToMountEntry(), nil
}

// GetMountByDataFile finds a mount entry by data file path
// Returns nil if no mount is found for the given data file
func (mf *MetaFile) GetMountByDataFile(dataFile string) (*MountEntry, error) {
	ctx := context.Background()
	model, err := mf.bunDB.GetMountByDataFile(ctx, dataFile)
	if err != nil {
		return nil, err
	}
	if model == nil {
		return nil, nil
	}
	return model.ToMountEntry(), nil
}

// GetConfig gets a config value
func (mf *MetaFile) GetConfig(key string) (string, error) {
	ctx := context.Background()
	return mf.bunDB.GetConfigValue(ctx, key)
}

// SetConfig sets a config value
func (mf *MetaFile) SetConfig(key, value string) error {
	ctx := context.Background()
	return mf.bunDB.SetConfigValue(ctx, key, value)
}

// AddProtectedFile adds a data file to the protected list
func (mf *MetaFile) AddProtectedFile(dataFile string) error {
	ctx := context.Background()
	return mf.bunDB.AddProtectedFile(ctx, dataFile)
}

// RemoveProtectedFile removes a data file from the protected list
func (mf *MetaFile) RemoveProtectedFile(dataFile string) error {
	ctx := context.Background()
	rows, err := mf.bunDB.RemoveProtectedFile(ctx, dataFile)
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("protected file not found: %s", dataFile)
	}
	return nil
}

// ListProtectedFiles returns all protected data files
func (mf *MetaFile) ListProtectedFiles() ([]ProtectedFileEntry, error) {
	ctx := context.Background()
	models, err := mf.bunDB.ListProtectedFiles(ctx)
	if err != nil {
		return nil, err
	}

	entries := make([]ProtectedFileEntry, len(models))
	for i, m := range models {
		entries[i] = *m.ToProtectedFileEntry()
	}
	return entries, nil
}

// IsProtected checks if a data file is protected
func (mf *MetaFile) IsProtected(dataFile string) (bool, error) {
	ctx := context.Background()
	model, err := mf.bunDB.GetProtectedFile(ctx, dataFile)
	if err != nil {
		return false, err
	}
	return model != nil, nil
}
