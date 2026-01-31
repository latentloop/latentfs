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
	"time"

	"github.com/uptrace/bun"
)

// Bun ORM models for latentfs database tables.
// These models mirror the existing database schema and can be used
// alongside the existing raw SQL code during incremental migration.

// SchemaInfoModel represents the schema_info table
type SchemaInfoModel struct {
	bun.BaseModel `bun:"table:schema_info"`

	Key   string `bun:"key,pk"`
	Value string `bun:"value,notnull"`
}

// WriteEpochModel represents the write_epochs table for MVCC versioning
type WriteEpochModel struct {
	bun.BaseModel `bun:"table:write_epochs"`

	Epoch     int64  `bun:"epoch,pk,autoincrement"`
	Status    string `bun:"status,notnull"`    // "draft", "ready", "deprecated"
	CreatedBy string `bun:"created_by,notnull"`
	CreatedAt int64  `bun:"created_at,notnull"` // Unix timestamp
	UpdatedAt int64  `bun:"updated_at,notnull"` // Unix timestamp
}

// ConfigModel represents the config table
type ConfigModel struct {
	bun.BaseModel `bun:"table:config"`

	Key   string `bun:"key,pk"`
	Value string `bun:"value,notnull"`
}

// InodeModel represents the inodes table with MVCC versioning.
// Note: Times are stored as Unix timestamps in the database.
type InodeModel struct {
	bun.BaseModel `bun:"table:inodes"`

	Ino        int64 `bun:"ino,pk"`
	WriteEpoch int64 `bun:"write_epoch,pk"`
	Mode       int64 `bun:"mode,notnull"`
	UID        int64 `bun:"uid,notnull"`
	GID        int64 `bun:"gid,notnull"`
	Size       int64 `bun:"size,notnull"`
	Atime      int64 `bun:"atime,notnull"` // Unix timestamp
	Mtime      int64 `bun:"mtime,notnull"` // Unix timestamp
	Ctime      int64 `bun:"ctime,notnull"` // Unix timestamp
	Nlink      int64 `bun:"nlink,notnull"`
}

// ToInode converts an InodeModel to the existing Inode struct
func (m *InodeModel) ToInode() *Inode {
	return &Inode{
		Ino:   m.Ino,
		Mode:  uint32(m.Mode),
		Uid:   uint32(m.UID),
		Gid:   uint32(m.GID),
		Size:  m.Size,
		Atime: time.Unix(m.Atime, 0),
		Mtime: time.Unix(m.Mtime, 0),
		Ctime: time.Unix(m.Ctime, 0),
		Nlink: int32(m.Nlink),
	}
}

// InodeModelFromInode converts an Inode to InodeModel
func InodeModelFromInode(inode *Inode, writeEpoch int64) *InodeModel {
	return &InodeModel{
		Ino:        inode.Ino,
		WriteEpoch: writeEpoch,
		Mode:       int64(inode.Mode),
		UID:        int64(inode.Uid),
		GID:        int64(inode.Gid),
		Size:       inode.Size,
		Atime:      inode.Atime.Unix(),
		Mtime:      inode.Mtime.Unix(),
		Ctime:      inode.Ctime.Unix(),
		Nlink:      int64(inode.Nlink),
	}
}

// DentryModel represents the dentries table with MVCC versioning
type DentryModel struct {
	bun.BaseModel `bun:"table:dentries"`

	ParentIno  int64  `bun:"parent_ino,pk"`
	Name       string `bun:"name,pk"`
	WriteEpoch int64  `bun:"write_epoch,pk"`
	Ino        int64  `bun:"ino,notnull"` // 0 = tombstone (deleted)
}

// ToDentry converts a DentryModel to the existing Dentry struct
func (m *DentryModel) ToDentry() *Dentry {
	return &Dentry{
		ParentIno: m.ParentIno,
		Name:      m.Name,
		Ino:       m.Ino,
	}
}

// DentryModelFromDentry converts a Dentry to DentryModel
func DentryModelFromDentry(d *Dentry, writeEpoch int64) *DentryModel {
	return &DentryModel{
		ParentIno:  d.ParentIno,
		Name:       d.Name,
		WriteEpoch: writeEpoch,
		Ino:        d.Ino,
	}
}

// ContentModel represents the content table (file chunks) with MVCC versioning
type ContentModel struct {
	bun.BaseModel `bun:"table:content"`

	Ino        int64  `bun:"ino,pk"`
	ChunkIdx   int64  `bun:"chunk_idx,pk"`
	WriteEpoch int64  `bun:"write_epoch,pk"`
	Data       []byte `bun:"data,notnull"`
}

// SymlinkModel represents the symlinks table with MVCC versioning
type SymlinkModel struct {
	bun.BaseModel `bun:"table:symlinks"`

	Ino        int64  `bun:"ino,pk"`
	WriteEpoch int64  `bun:"write_epoch,pk"`
	Target     string `bun:"target,notnull"`
}

// SnapshotModel represents the snapshots metadata table
type SnapshotModel struct {
	bun.BaseModel `bun:"table:snapshots"`

	ID          string `bun:"id,pk"`
	Message     string `bun:"message"`
	Tag         string `bun:"tag,unique,nullzero"` // nullzero: empty string -> NULL (allows multiple untagged snapshots)
	CreatedAt   int64  `bun:"created_at,notnull"`  // Unix timestamp
	FileCount   int64  `bun:"file_count,notnull"`  // Number of files in snapshot
	TotalSize   int64  `bun:"total_size,notnull"`  // Total size of all files in bytes
	SourceEpoch int64  `bun:"source_epoch"`        // Epoch at which snapshot was taken (for overlay restore)
}

// SnapshotInodeModel represents inode state at snapshot time
type SnapshotInodeModel struct {
	bun.BaseModel `bun:"table:snapshot_inodes"`

	SnapshotID string `bun:"snapshot_id,pk"`
	Ino        int64  `bun:"ino,pk"`
	Mode       int64  `bun:"mode,notnull"`
	UID        int64  `bun:"uid,notnull"`
	GID        int64  `bun:"gid,notnull"`
	Size       int64  `bun:"size,notnull"`
	Atime      int64  `bun:"atime,notnull"`
	Mtime      int64  `bun:"mtime,notnull"`
	Ctime      int64  `bun:"ctime,notnull"`
	Nlink      int64  `bun:"nlink,notnull"`
}

// SnapshotDentryModel represents directory structure at snapshot time
type SnapshotDentryModel struct {
	bun.BaseModel `bun:"table:snapshot_dentries"`

	SnapshotID string `bun:"snapshot_id,pk"`
	ParentIno  int64  `bun:"parent_ino,pk"`
	Name       string `bun:"name,pk"`
	Ino        int64  `bun:"ino,notnull"`
}

// ContentBlockModel represents deduplicated content storage (content-addressable)
type ContentBlockModel struct {
	bun.BaseModel `bun:"table:content_blocks"`

	Hash string `bun:"hash,pk"` // SHA-256 hash
	Data []byte `bun:"data,notnull"`
}

// SnapshotContentModel references deduplicated blocks for a snapshot
type SnapshotContentModel struct {
	bun.BaseModel `bun:"table:snapshot_content"`

	SnapshotID string `bun:"snapshot_id,pk"`
	Ino        int64  `bun:"ino,pk"`
	ChunkIdx   int64  `bun:"chunk_idx,pk"`
	Hash       string `bun:"hash,notnull"` // References content_blocks.hash
}

// SnapshotSymlinkModel represents symlink targets at snapshot time
type SnapshotSymlinkModel struct {
	bun.BaseModel `bun:"table:snapshot_symlinks"`

	SnapshotID string `bun:"snapshot_id,pk"`
	Ino        int64  `bun:"ino,pk"`
	Target     string `bun:"target,notnull"`
}

// MountEntryModel represents a mount mapping in the meta file
type MountEntryModel struct {
	bun.BaseModel `bun:"table:mounts"`

	ID            int64  `bun:"id,pk,autoincrement"`
	SubPath       string `bun:"sub_path,notnull,unique"`
	DataFile      string `bun:"data_file,notnull"`
	SourceFolder  string `bun:"source_folder"`
	ForkType      string `bun:"fork_type"`
	SymlinkTarget string `bun:"symlink_target"`
	CreatedAt     int64  `bun:"created_at,notnull"` // Unix timestamp
}

// ToMountEntry converts a MountEntryModel to the existing MountEntry struct
func (m *MountEntryModel) ToMountEntry() *MountEntry {
	return &MountEntry{
		ID:            m.ID,
		SubPath:       m.SubPath,
		DataFile:      m.DataFile,
		SourceFolder:  m.SourceFolder,
		ForkType:      m.ForkType,
		SymlinkTarget: m.SymlinkTarget,
		CreatedAt:     time.Unix(m.CreatedAt, 0),
	}
}

// MountEntryModelFromMountEntry converts a MountEntry to MountEntryModel
func MountEntryModelFromMountEntry(entry *MountEntry) *MountEntryModel {
	return &MountEntryModel{
		ID:            entry.ID,
		SubPath:       entry.SubPath,
		DataFile:      entry.DataFile,
		SourceFolder:  entry.SourceFolder,
		ForkType:      entry.ForkType,
		SymlinkTarget: entry.SymlinkTarget,
		CreatedAt:     entry.CreatedAt.Unix(),
	}
}

// ProtectedFileModel represents a protected data file in the meta file
type ProtectedFileModel struct {
	bun.BaseModel `bun:"table:protected_files"`

	ID        int64  `bun:"id,pk,autoincrement"`
	DataFile  string `bun:"data_file,notnull,unique"`
	CreatedAt int64  `bun:"created_at,notnull"` // Unix timestamp
}

// ProtectedFileEntry represents a protected file entry
type ProtectedFileEntry struct {
	ID        int64
	DataFile  string
	CreatedAt time.Time
}

// ToProtectedFileEntry converts a ProtectedFileModel to ProtectedFileEntry
func (m *ProtectedFileModel) ToProtectedFileEntry() *ProtectedFileEntry {
	return &ProtectedFileEntry{
		ID:        m.ID,
		DataFile:  m.DataFile,
		CreatedAt: time.Unix(m.CreatedAt, 0),
	}
}
