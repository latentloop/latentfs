# latentfs data

Manage data snapshots.

## Synopsis

```
latentfs data save [flags]
latentfs data restore <snapshot> [flags] [-- <path>...]
latentfs data ls-saves [flags]
latentfs data remove <snapshot> [flags]
latentfs data gc [flags]
```

## Description

Manage snapshots for a latentfs data file. Snapshots capture the filesystem state at a point in time and can be restored later.

## Subcommands

### save

Create a snapshot of the current filesystem state.

```
latentfs data save [flags]
```

**Flags:**
- `-m, --message <text>` - Snapshot message
- `-t, --tag <name>` - Snapshot tag (must be unique)
- `-d, --data-file <path>` - Path to .latentfs data file
- `--allow-partial` - Continue even if some files cannot be read
- `--full` - Create full snapshot including source folder (for overlay forks)

### restore

Restore files from a snapshot.

```
latentfs data restore <snapshot> [flags] [-- <path>...]
```

**Arguments:**
- `snapshot` - Snapshot ID (full or prefix) or tag name

**Flags:**
- `-y, --yes` - Skip confirmation prompt
- `-d, --data-file <path>` - Path to .latentfs data file

**Important:** All flags must come BEFORE the `--` separator. Anything after `--` is interpreted as file paths.

### ls-saves

List all snapshots in a data file.

```
latentfs data ls-saves [flags]
```

**Flags:**
- `-d, --data-file <path>` - Path to .latentfs data file

### remove

Remove a snapshot permanently.

```
latentfs data remove <snapshot> [flags]
```

**Flags:**
- `-y, --yes` - Skip confirmation prompt
- `-d, --data-file <path>` - Path to .latentfs data file

### gc

Run garbage collection to clean orphaned data.

```
latentfs data gc [flags]
```

**Flags:**
- `--stats` - Show stats without cleaning
- `--vacuum` - Also run SQLite VACUUM
- `-d, --data-file <path>` - Path to .latentfs data file

## Path Resolution

The data file can be specified in several ways:
1. Explicitly with `-d/--data-file`
2. Auto-detected from current working directory (if inside a fork mount)

## Examples

```bash
# Create a snapshot with message
latentfs data save -m "Initial version"

# Create a snapshot with tag
latentfs data save -m "Release" -t v1.0

# Save from specific data file
latentfs data save -d ~/data/project.latentfs -m "Backup"

# Create full snapshot (for overlay forks, includes source folder)
latentfs data save -m "Full backup" --full

# List snapshots
latentfs data ls-saves

# List snapshots from specific data file
latentfs data ls-saves -d ~/data/project.latentfs

# Restore all files from tagged snapshot
latentfs data restore v1

# Restore without confirmation
latentfs data restore v1 -y

# Restore specific files
latentfs data restore abc123 -- src/main.go

# Restore multiple files with flags
latentfs data restore v1 -d ~/data/project.latentfs -y -- file1.txt file2.txt

# Remove a snapshot
latentfs data remove v1.0

# Remove without confirmation
latentfs data remove v1.0 -y

# Show storage stats
latentfs data gc --stats

# Run garbage collection
latentfs data gc

# Run GC with VACUUM
latentfs data gc --vacuum
```

## Output Formats

### ls-saves Output

```
snapshot abc12345 (tag: v1.0)
Date:   Mon Jan 15 10:30:45 2024

    Release version 1.0

snapshot def67890
Date:   Sun Jan 14 15:20:30 2024

    Initial commit
```

### gc --stats Output

```
Storage: 2.3 MB total (5 snapshots, 142 content blocks)
Reclaimable: 512.0 KB (12 orphaned blocks, 2 deprecated epochs)
```

## Snapshot Identification

Snapshots can be referenced by:
- Full ID (e.g., `abc123def456...`)
- ID prefix (e.g., `abc123`)
- Tag name (e.g., `v1.0`)

## See Also

- [fork](fork.md) - Create forks
- [mount](mount.md) - Mount data files
- [info](info.md) - Get mount information
