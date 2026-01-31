# latentfs fork

Fork a folder with copy-on-write semantics.

## Synopsis

```
latentfs fork <source> [flags]
latentfs fork --source-empty [flags]
```

## Description

Creates a copy-on-write overlay on top of an existing folder. The fork command:
- Creates a data file (`-d/--data-file` or derived from `--mount`)
- Optionally mounts it and creates a symlink from target to the mount
- Reads fall through to the source folder (overlay mode)
- Writes are captured in the overlay

## Flags

| Flag | Short | Description |
|------|-------|-------------|
| `--mount` | `-m` | Target path for the mount symlink |
| `--data-file` | `-d` | Custom path for the data file |
| `--source-empty` | | Create an empty fork (no source folder) |
| `--type` | `-t` | Fork type: `overlay` (default) or `full` |
| `--allow-partial` | | Continue full fork even if some files cannot be read |
| `--config-dir` | | Config directory relative to source (default: `.latentfs`) |

## Fork Types

### Overlay (default)

- Creates an overlay on top of the source folder
- Reads fall through to source if file not modified
- Writes are captured in the data file
- Changes in source are visible (unless file was modified in fork)

### Full

- Creates a complete snapshot of the source folder
- All files are copied into the data file
- Independent of source folder after creation
- Source folder can be deleted without affecting the fork

## Examples

```bash
# Fork and mount an existing folder (overlay mode)
latentfs fork ~/projects/myapp -m ~/experiments/myapp-test

# Fork and mount with custom data file location
latentfs fork ~/projects/myapp -m ~/experiments/test -d ~/data/test.latentfs

# Fork without mounting (create data file only)
latentfs fork ~/projects/myapp -d ~/data/myapp.latentfs

# Create an empty data file without mounting
latentfs fork --source-empty -d ~/data/empty.latentfs

# Create an empty fork and mount it
latentfs fork --source-empty -m ~/workspace/newproject

# Create a full fork (copies all files)
latentfs fork ~/projects/myapp --type=full -m ~/backup/myapp-backup

# Full fork with partial success (skip unreadable files)
latentfs fork ~/projects/myapp --type=full --allow-partial -m ~/backup
```

## Configuration

The fork command looks for configuration in `<source>/.latentfs/config.yaml` (or the directory specified by `--config-dir`). This config controls:

- `gitignore: true/false` - Whether to respect .gitignore patterns
- `includes: []` - Patterns to include
- `excludes: []` - Patterns to exclude

## Restrictions

- Must specify either `--mount` or `--data-file`
- Data file cannot be inside a latentfs mount
- Target cannot be inside an existing mount
- `--type=full` cannot be used with `--source-empty`
- Overlay fork source cannot be inside a mount (use `--type=full` instead)

## See Also

- [mount](mount.md) - Mount existing data files
- [data](data.md) - Manage snapshots
- [info](info.md) - Get fork information
