# latentfs

Intermediate layer between AI agent and the file system.

## Synopsis

```
latentfs [command]
```

## Description

LatentFS is an intermediate layer between AI agents and the file system that supports data fork/save/restore through the OS VFS interface.

The daemon is automatically started when running commands that require it.

## Commands

| Command | Description |
|---------|-------------|
| [mount](mount.md) | Mount a .latentfs data file |
| [unmount](unmount.md) | Unmount a folder |
| [fork](fork.md) | Fork a folder with copy-on-write semantics |
| [daemon](daemon.md) | Daemon management commands |
| [info](info.md) | Show mount information for a path |
| [data](data.md) | Manage data snapshots |

## Global Flags

```
-h, --help      Help for latentfs
-v, --version   Version for latentfs
```

## Examples

```bash
# Fork a project folder
latentfs fork ~/projects/myapp -m ~/experiments/test

# Create a snapshot
latentfs data save -m "checkpoint"

# List active mounts
latentfs mount ls

# Check daemon status
latentfs daemon status
```

## See Also

- [mount](mount.md) - Mount operations
- [fork](fork.md) - Create copy-on-write overlays
- [data](data.md) - Snapshot management
