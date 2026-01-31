LatentFS is an intermediate layer between AI agents and the file system that supports data fork/save/restore through the OS VFS interface.

## Overview

- **Copy-on-Write Forks** - Create overlay forks of directories where writes are captured without modifying the source
- **Snapshots** - Save and restore filesystem states at any point
- **Native Mounting** - Mounts overlay to folder via NFS
- **Background Daemon** - Manages multiple concurrent mounts with auto-start/stop

## Commands

```bash
# Initialize a LatentFS project (creates .latentfs/config.yaml)
latentfs init ~/projects/myapp

# Fork a directory and mount (copy-on-write overlay)
latentfs fork ~/projects/myapp -m ~/experiments/test

# Fork with full copy
latentfs fork ~/projects/myapp --type=full -m ~/backup/myapp-backup

# Mount a data file
latentfs mount ./my-mount -d ./data.latentfs

# Save a snapshot
latentfs data save -m "Initial version" -t v1.0

# List snapshots
latentfs data ls-saves

# Restore a snapshot
latentfs data restore v1.0

# Unmount
latentfs unmount --all
```

## Development

```bash
make build
make install-dev
```