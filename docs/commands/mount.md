# latentfs mount

Mount a .latentfs data file.

## Synopsis

```
latentfs mount <mount-point> -d <path> [flags]
latentfs mount ls
latentfs mount check <path>... [flags]
```

## Description

Mounts a .latentfs data file at the specified mount point. The daemon will be started automatically if not running.

The mount uses a central mount point with a UUID subfolder and creates a symlink from the target path to the actual mount location.

## Subcommands

### mount

Mount a .latentfs data file.

```
latentfs mount <mount-point> -d <path>
```

**Flags:**
- `-d, --data-file <path>` - Path to the .latentfs data file (required)

### ls

List all currently active latentfs mounts.

```
latentfs mount ls
```

### check

Check if one or more paths are currently mounted.

```
latentfs mount check <path>... [flags]
```

**Flags:**
- `-q, --quiet` - Suppress output, only set exit code

Returns exit code 0 if ALL paths are mounted, non-zero otherwise.

## Examples

```bash
# Mount a data file
latentfs mount ./my-mount -d ./data.latentfs

# Mount with absolute paths
latentfs mount /Volumes/mydata --data-file ~/backups/mydata.latentfs

# List active mounts
latentfs mount ls

# Check if a path is mounted (useful in scripts)
latentfs mount check ~/projects/fork1 && echo "mounted"

# Check multiple paths quietly
latentfs mount check -q ~/fork1 ~/fork2
```

## Output Format

The `ls` subcommand shows mount information:

```
Active mounts (2):
  /path/to/data.latentfs -> /path/to/target [soft-fork]
    source: /path/to/source (copy-on-write)
  /path/to/other.latentfs -> /path/to/target2 [hard-fork]
    origin: /path/to/origin (independent copy)
```

## Restrictions

- Data file cannot be inside a latentfs mount (causes NFS deadlock)
- Mount target cannot be inside an existing mount
- Target must be empty or non-existent

## See Also

- [unmount](unmount.md) - Unmount folders
- [fork](fork.md) - Create new forks
- [info](info.md) - Get mount information
