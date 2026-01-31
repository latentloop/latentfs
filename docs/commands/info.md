# latentfs info

Show mount information for a path.

## Synopsis

```
latentfs info [path]
```

## Description

Check if a path is inside a latentfs mount and show the corresponding data file.

This is useful for determining which data file backs a particular directory or file. If no path is specified, uses the current directory.

## Arguments

- `path` - Path to check (optional, defaults to current directory)

## Examples

```bash
# Check current directory
latentfs info

# Check specific path
latentfs info .

# Check a fork directory
latentfs info ~/projects/my-fork

# Check a file inside a fork
latentfs info /path/to/file.txt
```

## Output

**When path is inside a mount:**
```
Path: /Users/name/experiments/test
Mount: yes
Data file: /Users/name/data/test.latentfs
Source folder: /Users/name/projects/myapp
Snapshots: 3
```

**When path is not inside a mount:**
```
Path: /Users/name/regular-folder
Mount: not in a latentfs mount
```

## Use Cases

- Determine which data file to use for snapshot commands
- Verify a path is correctly mounted
- Find the source folder for a soft-fork
- Check snapshot count without listing all snapshots

## See Also

- [mount](mount.md) - Mount operations
- [data](data.md) - Snapshot management
