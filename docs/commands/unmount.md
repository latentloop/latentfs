# latentfs unmount

Unmount a latentfs folder.

## Synopsis

```
latentfs unmount <mount-point>
latentfs unmount --all
latentfs umount <mount-point>
```

## Description

Unmounts a mounted latentfs folder. The daemon continues running after unmounting.

## Aliases

- `umount`

## Flags

- `-a, --all` - Unmount all folders (daemon continues running)

## Examples

```bash
# Unmount a specific folder
latentfs unmount ~/experiments/test

# Unmount all folders
latentfs unmount --all

# Using the short alias
latentfs umount ~/experiments/test
```

## Behavior

- Requires the daemon to be running
- Removes the symlink from the target path
- Releases the mount from the central mount point
- Does not delete the data file

## Error Handling

Returns an error if:
- Daemon is not running
- Mount point is not found
- No mount point specified (without `--all`)

## See Also

- [mount](mount.md) - Mount data files
- [daemon](daemon.md) - Daemon management
