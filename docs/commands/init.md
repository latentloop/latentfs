# latentfs init

Initialize a LatentFS project in a directory.

## Synopsis

```
latentfs init [directory]
```

## Description

Creates a `.latentfs` directory with default configuration files, similar to `git init`. If no directory is specified, initializes the current directory.

## Arguments

- `directory` - Optional. The directory to initialize. Defaults to current directory.

## Behavior

- Creates `.latentfs/` directory if it doesn't exist
- Creates `.latentfs/config.yaml` with default configuration
- If `.latentfs/` already exists, reports "Reinitialized"
- If `config.yaml` already exists, it is not overwritten

## Default Configuration

The default `config.yaml` contains:

```yaml
logging: none
gitignore: true
includes:
  - ".git"
excludes: []
```

## Examples

```bash
# Initialize current directory
latentfs init

# Initialize a specific directory
latentfs init /path/to/project

# Initialize and enable autosave (edit config after)
latentfs init
vi .latentfs/config.yaml
```

## Output

```
Initialized empty LatentFS project in /path/to/project/.latentfs
  created config.yaml
```

Or if already initialized:

```
Reinitialized existing LatentFS project in /path/to/project/.latentfs
  config.yaml already exists (not modified)
```

## See Also

- [data](data.md) - Snapshot management
- [mount](mount.md) - Mount a data file
