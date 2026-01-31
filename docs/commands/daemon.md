# latentfs daemon

Daemon management commands.

## Synopsis

```
latentfs daemon start [flags]
latentfs daemon stop
latentfs daemon status
latentfs daemon config [flags]
```

## Description

Commands for controlling the latentfs daemon. The daemon manages NFS mounts and provides IPC for CLI commands.

## Subcommands

### start

Start the daemon.

```
latentfs daemon start [flags]
```

**Flags:**
- `-f, --foreground` - Run in foreground (don't daemonize)
- `--restart` - Restart daemon if already running (no confirmation)
- `--skip-cleanup` - Skip startup cleanup (stale mounts, zombie daemons)

### stop

Stop the running daemon.

```
latentfs daemon stop
```

Gracefully stops the daemon and cleans up mounts.

### status

Show daemon status including auto-start settings.

```
latentfs daemon status
```

**Output:**
```
Daemon: running (PID 12345)
Auto-start on login: enabled (active)
Log level: none
```

### config

Configure persistent daemon settings.

```
latentfs daemon config [flags]
```

**Flags:**
- `--logging <level>` - Log level: trace, debug, info, warn, none
- `--login-start <on|off>` - Auto-start on login (macOS only)

Settings are stored in `~/.latentfs/settings.yaml` and take effect on next daemon start.

## Examples

```bash
# Start the daemon in background
latentfs daemon start

# Start in foreground (for debugging)
latentfs daemon start -f

# Restart the daemon
latentfs daemon start --restart

# Stop the daemon
latentfs daemon stop

# Check daemon status
latentfs daemon status

# Show current configuration
latentfs daemon config

# Enable trace logging
latentfs daemon config --logging trace

# Disable logging
latentfs daemon config --logging none

# Enable auto-start on login
latentfs daemon config --login-start on

# Disable auto-start on login
latentfs daemon config --login-start off

# Configure multiple settings at once
latentfs daemon config --logging debug --login-start on
```

## Auto-Start (macOS)

On macOS, the `config --login-start` option manages a LaunchAgent that starts the daemon when you log in.

- LaunchAgent path: `~/Library/LaunchAgents/com.latentfs.daemon.plist`
- The daemon is started with default settings from `~/.latentfs/settings.yaml`

## Logging

Log levels (from most to least verbose):
- `trace` - Detailed debugging information
- `debug` - Debug messages
- `info` - Informational messages
- `warn` - Warnings only
- `none` - No logging (default)

Logs are written to `~/.latentfs/daemon.log` when enabled.

## See Also

- [mount](mount.md) - Mount operations
- [unmount](unmount.md) - Unmount operations
