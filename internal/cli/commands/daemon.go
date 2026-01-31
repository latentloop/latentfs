package commands

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"latentfs/internal/daemon"
	"latentfs/internal/util"
)

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Daemon management commands",
	Long:  `Commands for controlling the latentfs daemon.`,
}

var daemonStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the daemon",
	Long:  `Starts the latentfs daemon in the background.`,
	Args:  cobra.NoArgs,
	RunE:  runDaemonStart,
}

var daemonStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the daemon",
	Long:  `Stops the running latentfs daemon.`,
	Args:  cobra.NoArgs,
	RunE:  runDaemonStop,
}

var daemonStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show daemon status",
	Long:  `Shows the current status of the latentfs daemon including auto-start settings.`,
	Args:  cobra.NoArgs,
	RunE:  runDaemonStatus,
}

var daemonConfigCmd = &cobra.Command{
	Use:   "config",
	Short: "Configure daemon settings",
	Long: `Configure persistent daemon settings.

Settings are stored in ~/.latentfs/settings.yaml and take effect on next daemon start.

Examples:
  # Enable trace logging
  latentfs daemon config --logging trace

  # Disable logging
  latentfs daemon config --logging none

  # Enable auto-start on login
  latentfs daemon config --login-start on

  # Disable auto-start on login
  latentfs daemon config --login-start off

  # Show current configuration
  latentfs daemon config`,
	Args: cobra.NoArgs,
	RunE: runDaemonConfig,
}

var daemonForeground bool
var daemonLogLevel string
var daemonRestart bool
var daemonSkipCleanup bool
var configLogLevel string
var configLoginStart string

func init() {
	daemonStartCmd.Flags().BoolVarP(&daemonForeground, "foreground", "f", false, "Run in foreground")
	daemonStartCmd.Flags().StringVar(&daemonLogLevel, "logging", "", "Log level (deprecated: use 'daemon config --logging' instead)")
	daemonStartCmd.Flags().MarkHidden("logging") // Hide deprecated flag
	daemonStartCmd.Flags().BoolVar(&daemonRestart, "restart", false, "Restart daemon if already running (no confirmation)")
	daemonStartCmd.Flags().BoolVar(&daemonSkipCleanup, "skip-cleanup", false, "Skip startup cleanup (stale mounts, zombie daemons)")
	daemonConfigCmd.Flags().StringVar(&configLogLevel, "logging", "", "Log level: trace, debug, info, warn, none")
	daemonConfigCmd.Flags().StringVar(&configLoginStart, "login-start", "", "Auto-start on login: on, off")
	daemonCmd.AddCommand(daemonStartCmd)
	daemonCmd.AddCommand(daemonStopCmd)
	daemonCmd.AddCommand(daemonStatusCmd)
	daemonCmd.AddCommand(daemonConfigCmd)
	rootCmd.AddCommand(daemonCmd)
}

func runDaemonStart(cmd *cobra.Command, args []string) error {
	// Check if already running
	if daemon.IsDaemonRunning() {
		pid, _ := daemon.GetPID()

		if daemonRestart {
			// --restart flag: stop and restart without prompting
			fmt.Printf("Daemon already running (PID %d), restarting...\n", pid)
			if err := stopDaemonAndWait(); err != nil {
				return fmt.Errorf("failed to stop daemon for restart: %w", err)
			}
		} else {
			// No --restart flag: just report and exit
			fmt.Printf("Daemon already running (PID %d)\n", pid)
			fmt.Println("Use --restart to restart the daemon")
			return nil
		}
	}

	// Load log level from settings (or use deprecated --logging flag for backwards compatibility)
	logLevel := daemonLogLevel
	if logLevel == "" {
		settings, err := daemon.LoadGlobalSettings()
		if err == nil {
			logLevel = settings.LogLevel
		}
	}

	if daemonForeground {
		// Run in foreground
		d := daemon.New()
		d.LogLevel = logLevel
		d.SkipCleanup = daemonSkipCleanup
		return d.Run()
	}

	// Start in background
	exe, err := os.Executable()
	if err != nil {
		return err
	}

	// Use "daemon start --foreground" for the actual daemon process
	// Pass log level via hidden --logging flag for the foreground process
	cmdArgs := []string{"daemon", "start", "--foreground"}
	if logLevel != "" {
		cmdArgs = append(cmdArgs, "--logging", logLevel)
	}
	if daemonSkipCleanup {
		cmdArgs = append(cmdArgs, "--skip-cleanup")
	}
	bgDaemon := exec.Command(exe, cmdArgs...)
	bgDaemon.Stdout = nil
	bgDaemon.Stderr = nil
	bgDaemon.Env = os.Environ() // Inherit environment variables (including LATENTFS_CONFIG_DIR)
	bgDaemon.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true, // Create new session (detach from terminal)
	}

	if err := bgDaemon.Start(); err != nil {
		return fmt.Errorf("failed to start daemon: %w", err)
	}

	// Wait for daemon to be ready (up to 10 seconds with fast polling)
	// Daemon startup includes: open meta DB, find port, start NFS server,
	// wait for port (3s), mount_nfs, start IPC server — total can exceed 5s
	// especially under parallel test contention.
	if util.WaitFixed(400, 25*time.Millisecond, daemon.IsDaemonRunning) {
		pid, _ := daemon.GetPID()
		fmt.Printf("Daemon started (PID %d)\n", pid)
		return nil
	}

	return fmt.Errorf("daemon did not start")
}

func runDaemonStop(cmd *cobra.Command, args []string) error {
	if !daemon.IsDaemonRunning() {
		fmt.Println("Daemon not running")
		// Still do cleanup in case there are stale artifacts
		daemon.CleanupOwnMount()
		return nil
	}

	if err := stopDaemonAndWait(); err != nil {
		return err
	}

	fmt.Println("Daemon stopped")
	return nil
}

// stopDaemonAndWait stops the daemon and waits for it to fully stop.
// It also performs cleanup if the daemon doesn't stop cleanly.
func stopDaemonAndWait() error {
	pid, _ := daemon.GetPID()

	// Connect and send stop request
	client, err := daemon.Connect()
	if err != nil {
		// Can't connect but daemon might still be running
		// Try to clean up anyway
		fmt.Println("Warning: could not connect to daemon, forcing cleanup")
		daemon.CleanupOwnMount()
		return nil
	}

	resp, err := client.Stop()
	client.Close()

	if err != nil {
		return fmt.Errorf("stop request failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}

	// Wait for daemon to actually stop (up to 10 seconds, poll every 25ms)
	stopped := util.WaitFixed(400, 25*time.Millisecond, func() bool {
		return !daemon.IsDaemonRunning()
	})

	if !stopped {
		// Daemon didn't stop gracefully, force cleanup
		fmt.Printf("Warning: daemon (PID %d) did not stop gracefully, forcing cleanup\n", pid)

		// Force kill the process
		if proc, err := os.FindProcess(pid); err == nil {
			proc.Signal(syscall.SIGKILL)
		}

		// Clean up stale mount
		daemon.CleanupOwnMount()

		// Wait a bit more for process to die
		time.Sleep(500 * time.Millisecond)

		if daemon.IsDaemonRunning() {
			return fmt.Errorf("failed to stop daemon (PID %d)", pid)
		}
	}

	// Final cleanup of any stale artifacts
	daemon.CleanupOwnMount()

	return nil
}

func runDaemonStatus(cmd *cobra.Command, args []string) error {
	// Load global settings
	settings, err := daemon.LoadGlobalSettings()
	if err != nil {
		return fmt.Errorf("failed to load settings: %w", err)
	}

	// Daemon status
	if daemon.IsDaemonRunning() {
		pid, _ := daemon.GetPID()
		fmt.Printf("Daemon: running (PID %d)\n", pid)
	} else {
		fmt.Println("Daemon: not running")
	}

	// Auto-start status (merged with LaunchAgent state)
	fmt.Printf("Auto-start on login: %s\n", getAutoStartStatus(settings.LoginStart))
	logLevel := settings.LogLevel
	if logLevel == "" {
		logLevel = "none"
	}
	fmt.Printf("Log level: %s\n", logLevel)

	return nil
}

// getAutoStartStatus returns a human-readable auto-start status that merges
// the config setting with the actual LaunchAgent state (macOS only).
func getAutoStartStatus(loginStart bool) string {
	if !daemon.LaunchAgentSupported() {
		return "not supported"
	}

	if !loginStart {
		return "disabled"
	}

	// LoginStart is enabled - check actual LaunchAgent state
	if !daemon.IsLaunchAgentInstalled() {
		return "enabled (not active - not installed)"
	}
	if !daemon.IsLaunchAgentLoaded() {
		return "enabled (not active - not loaded)"
	}
	return "enabled (active)"
}

func runDaemonConfig(cmd *cobra.Command, args []string) error {
	// Load current settings
	settings, err := daemon.LoadGlobalSettings()
	if err != nil {
		return fmt.Errorf("failed to load settings: %w", err)
	}

	// If no flags provided, show current config
	if configLogLevel == "" && configLoginStart == "" {
		fmt.Println("Current daemon configuration:")
		logLevel := settings.LogLevel
		if logLevel == "" {
			logLevel = "none"
		}
		fmt.Printf("  Log level: %s\n", logLevel)
		fmt.Printf("  Auto-start on login: %s\n", getAutoStartStatus(settings.LoginStart))
		fmt.Println()
		fmt.Println("To change settings:")
		fmt.Println("  latentfs daemon config --logging <level>")
		fmt.Println("  latentfs daemon config --login-start <on|off>")
		return nil
	}

	// Handle --login-start flag
	if configLoginStart != "" {
		if err := handleLoginStartConfig(settings, configLoginStart); err != nil {
			return err
		}
	}

	// Handle --logging flag
	if configLogLevel != "" {
		if err := handleLoggingConfig(settings, configLogLevel); err != nil {
			return err
		}
	}

	return nil
}

// handleLoginStartConfig handles the --login-start flag
func handleLoginStartConfig(settings *daemon.GlobalSettings, value string) error {
	// Check platform support
	if !daemon.LaunchAgentSupported() {
		return fmt.Errorf("auto-start on login is only supported on macOS")
	}

	switch value {
	case "on":
		// Enable auto-start
		settings.LoginStart = true

		// Install LaunchAgent
		if err := daemon.InstallLaunchAgent(); err != nil {
			return fmt.Errorf("failed to install LaunchAgent: %w", err)
		}

		// Load it
		if err := daemon.LoadLaunchAgent(); err != nil {
			// Not fatal - might already be loaded or daemon already running
			fmt.Printf("Note: %v\n", err)
		}

		// Save settings
		if err := daemon.SaveGlobalSettings(settings); err != nil {
			return fmt.Errorf("failed to save settings: %w", err)
		}

		fmt.Println("Auto-start on login enabled")
		fmt.Printf("LaunchAgent installed at: %s\n", daemon.LaunchAgentPath())

	case "off":
		// Disable auto-start
		settings.LoginStart = false

		// Uninstall LaunchAgent
		if err := daemon.UninstallLaunchAgent(); err != nil {
			return fmt.Errorf("failed to uninstall LaunchAgent: %w", err)
		}

		// Save settings
		if err := daemon.SaveGlobalSettings(settings); err != nil {
			return fmt.Errorf("failed to save settings: %w", err)
		}

		fmt.Println("Auto-start on login disabled")

	default:
		return fmt.Errorf("invalid --login-start value %q: must be 'on' or 'off'", value)
	}

	return nil
}

// handleLoggingConfig handles the --logging flag
func handleLoggingConfig(settings *daemon.GlobalSettings, value string) error {
	// Validate log level
	validLevels := map[string]bool{
		"trace": true, "debug": true, "info": true, "warn": true, "none": true, "": true,
	}
	normalizedLevel := value
	if normalizedLevel == "off" {
		normalizedLevel = "none"
	}
	if !validLevels[normalizedLevel] {
		return fmt.Errorf("invalid log level %q: must be one of trace, debug, info, warn, none", value)
	}

	// Update log level
	if normalizedLevel == "none" {
		settings.LogLevel = ""
	} else {
		settings.LogLevel = normalizedLevel
	}

	// Save settings
	if err := daemon.SaveGlobalSettings(settings); err != nil {
		return fmt.Errorf("failed to save settings: %w", err)
	}

	displayLevel := settings.LogLevel
	if displayLevel == "" {
		displayLevel = "none"
	}
	fmt.Printf("Log level set to: %s\n", displayLevel)

	// If daemon is running, notify it to reload config
	if daemon.IsDaemonRunning() {
		client, err := daemon.Connect()
		if err == nil {
			if err := client.ReloadConfig(); err != nil {
				fmt.Printf("Note: Failed to notify daemon: %v\n", err)
				fmt.Println("Restart the daemon for the new log level to take effect:")
				fmt.Println("  latentfs daemon start --restart")
			} else {
				fmt.Println("Daemon notified to reload configuration")
			}
			client.Close()
		}
	}

	return nil
}
