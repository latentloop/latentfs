package commands

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"

	"latentfs/internal/daemon"
)

var unmountCmd = &cobra.Command{
	Use:     "unmount <mount-point>",
	Aliases: []string{"umount"},
	Short:   "Unmount a folder",
	Long: `Unmounts a mounted latentfs folder.

Use --all to unmount all folders (daemon continues running).`,
	Args: cobra.MaximumNArgs(1),
	RunE: runUnmount,
}

var unmountAll bool

func init() {
	unmountCmd.Flags().BoolVarP(&unmountAll, "all", "a", false, "Unmount all folders")
	rootCmd.AddCommand(unmountCmd)
}

func runUnmount(cmd *cobra.Command, args []string) error {
	if !unmountAll && len(args) == 0 {
		return fmt.Errorf("mount point required (or use --all)")
	}

	// Check daemon is running
	if !daemon.IsDaemonRunning() {
		return fmt.Errorf("daemon is not running")
	}

	// Connect to daemon
	client, err := daemon.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to daemon: %w", err)
	}
	defer client.Close()

	var target string
	if !unmountAll && len(args) > 0 {
		target, _ = filepath.Abs(args[0])
	}

	// Send unmount request
	resp, err := client.Unmount(target, unmountAll)
	if err != nil {
		return fmt.Errorf("unmount request failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}

	fmt.Println(resp.Message)
	return nil
}
