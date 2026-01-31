package commands

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"latentfs/internal/artifacts"
)

var initCmd = &cobra.Command{
	Use:   "init [directory]",
	Short: "Initialize a LatentFS project",
	Long: `Initialize a new LatentFS project in the specified directory (or current directory).

Creates a .latentfs directory with default configuration files.
Similar to 'git init', this prepares the directory for LatentFS operations.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runInit,
}

func init() {
	rootCmd.AddCommand(initCmd)
}

func runInit(cmd *cobra.Command, args []string) error {
	// Determine target directory
	targetDir := "."
	if len(args) > 0 {
		targetDir = args[0]
	}

	// Resolve to absolute path
	absDir, err := filepath.Abs(targetDir)
	if err != nil {
		return fmt.Errorf("failed to resolve path: %w", err)
	}

	// Create .latentfs directory
	latentfsDir := filepath.Join(absDir, ".latentfs")
	if _, err := os.Stat(latentfsDir); err == nil {
		fmt.Printf("Reinitialized existing LatentFS project in %s\n", latentfsDir)
	} else {
		if err := os.MkdirAll(latentfsDir, 0755); err != nil {
			return fmt.Errorf("failed to create .latentfs directory: %w", err)
		}
		fmt.Printf("Initialized empty LatentFS project in %s\n", latentfsDir)
	}

	// Write config.yaml
	configPath := filepath.Join(latentfsDir, "config.yaml")
	if _, err := os.Stat(configPath); err == nil {
		// Config already exists, don't overwrite
		fmt.Printf("  config.yaml already exists (not modified)\n")
	} else {
		if err := os.WriteFile(configPath, artifacts.ProjectConfig, 0644); err != nil {
			return fmt.Errorf("failed to write config.yaml: %w", err)
		}
		fmt.Printf("  created config.yaml\n")
	}

	return nil
}
