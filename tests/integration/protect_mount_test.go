package integration

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/gomega"
)

func TestProtectedDataFileCanBeMounted(t *testing.T) {
	t.Skip("agent command temporarily disabled")

	env := NewSharedDaemonTestEnv(t, "protect_mount")
	defer env.Cleanup()

	t.Run("protected datafile can be mounted", func(t *testing.T) {
		g := NewWithT(t)

		// Create project dir with autosave config
		projectDir := filepath.Join(env.TestDir, "project_protect_mount")
		latentfsDir := filepath.Join(projectDir, ".latentfs")
		os.MkdirAll(latentfsDir, 0755)

		// Write config with autosave enabled
		configContent := "autosave: true\n"
		os.WriteFile(filepath.Join(latentfsDir, "config.yaml"), []byte(configContent), 0644)

		// Create a project file
		os.WriteFile(filepath.Join(projectDir, "hello.txt"), []byte("hello world"), 0644)

		// Trigger autosave (this will create datafile)
		result := runAgentNotify(env.configDir, projectDir, "", "POST_TOOL_USE")
		g.Expect(result.ExitCode).To(Equal(0), "agent notify should succeed: %s", result.Combined)

		dataFilePath := filepath.Join(latentfsDir, "autosave.latentfs")

		// Explicitly protect the file
		protectResult := env.RunCLI("agent", "protect", dataFilePath)
		g.Expect(protectResult.ExitCode).To(Equal(0), "protect should succeed: %s", protectResult.Combined)

		// Verify the file is protected
		lsResult := env.RunCLI("agent", "ls-protect")
		g.Expect(lsResult.ExitCode).To(Equal(0))
		g.Expect(lsResult.Stdout).To(ContainSubstring("autosave.latentfs"))

		// Now try to mount the same datafile (should work even though protected)
		// This verifies that protection (keeping file open) doesn't block mounting
		targetPath := filepath.Join(env.TestDir, "mount_target")

		mountResult := env.RunCLI("mount", targetPath, "-d", dataFilePath)
		g.Expect(mountResult.ExitCode).To(Equal(0), "mount should succeed even with protected datafile: %s", mountResult.Combined)
		g.Expect(mountResult.Stdout).To(ContainSubstring("Mounted"), "mount output should indicate success")

		// Verify mount is listed
		mountLsResult := env.RunCLI("mount", "ls")
		g.Expect(mountLsResult.ExitCode).To(Equal(0))
		g.Expect(mountLsResult.Stdout).To(ContainSubstring("autosave.latentfs"), "mounted datafile should appear in mount list")

		// Clean up mount
		env.RunCLI("unmount", targetPath)
	})
}
