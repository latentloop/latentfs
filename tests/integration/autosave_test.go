// Copyright 2024 LatentFS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	"latentfs/internal/daemon"
	"latentfs/internal/storage"
)

func TestAutosave(t *testing.T) {
	t.Skip("agent command temporarily disabled")

	env := NewSharedDaemonTestEnv(t, "autosave")
	defer env.Cleanup()

	t.Run("autosave creates snapshot on POST_TOOL_USE", func(t *testing.T) {
		g := NewWithT(t)

		// Create project dir with files and config
		projectDir := filepath.Join(env.TestDir, "project_autosave")
		latentfsDir := filepath.Join(projectDir, ".latentfs")
		err := os.MkdirAll(latentfsDir, 0755)
		g.Expect(err).NotTo(HaveOccurred())

		// Write config with autosave enabled
		configContent := "autosave: true\nlogging: none\n"
		err = os.WriteFile(filepath.Join(latentfsDir, "config.yaml"), []byte(configContent), 0644)
		g.Expect(err).NotTo(HaveOccurred())

		// Create some project files
		srcDir := filepath.Join(projectDir, "src")
		os.MkdirAll(srcDir, 0755)
		os.WriteFile(filepath.Join(srcDir, "main.go"), []byte("package main\nfunc main() {}\n"), 0644)
		os.WriteFile(filepath.Join(projectDir, "README.md"), []byte("# Test\n"), 0644)

		// Fire agent notify POST_TOOL_USE (autosave runs synchronously in CLI)
		result := runAgentNotify(env.configDir, projectDir, "", "POST_TOOL_USE")
		g.Expect(result.ExitCode).To(Equal(0), "agent notify should succeed: %s", result.Combined)

		// Autosave runs synchronously in CLI, but use Eventually for robustness
		dataFilePath := filepath.Join(latentfsDir, "autosave.latentfs")
		g.Eventually(func() bool {
			_, err := os.Stat(dataFilePath)
			return err == nil
		}).WithTimeout(10 * time.Second).WithPolling(200 * time.Millisecond).Should(BeTrue(),
			"autosave data file should be created")

		// Verify snapshot exists in the data file
		g.Eventually(func() int {
			df, err := storage.Open(dataFilePath)
			if err != nil {
				return 0
			}
			defer df.Close()
			snapshots, err := df.ListSnapshots()
			if err != nil {
				return 0
			}
			return len(snapshots)
		}).WithTimeout(10 * time.Second).WithPolling(500 * time.Millisecond).Should(BeNumerically(">=", 1),
			"at least one snapshot should exist")

		// Open data file and verify contents
		df, err := storage.Open(dataFilePath)
		g.Expect(err).NotTo(HaveOccurred())
		defer df.Close()

		snapshots, err := df.ListSnapshots()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(len(snapshots)).To(BeNumerically(">=", 1))
		// Message format: "==Auto Save== session: {id}" or "==Auto Save=="
		// followed by tree-style file diff with + for added files
		g.Expect(snapshots[0].Message).To(HavePrefix("==Auto Save=="))
		g.Expect(snapshots[0].Message).To(ContainSubstring("+ ")) // tree format uses "+ " for added files
	})

	t.Run("autosave creates snapshot on PRE_TOOL_USE", func(t *testing.T) {
		g := NewWithT(t)

		// Create project dir with files and config
		projectDir := filepath.Join(env.TestDir, "project_autosave_pre")
		latentfsDir := filepath.Join(projectDir, ".latentfs")
		err := os.MkdirAll(latentfsDir, 0755)
		g.Expect(err).NotTo(HaveOccurred())

		// Write config with autosave enabled
		configContent := "autosave: true\nlogging: none\n"
		err = os.WriteFile(filepath.Join(latentfsDir, "config.yaml"), []byte(configContent), 0644)
		g.Expect(err).NotTo(HaveOccurred())

		// Create some project files
		srcDir := filepath.Join(projectDir, "src")
		os.MkdirAll(srcDir, 0755)
		os.WriteFile(filepath.Join(srcDir, "main.go"), []byte("package main\nfunc main() {}\n"), 0644)
		os.WriteFile(filepath.Join(projectDir, "README.md"), []byte("# Test PRE\n"), 0644)

		// Fire agent notify PRE_TOOL_USE (autosave runs synchronously in CLI)
		result := runAgentNotify(env.configDir, projectDir, "", "PRE_TOOL_USE")
		g.Expect(result.ExitCode).To(Equal(0), "agent notify should succeed: %s", result.Combined)

		// Autosave runs synchronously in CLI, but use Eventually for robustness
		dataFilePath := filepath.Join(latentfsDir, "autosave.latentfs")
		g.Eventually(func() bool {
			_, err := os.Stat(dataFilePath)
			return err == nil
		}).WithTimeout(10 * time.Second).WithPolling(200 * time.Millisecond).Should(BeTrue(),
			"autosave data file should be created on PRE_TOOL_USE")

		// Verify snapshot exists in the data file
		g.Eventually(func() int {
			df, err := storage.Open(dataFilePath)
			if err != nil {
				return 0
			}
			defer df.Close()
			snapshots, err := df.ListSnapshots()
			if err != nil {
				return 0
			}
			return len(snapshots)
		}).WithTimeout(10 * time.Second).WithPolling(500 * time.Millisecond).Should(BeNumerically(">=", 1),
			"at least one snapshot should exist from PRE_TOOL_USE")

		// Open data file and verify contents
		df, err := storage.Open(dataFilePath)
		g.Expect(err).NotTo(HaveOccurred())
		defer df.Close()

		snapshots, err := df.ListSnapshots()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(len(snapshots)).To(BeNumerically(">=", 1))
		g.Expect(snapshots[0].Message).To(HavePrefix("==Auto Save=="))
	})

	t.Run("autosave excludes .latentfs directory", func(t *testing.T) {
		g := NewWithT(t)

		projectDir := filepath.Join(env.TestDir, "project_exclude_latentfs")
		latentfsDir := filepath.Join(projectDir, ".latentfs")
		os.MkdirAll(latentfsDir, 0755)

		configContent := "autosave: true\n"
		os.WriteFile(filepath.Join(latentfsDir, "config.yaml"), []byte(configContent), 0644)
		os.WriteFile(filepath.Join(latentfsDir, "some_file.txt"), []byte("internal"), 0644)

		// Create a project file
		os.WriteFile(filepath.Join(projectDir, "hello.txt"), []byte("hello world"), 0644)

		result := runAgentNotify(env.configDir, projectDir, "", "POST_TOOL_USE")
		g.Expect(result.ExitCode).To(Equal(0))

		dataFilePath := filepath.Join(latentfsDir, "autosave.latentfs")
		g.Eventually(func() bool {
			_, err := os.Stat(dataFilePath)
			return err == nil
		}).WithTimeout(10 * time.Second).WithPolling(200 * time.Millisecond).Should(BeTrue())

		// Wait for snapshot
		g.Eventually(func() int {
			df, err := storage.Open(dataFilePath)
			if err != nil {
				return 0
			}
			defer df.Close()
			snaps, _ := df.ListSnapshots()
			return len(snaps)
		}).WithTimeout(10 * time.Second).WithPolling(500 * time.Millisecond).Should(BeNumerically(">=", 1))

		df, err := storage.Open(dataFilePath)
		g.Expect(err).NotTo(HaveOccurred())
		defer df.Close()

		// Collect all paths in the latest snapshot
		paths := df.CollectAllSnapshotPaths()

		// .latentfs should NOT be in the snapshot
		for _, p := range paths {
			g.Expect(p).NotTo(HavePrefix("/.latentfs"), "snapshot should not contain .latentfs paths, but found: %s", p)
		}

		// hello.txt SHOULD be in the snapshot
		g.Expect(paths).To(ContainElement("/hello.txt"))
	})

	t.Run("autosave respects gitignore", func(t *testing.T) {
		g := NewWithT(t)

		projectDir := filepath.Join(env.TestDir, "project_gitignore")
		latentfsDir := filepath.Join(projectDir, ".latentfs")
		os.MkdirAll(latentfsDir, 0755)

		configContent := "autosave: true\ngitignore: true\nincludes:\n  - \".git\"\n"
		os.WriteFile(filepath.Join(latentfsDir, "config.yaml"), []byte(configContent), 0644)

		// Create .gitignore
		os.WriteFile(filepath.Join(projectDir, ".gitignore"), []byte("*.log\nbuild/\n"), 0644)

		// Create files: some ignored, some not
		os.WriteFile(filepath.Join(projectDir, "main.go"), []byte("package main"), 0644)
		os.WriteFile(filepath.Join(projectDir, "debug.log"), []byte("log data"), 0644)
		os.MkdirAll(filepath.Join(projectDir, "build"), 0755)
		os.WriteFile(filepath.Join(projectDir, "build", "out.bin"), []byte("binary"), 0644)

		// Create .git dir (should be included via includes override)
		os.MkdirAll(filepath.Join(projectDir, ".git"), 0755)
		os.WriteFile(filepath.Join(projectDir, ".git", "HEAD"), []byte("ref: refs/heads/main\n"), 0644)

		result := runAgentNotify(env.configDir, projectDir, "", "POST_TOOL_USE")
		g.Expect(result.ExitCode).To(Equal(0))

		dataFilePath := filepath.Join(latentfsDir, "autosave.latentfs")
		g.Eventually(func() int {
			df, err := storage.Open(dataFilePath)
			if err != nil {
				return 0
			}
			defer df.Close()
			snaps, _ := df.ListSnapshots()
			return len(snaps)
		}).WithTimeout(10 * time.Second).WithPolling(500 * time.Millisecond).Should(BeNumerically(">=", 1))

		df, err := storage.Open(dataFilePath)
		g.Expect(err).NotTo(HaveOccurred())
		defer df.Close()

		paths := df.CollectAllSnapshotPaths()

		// main.go and .gitignore should be present
		g.Expect(paths).To(ContainElement("/main.go"))
		g.Expect(paths).To(ContainElement("/.gitignore"))

		// .git should be present (includes override)
		g.Expect(paths).To(ContainElement("/.git"))
		g.Expect(paths).To(ContainElement("/.git/HEAD"))

		// debug.log and build/ should be excluded (gitignored)
		g.Expect(paths).NotTo(ContainElement("/debug.log"))
		g.Expect(paths).NotTo(ContainElement("/build"))
		g.Expect(paths).NotTo(ContainElement("/build/out.bin"))
	})

	t.Run("autosave skips when not enabled", func(t *testing.T) {
		g := NewWithT(t)

		projectDir := filepath.Join(env.TestDir, "project_no_autosave")
		latentfsDir := filepath.Join(projectDir, ".latentfs")
		os.MkdirAll(latentfsDir, 0755)

		configContent := "autosave: false\n"
		os.WriteFile(filepath.Join(latentfsDir, "config.yaml"), []byte(configContent), 0644)
		os.WriteFile(filepath.Join(projectDir, "file.txt"), []byte("data"), 0644)

		result := runAgentNotify(env.configDir, projectDir, "", "POST_TOOL_USE")
		g.Expect(result.ExitCode).To(Equal(0))

		// Autosave runs synchronously in CLI, so file should not exist immediately
		dataFilePath := filepath.Join(latentfsDir, "autosave.latentfs")
		_, err := os.Stat(dataFilePath)
		g.Expect(os.IsNotExist(err)).To(BeTrue(), "autosave data file should NOT be created when autosave=false")
	})

	t.Run("sequential autosave creates multiple snapshots", func(t *testing.T) {
		g := NewWithT(t)

		projectDir := filepath.Join(env.TestDir, "project_sequential")
		latentfsDir := filepath.Join(projectDir, ".latentfs")
		os.MkdirAll(latentfsDir, 0755)

		// Use aggressive strategy to ensure second save triggers
		configContent := "autosave: true\nsave-strategy: aggressive\n"
		os.WriteFile(filepath.Join(latentfsDir, "config.yaml"), []byte(configContent), 0644)

		// Create initial files
		os.WriteFile(filepath.Join(projectDir, "file.txt"), []byte("initial content"), 0644)

		// First autosave
		result1 := runAgentNotify(env.configDir, projectDir, "", "POST_TOOL_USE")
		g.Expect(result1.ExitCode).To(Equal(0))

		// Get aggressive strategy threshold from config and create content that exceeds it
		strategy := daemon.GetSaveStrategy("aggressive")
		contentSize := strategy.SizeThreshold + 512 // Exceed threshold by 512 bytes
		largeContent := strings.Repeat("x", int(contentSize))
		os.WriteFile(filepath.Join(projectDir, "file.txt"), []byte(largeContent), 0644)

		// Second autosave
		result2 := runAgentNotify(env.configDir, projectDir, "", "POST_TOOL_USE")
		g.Expect(result2.ExitCode).To(Equal(0))

		// Autosave runs synchronously in CLI, so snapshots should already exist
		dataFilePath := filepath.Join(latentfsDir, "autosave.latentfs")
		df, err := storage.Open(dataFilePath)
		g.Expect(err).NotTo(HaveOccurred())
		defer df.Close()

		snapshots, err := df.ListSnapshots()
		g.Expect(err).NotTo(HaveOccurred())

		// Both calls should create snapshots (content changed between calls)
		g.Expect(len(snapshots)).To(Equal(2))
	})

	t.Run("save strategy skips autosave when threshold not met", func(t *testing.T) {
		g := NewWithT(t)

		projectDir := filepath.Join(env.TestDir, "project_strategy_skip")
		latentfsDir := filepath.Join(projectDir, ".latentfs")
		os.MkdirAll(latentfsDir, 0755)

		// Use passive strategy - small changes should be skipped
		configContent := "autosave: true\nsave-strategy: passive\n"
		os.WriteFile(filepath.Join(latentfsDir, "config.yaml"), []byte(configContent), 0644)

		// Create initial file
		os.WriteFile(filepath.Join(projectDir, "file.txt"), []byte("initial content"), 0644)

		// First autosave (new file always saves)
		result1 := runAgentNotify(env.configDir, projectDir, "", "POST_TOOL_USE")
		g.Expect(result1.ExitCode).To(Equal(0))

		// Small modification (well below passive threshold from config)
		strategy := daemon.GetSaveStrategy("passive")
		smallContent := strings.Repeat("x", int(strategy.SizeThreshold/10)) // Only 10% of threshold
		os.WriteFile(filepath.Join(projectDir, "file.txt"), []byte(smallContent), 0644)

		// Second autosave - should be skipped (below threshold)
		result2 := runAgentNotify(env.configDir, projectDir, "", "POST_TOOL_USE")
		g.Expect(result2.ExitCode).To(Equal(0))

		// Only one snapshot should exist (second was skipped)
		dataFilePath := filepath.Join(latentfsDir, "autosave.latentfs")
		df, err := storage.Open(dataFilePath)
		g.Expect(err).NotTo(HaveOccurred())
		defer df.Close()

		snapshots, err := df.ListSnapshots()
		g.Expect(err).NotTo(HaveOccurred())

		// Only one snapshot (second call was below threshold)
		g.Expect(len(snapshots)).To(Equal(1))
	})

	t.Run("autosave respects excludes", func(t *testing.T) {
		g := NewWithT(t)

		projectDir := filepath.Join(env.TestDir, "project_excludes")
		latentfsDir := filepath.Join(projectDir, ".latentfs")
		os.MkdirAll(latentfsDir, 0755)

		// Config with excludes: node_modules and vendor should be excluded
		configContent := "autosave: true\nexcludes:\n  - \"node_modules\"\n  - \"vendor\"\n"
		os.WriteFile(filepath.Join(latentfsDir, "config.yaml"), []byte(configContent), 0644)

		// Create files: some in excluded dirs, some not
		os.WriteFile(filepath.Join(projectDir, "main.go"), []byte("package main"), 0644)
		os.MkdirAll(filepath.Join(projectDir, "node_modules", "pkg"), 0755)
		os.WriteFile(filepath.Join(projectDir, "node_modules", "pkg", "index.js"), []byte("module.exports = {}"), 0644)
		os.MkdirAll(filepath.Join(projectDir, "vendor"), 0755)
		os.WriteFile(filepath.Join(projectDir, "vendor", "lib.go"), []byte("package vendor"), 0644)
		os.WriteFile(filepath.Join(projectDir, "src", "app.go"), []byte("package src"), 0644)
		os.MkdirAll(filepath.Join(projectDir, "src"), 0755)
		os.WriteFile(filepath.Join(projectDir, "src", "app.go"), []byte("package src"), 0644)

		result := runAgentNotify(env.configDir, projectDir, "", "POST_TOOL_USE")
		g.Expect(result.ExitCode).To(Equal(0))

		dataFilePath := filepath.Join(latentfsDir, "autosave.latentfs")
		g.Eventually(func() int {
			df, err := storage.Open(dataFilePath)
			if err != nil {
				return 0
			}
			defer df.Close()
			snaps, _ := df.ListSnapshots()
			return len(snaps)
		}).WithTimeout(10 * time.Second).WithPolling(500 * time.Millisecond).Should(BeNumerically(">=", 1))

		df, err := storage.Open(dataFilePath)
		g.Expect(err).NotTo(HaveOccurred())
		defer df.Close()

		paths := df.CollectAllSnapshotPaths()

		// main.go and src/app.go should be present
		g.Expect(paths).To(ContainElement("/main.go"))
		g.Expect(paths).To(ContainElement("/src/app.go"))

		// node_modules and vendor should be excluded
		g.Expect(paths).NotTo(ContainElement("/node_modules"))
		g.Expect(paths).NotTo(ContainElement("/node_modules/pkg"))
		g.Expect(paths).NotTo(ContainElement("/node_modules/pkg/index.js"))
		g.Expect(paths).NotTo(ContainElement("/vendor"))
		g.Expect(paths).NotTo(ContainElement("/vendor/lib.go"))
	})

	t.Run("snapshot includes file_count and total_size", func(t *testing.T) {
		g := NewWithT(t)

		projectDir := filepath.Join(env.TestDir, "project_summary")
		latentfsDir := filepath.Join(projectDir, ".latentfs")
		os.MkdirAll(latentfsDir, 0755)

		configContent := "autosave: true\n"
		os.WriteFile(filepath.Join(latentfsDir, "config.yaml"), []byte(configContent), 0644)

		// Create files with known sizes
		// file1: 100 bytes, file2: 200 bytes, file3: 300 bytes = 600 bytes total, 3 files
		os.WriteFile(filepath.Join(projectDir, "file1.txt"), make([]byte, 100), 0644)
		os.WriteFile(filepath.Join(projectDir, "file2.txt"), make([]byte, 200), 0644)
		os.WriteFile(filepath.Join(projectDir, "file3.txt"), make([]byte, 300), 0644)

		result := runAgentNotify(env.configDir, projectDir, "", "POST_TOOL_USE")
		g.Expect(result.ExitCode).To(Equal(0))

		dataFilePath := filepath.Join(latentfsDir, "autosave.latentfs")
		g.Eventually(func() int {
			df, err := storage.Open(dataFilePath)
			if err != nil {
				return 0
			}
			defer df.Close()
			snaps, _ := df.ListSnapshots()
			return len(snaps)
		}).WithTimeout(10 * time.Second).WithPolling(500 * time.Millisecond).Should(BeNumerically(">=", 1))

		df, err := storage.Open(dataFilePath)
		g.Expect(err).NotTo(HaveOccurred())
		defer df.Close()

		snapshots, err := df.ListSnapshots()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(len(snapshots)).To(BeNumerically(">=", 1))

		// Verify file_count and total_size
		snap := snapshots[0]
		g.Expect(snap.FileCount).To(Equal(int64(3)), "should have 3 files")
		g.Expect(snap.TotalSize).To(Equal(int64(600)), "total size should be 600 bytes")
	})
}
