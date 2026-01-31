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
	"strings"
	"testing"

	. "github.com/onsi/gomega"
)

func TestDataRemoveCommand(t *testing.T) {
	// Share daemon across all subtests
	shared := NewSharedDaemonTestEnv(t, "data_remove")
	defer shared.Cleanup()

	t.Run("remove by tag with confirmation skip", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "tag")
		defer env.Cleanup()

		env.InitDataFile()

		// Create file and snapshot
		env.WriteFile("test.txt", "test content")

		result := env.RunCLI("data", "save", "-m", "test snapshot", "-t", "v1",
			"--data-file", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to save snapshot: %s", result.Combined)
		}

		// Verify snapshot exists
		result = env.RunCLI("data", "ls-saves", "--data-file", env.DataFile)
		env.g.Expect(result.Combined).To(ContainSubstring("v1"))

		// Remove snapshot with -y flag
		result = env.RunCLI("data", "remove", "v1", "-y", "--data-file", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to remove snapshot: %s", result.Combined)
		}
		env.g.Expect(result.Combined).To(ContainSubstring("Removed snapshot"))

		// Verify snapshot is gone
		result = env.RunCLI("data", "ls-saves", "--data-file", env.DataFile)
		env.g.Expect(result.Combined).NotTo(ContainSubstring("v1"))
	})

	t.Run("remove preserves other snapshots", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "preserve")
		defer env.Cleanup()

		env.InitDataFile()

		// Create file and two snapshots
		env.WriteFile("test.txt", "test content")

		env.RunCLI("data", "save", "-m", "snapshot A", "-t", "v1",
			"--data-file", env.DataFile)
		env.RunCLI("data", "save", "-m", "snapshot B", "-t", "v2",
			"--data-file", env.DataFile)

		// Verify both exist
		result := env.RunCLI("data", "ls-saves", "--data-file", env.DataFile)
		env.g.Expect(result.Combined).To(ContainSubstring("v1"))
		env.g.Expect(result.Combined).To(ContainSubstring("v2"))

		// Remove v1
		env.RunCLI("data", "remove", "v1", "-y", "--data-file", env.DataFile)

		// Verify v1 is gone but v2 remains
		result = env.RunCLI("data", "ls-saves", "--data-file", env.DataFile)
		env.g.Expect(result.Combined).NotTo(ContainSubstring("v1"))
		env.g.Expect(result.Combined).To(ContainSubstring("v2"))
	})

	t.Run("remove nonexistent snapshot fails", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "notfound")
		defer env.Cleanup()

		env.InitDataFile()

		result := env.RunCLI("data", "remove", "nonexistent", "-y",
			"--data-file", env.DataFile)
		env.g.Expect(result.ExitCode).NotTo(Equal(0))
		env.g.Expect(result.Combined).To(ContainSubstring("not found"))
	})
}

func TestDataGCCommand(t *testing.T) {
	// Share daemon across all subtests
	shared := NewSharedDaemonTestEnv(t, "data_gc")
	defer shared.Cleanup()

	t.Run("gc stats shows storage info", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "stats")
		defer env.Cleanup()

		env.InitDataFile()

		// Create file and snapshot
		env.WriteFile("test.txt", "test content for gc")
		env.RunCLI("data", "save", "-m", "test", "-t", "v1",
			"--data-file", env.DataFile)

		// Run gc --stats
		result := env.RunCLI("data", "gc", "--stats", "--data-file", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to run gc --stats: %s", result.Combined)
		}

		// Verify output format
		env.g.Expect(result.Combined).To(ContainSubstring("Storage:"))
		env.g.Expect(result.Combined).To(ContainSubstring("snapshots"))
		env.g.Expect(result.Combined).To(ContainSubstring("content blocks"))
	})

	t.Run("gc cleans orphaned content after snapshot removal", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "clean")
		defer env.Cleanup()

		env.InitDataFile()

		// Create file and snapshot
		env.WriteFile("test.txt", "unique content for orphan test")
		env.RunCLI("data", "save", "-m", "test", "-t", "v1",
			"--data-file", env.DataFile)

		// Check initial stats
		result := env.RunCLI("data", "gc", "--stats", "--data-file", env.DataFile)
		env.g.Expect(result.Combined).To(ContainSubstring("1 snapshots"))

		// Remove snapshot
		env.RunCLI("data", "remove", "v1", "-y", "--data-file", env.DataFile)

		// Check stats show orphaned blocks
		result = env.RunCLI("data", "gc", "--stats", "--data-file", env.DataFile)
		// Should show 0 snapshots and reclaimable data
		env.g.Expect(result.Combined).To(ContainSubstring("0 snapshots"))
		env.g.Expect(result.Combined).To(ContainSubstring("Reclaimable:"))
		env.g.Expect(result.Combined).To(ContainSubstring("orphaned blocks"))

		// Run actual GC
		result = env.RunCLI("data", "gc", "--data-file", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to run gc: %s", result.Combined)
		}
		env.g.Expect(result.Combined).To(ContainSubstring("Cleaned:"))

		// Verify orphaned blocks are gone
		result = env.RunCLI("data", "gc", "--stats", "--data-file", env.DataFile)
		env.g.Expect(result.Combined).To(ContainSubstring("Reclaimable: none"))
	})

	t.Run("gc preserves live data", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "preserves_live")
		defer env.Cleanup()

		env.InitDataFile()

		// Create file
		env.WriteFile("live.txt", "live content")

		// Create snapshot then remove it
		env.RunCLI("data", "save", "-m", "test", "-t", "v1",
			"--data-file", env.DataFile)
		env.RunCLI("data", "remove", "v1", "-y", "--data-file", env.DataFile)

		// Run GC
		env.RunCLI("data", "gc", "--data-file", env.DataFile)

		// Verify live file still accessible
		content := env.ReadFile("live.txt")
		env.g.Expect(content).To(Equal("live content"))
	})

	t.Run("gc with vacuum", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "vacuum")
		defer env.Cleanup()

		env.InitDataFile()

		// Create file and snapshot
		env.WriteFile("test.txt", "test content")
		env.RunCLI("data", "save", "-m", "test", "-t", "v1",
			"--data-file", env.DataFile)

		// Remove snapshot to create orphaned data
		env.RunCLI("data", "remove", "v1", "-y", "--data-file", env.DataFile)

		// Run GC with vacuum
		result := env.RunCLI("data", "gc", "--vacuum", "--data-file", env.DataFile)
		if result.ExitCode != 0 {
			t.Fatalf("Failed to run gc --vacuum: %s", result.Combined)
		}
		env.g.Expect(result.Combined).To(ContainSubstring("VACUUM completed"))
	})
}

func TestSaveMessageLengthValidation(t *testing.T) {
	// Share daemon across all subtests
	shared := NewSharedDaemonTestEnv(t, "save_msg")
	defer shared.Cleanup()

	t.Run("message at max length succeeds", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "max")
		defer env.Cleanup()

		env.InitDataFile()

		// Create 4096 character message
		message := strings.Repeat("a", 4096)

		result := env.RunCLI("data", "save", "-m", message, "-t", "v1",
			"--data-file", env.DataFile)
		env.g.Expect(result.ExitCode).To(Equal(0))
	})

	t.Run("message exceeding max length is truncated", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "exceed")
		defer env.Cleanup()

		env.InitDataFile()

		// Create 4097 character message
		message := strings.Repeat("a", 4097)

		result := env.RunCLI("data", "save", "-m", message, "-t", "v1",
			"--data-file", env.DataFile)
		env.g.Expect(result.ExitCode).To(Equal(0))
		env.g.Expect(result.Combined).To(ContainSubstring("truncated to 4096"))
	})
}
