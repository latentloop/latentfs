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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestNFSPerf measures NFS performance across cp-r, rm-rf, round-trip, and
// regression guard scenarios. All subtests share a single daemon to avoid
// repeated daemon start/stop overhead (~2-3s each).
func TestNFSPerf(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	shared := NewSharedDaemonTestEnv(t, "nfs_perf")
	defer shared.Cleanup()

	// --- cp -r ---

	t.Run("CpR/FlatDir_100SmallFiles", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "cpr_flat100")
		defer env.Cleanup()
		env.InitDataFile()

		srcDir := filepath.Join(env.TestDir, "src_flat100")
		totalFiles := createLocalTree(t, srcDir, 100, 0, 1024)

		dst := filepath.Join(env.Mount, "copied")
		dur := timedCpR(t, srcDir, dst)
		reportPerf(t, "cp-r flat 100 small files (1KB)", totalFiles, dur)

		verifyTreeCopied(t, dst, totalFiles)
	})

	t.Run("CpR/FlatDir_200SmallFiles", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "cpr_flat200")
		defer env.Cleanup()
		env.InitDataFile()

		srcDir := filepath.Join(env.TestDir, "src_flat200")
		totalFiles := createLocalTree(t, srcDir, 200, 0, 256)

		dst := filepath.Join(env.Mount, "copied")
		dur := timedCpR(t, srcDir, dst)
		reportPerf(t, "cp-r flat 200 small files (256B)", totalFiles, dur)

		verifyTreeCopied(t, dst, totalFiles)
	})

	t.Run("CpR/DeepTree_4Levels", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "cpr_deep4")
		defer env.Cleanup()
		env.InitDataFile()

		srcDir := filepath.Join(env.TestDir, "src_deep4")
		totalFiles := createDeepTree(t, srcDir, "", 4, 3, 5)

		dst := filepath.Join(env.Mount, "copied")
		dur := timedCpR(t, srcDir, dst)
		reportPerf(t, "cp-r deep 4-level tree", totalFiles, dur)

		verifyTreeCopied(t, dst, totalFiles)
	})

	t.Run("CpR/WideDir_200SmallFiles", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "cpr_wide200")
		defer env.Cleanup()
		env.InitDataFile()

		srcDir := filepath.Join(env.TestDir, "src_wide200")
		totalFiles := createLocalTree(t, srcDir, 200, 0, 64)

		dst := filepath.Join(env.Mount, "copied")
		dur := timedCpR(t, srcDir, dst)
		reportPerf(t, "cp-r wide 200 small files (64B)", totalFiles, dur)

		verifyTreeCopied(t, dst, totalFiles)
	})

	t.Run("CpR/MediumFiles_50x32KB", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "cpr_med50")
		defer env.Cleanup()
		env.InitDataFile()

		srcDir := filepath.Join(env.TestDir, "src_med50")
		totalFiles := createLocalTree(t, srcDir, 50, 0, 32*1024)

		dst := filepath.Join(env.Mount, "copied")
		dur := timedCpR(t, srcDir, dst)
		reportPerf(t, "cp-r 50 medium files (32KB)", totalFiles, dur)

		verifyTreeCopied(t, dst, totalFiles)
	})

	t.Run("CpR/LargeFiles_10x256KB", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "cpr_large10")
		defer env.Cleanup()
		env.InitDataFile()

		srcDir := filepath.Join(env.TestDir, "src_large10")
		totalFiles := createLocalTree(t, srcDir, 10, 0, 256*1024)

		dst := filepath.Join(env.Mount, "copied")
		dur := timedCpR(t, srcDir, dst)
		totalBytes := int64(10 * 256 * 1024)
		t.Logf("  Throughput: %.2f MB/s", float64(totalBytes)/1024/1024/dur.Seconds())
		reportPerf(t, "cp-r 10 large files (256KB)", totalFiles, dur)

		verifyTreeCopied(t, dst, totalFiles)
	})

	// --- rm -rf (empty fork / no source) ---

	t.Run("RmRf/Empty_FlatDir_100Files", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "rmrf_flat100")
		defer env.Cleanup()
		env.InitDataFile()

		for i := 0; i < 100; i++ {
			env.WriteFile(fmt.Sprintf("batch/%04d.txt", i), fmt.Sprintf("content-%d", i))
		}

		target := filepath.Join(env.Mount, "batch")
		dur := timedRemoveAll(t, target)
		reportPerf(t, "rm-rf empty flat 100 files", 100, dur)

		if env.FileExists("batch") {
			t.Error("directory should not exist after RemoveAll")
		}
	})

	t.Run("RmRf/Empty_FlatDir_300Files", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "rmrf_flat300")
		defer env.Cleanup()
		env.InitDataFile()

		for i := 0; i < 300; i++ {
			env.WriteFile(fmt.Sprintf("batch/%04d.txt", i), "x")
		}

		target := filepath.Join(env.Mount, "batch")
		dur := timedRemoveAll(t, target)
		reportPerf(t, "rm-rf empty flat 300 files", 300, dur)

		if env.FileExists("batch") {
			t.Error("directory should not exist after RemoveAll")
		}
	})

	t.Run("RmRf/Empty_DeepTree_3Levels", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "rmrf_deep3")
		defer env.Cleanup()
		env.InitDataFile()

		totalFiles := createDeepTree(t, env.Mount, "deep", 3, 3, 5)

		target := filepath.Join(env.Mount, "deep")
		dur := timedRemoveAll(t, target)
		reportPerf(t, "rm-rf empty deep 3-level tree", totalFiles, dur)

		if env.FileExists("deep") {
			t.Error("directory should not exist after RemoveAll")
		}
	})

	t.Run("RmRf/Empty_WideDir_200Files", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "rmrf_wide200")
		defer env.Cleanup()
		env.InitDataFile()

		for i := 0; i < 200; i++ {
			env.WriteFile(fmt.Sprintf("wide/%04d.txt", i), "x")
		}

		target := filepath.Join(env.Mount, "wide")
		dur := timedRemoveAll(t, target)
		reportPerf(t, "rm-rf empty wide 200 files", 200, dur)

		if env.FileExists("wide") {
			t.Error("directory should not exist after RemoveAll")
		}
	})

	// --- rm -rf (overlay / with source) ---

	t.Run("RmRf/Overlay_FlatDir_100Files", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "rmrf_sf_flat100")
		defer env.Cleanup()

		for i := 0; i < 100; i++ {
			env.CreateSourceFile(fmt.Sprintf("batch/%04d.txt", i), fmt.Sprintf("content-%d", i))
		}
		env.ForkWithSource()

		target := filepath.Join(env.TargetPath, "batch")
		dur := timedRemoveAll(t, target)
		reportPerf(t, "rm-rf overlay flat 100 files", 100, dur)

		if env.TargetFileExists("batch") {
			t.Error("directory should not exist after RemoveAll")
		}
	})

	t.Run("RmRf/Overlay_DeepTree_3Levels", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "rmrf_sf_deep3")
		defer env.Cleanup()

		totalFiles := createDeepTree(t, env.SourceDir, "deep", 3, 3, 5)
		env.ForkWithSource()

		target := filepath.Join(env.TargetPath, "deep")
		dur := timedRemoveAll(t, target)
		reportPerf(t, "rm-rf overlay deep 3-level tree", totalFiles, dur)

		if env.TargetFileExists("deep") {
			t.Error("directory should not exist after RemoveAll")
		}
	})

	t.Run("RmRf/Overlay_WideDir_200Files", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "rmrf_sf_wide200")
		defer env.Cleanup()

		for i := 0; i < 200; i++ {
			env.CreateSourceFile(fmt.Sprintf("wide/%04d.txt", i), "x")
		}
		env.ForkWithSource()

		target := filepath.Join(env.TargetPath, "wide")
		dur := timedRemoveAll(t, target)
		reportPerf(t, "rm-rf overlay wide 200 files", 200, dur)

		if env.TargetFileExists("wide") {
			t.Error("directory should not exist after RemoveAll")
		}
	})

	// --- Round-trip (cp-r then rm-rf) ---

	t.Run("RoundTrip/CopyThenDelete_200Files", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "rt_200")
		defer env.Cleanup()
		env.InitDataFile()

		srcDir := filepath.Join(env.TestDir, "src_rt")
		totalFiles := createLocalTree(t, srcDir, 200, 0, 512)

		dst := filepath.Join(env.Mount, "roundtrip")

		cpDur := timedCpR(t, srcDir, dst)
		reportPerf(t, "round-trip cp-r 200 files (512B)", totalFiles, cpDur)

		verifyTreeCopied(t, dst, totalFiles)

		rmDur := timedRemoveAll(t, dst)
		reportPerf(t, "round-trip rm-rf 200 files", totalFiles, rmDur)

		if dirExists(dst) {
			t.Error("directory should not exist after RemoveAll")
		}

		t.Logf("=== Round-trip total: cp-r %v + rm-rf %v = %v", cpDur, rmDur, cpDur+rmDur)
	})

	t.Run("RoundTrip/Overlay_CopyThenDelete_200Files", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "rt_sf_200")
		defer env.Cleanup()

		// Pre-populate source with files, then fork
		for i := 0; i < 200; i++ {
			env.CreateSourceFile(fmt.Sprintf("data/%04d.txt", i), fmt.Sprintf("content-%d", i))
		}
		env.ForkWithSource()

		// Write new files into the fork overlay
		srcDir := filepath.Join(env.TestDir, "src_rt_sf")
		totalFiles := createLocalTree(t, srcDir, 200, 0, 512)

		dst := filepath.Join(env.TargetPath, "new_data")
		cpDur := timedCpR(t, srcDir, dst)
		reportPerf(t, "overlay round-trip cp-r 200 files", totalFiles, cpDur)

		// Now rm -rf source-only directory (tombstone path)
		srcTarget := filepath.Join(env.TargetPath, "data")
		rmSrcDur := timedRemoveAll(t, srcTarget)
		reportPerf(t, "overlay round-trip rm-rf 200 source-only files", 200, rmSrcDur)

		// Then rm -rf overlay directory
		rmOvlDur := timedRemoveAll(t, dst)
		reportPerf(t, "overlay round-trip rm-rf 200 new files", totalFiles, rmOvlDur)

		t.Logf("=== Overlay round-trip: cp-r %v + rm-rf(source) %v + rm-rf(overlay) %v = %v",
			cpDur, rmSrcDur, rmOvlDur, cpDur+rmSrcDur+rmOvlDur)
	})

	// --- Regression guard (fails if operations exceed budget) ---

	t.Run("RegressionGuard/Empty_100Files_MaxBudget", func(t *testing.T) {
		env := shared.CreateSubtestEnv(t, "guard_ovl100")
		defer env.Cleanup()
		env.InitDataFile()

		for i := 0; i < 100; i++ {
			env.WriteFile(fmt.Sprintf("batch/%04d.txt", i), "x")
		}

		target := filepath.Join(env.Mount, "batch")
		dur := timedRemoveAll(t, target)
		reportPerf(t, "guard: rm-rf empty 100 files", 100, dur)

		// Budget: 100 files × 50ms/file = 5s max
		maxDur := 5 * time.Second
		if dur > maxDur {
			t.Errorf("rm-rf empty 100 files took %v, exceeds budget %v (%.1f ms/file)",
				dur, maxDur, dur.Seconds()*1000/100)
		}
	})

	t.Run("RegressionGuard/Overlay_100Files_MaxBudget", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "guard_sf100")
		defer env.Cleanup()

		for i := 0; i < 100; i++ {
			env.CreateSourceFile(fmt.Sprintf("batch/%04d.txt", i), "x")
		}
		env.ForkWithSource()

		target := filepath.Join(env.TargetPath, "batch")
		dur := timedRemoveAll(t, target)
		reportPerf(t, "guard: rm-rf overlay 100 files", 100, dur)

		// Budget: 100 files × 75ms/file = 7.5s max (source-only is costlier)
		maxDur := 8 * time.Second
		if dur > maxDur {
			t.Errorf("rm-rf overlay 100 files took %v, exceeds budget %v (%.1f ms/file)",
				dur, maxDur, dur.Seconds()*1000/100)
		}
	})

	t.Run("RegressionGuard/Overlay_DeepTree_MaxBudget", func(t *testing.T) {
		env := shared.CreateForkSubtestEnv(t, "guard_sf_deep")
		defer env.Cleanup()

		// 3 levels, 3 branches, 5 files per dir = 65 files, 13 dirs
		totalFiles := createDeepTree(t, env.SourceDir, "deep", 3, 3, 5)
		env.ForkWithSource()

		target := filepath.Join(env.TargetPath, "deep")
		dur := timedRemoveAll(t, target)
		reportPerf(t, "guard: rm-rf overlay deep 3-level", totalFiles, dur)

		// Budget: 65 files × 75ms + 13 dirs × 50ms ≈ 5.5s, round up to 8s
		maxDur := 8 * time.Second
		if dur > maxDur {
			t.Errorf("rm-rf overlay deep tree (%d files) took %v, exceeds budget %v (%.1f ms/file)",
				totalFiles, dur, maxDur, dur.Seconds()*1000/float64(totalFiles))
		}
	})
}

// --- Helpers ---

// timedRemoveAll runs os.RemoveAll and returns the wall-clock duration.
func timedRemoveAll(t *testing.T, path string) time.Duration {
	t.Helper()
	start := time.Now()
	if err := os.RemoveAll(path); err != nil {
		t.Fatalf("RemoveAll(%q) failed: %v", path, err)
	}
	return time.Since(start)
}

// timedCpR recursively copies src to dst using Go os operations and returns the
// wall-clock duration. Each file write goes through the NFS mount (LOOKUP, CREATE,
// WRITE RPCs), matching real-world NFS I/O patterns without macOS xattr issues.
func timedCpR(t *testing.T, src, dst string) time.Duration {
	t.Helper()
	start := time.Now()
	err := copyDirRecursive(src, dst)
	dur := time.Since(start)
	if err != nil {
		t.Fatalf("copyDirRecursive(%q, %q) failed: %v", src, dst, err)
	}
	return dur
}

// copyDirRecursive copies a directory tree from src to dst using os operations.
func copyDirRecursive(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)

		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode())
	})
}

// reportPerf logs a standardized performance line.
func reportPerf(t *testing.T, label string, fileCount int, dur time.Duration) {
	t.Helper()
	fps := float64(fileCount) / dur.Seconds()
	msPerFile := dur.Seconds() * 1000 / float64(fileCount)
	t.Logf("PERF %-50s %5d files  %8.1f ms  %8.1f files/s  %6.2f ms/file",
		label, fileCount, float64(dur.Milliseconds()), fps, msPerFile)
}

// createDeepTree creates a directory tree with the given depth, branching factor,
// and files per directory. Returns total file count.
//
//	depth=3, branches=2, filesPerDir=3 creates:
//	  root/
//	    f0.txt, f1.txt, f2.txt
//	    d0/
//	      f0.txt, f1.txt, f2.txt
//	      d0/ (leaf)
//	        f0.txt, f1.txt, f2.txt
//	      d1/ (leaf)
//	        f0.txt, f1.txt, f2.txt
//	    d1/
//	      ... same structure
func createDeepTree(t *testing.T, base, subdir string, depth, branches, filesPerDir int) int {
	t.Helper()
	dir := filepath.Join(base, subdir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("MkdirAll(%q) failed: %v", dir, err)
	}

	total := 0

	// Create files at this level
	for i := 0; i < filesPerDir; i++ {
		fpath := filepath.Join(dir, fmt.Sprintf("f%d.txt", i))
		if err := os.WriteFile(fpath, []byte(fmt.Sprintf("depth=%d file=%d", depth, i)), 0644); err != nil {
			t.Fatalf("WriteFile(%q) failed: %v", fpath, err)
		}
		total++
	}

	// Recurse into subdirectories
	if depth > 1 {
		for b := 0; b < branches; b++ {
			child := filepath.Join(subdir, fmt.Sprintf("d%d", b))
			total += createDeepTree(t, base, child, depth-1, branches, filesPerDir)
		}
	}

	return total
}

// createLocalTree creates a flat directory with numFiles files of fileSize bytes,
// plus numDirs empty subdirectories. Returns total file count.
func createLocalTree(t *testing.T, dir string, numFiles, numDirs, fileSize int) int {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("MkdirAll(%q) failed: %v", dir, err)
	}

	content := make([]byte, fileSize)
	for i := range content {
		content[i] = byte(i % 251) // use prime to avoid patterns
	}

	for i := 0; i < numFiles; i++ {
		fpath := filepath.Join(dir, fmt.Sprintf("%04d.txt", i))
		if err := os.WriteFile(fpath, content, 0644); err != nil {
			t.Fatalf("WriteFile(%q) failed: %v", fpath, err)
		}
	}

	for i := 0; i < numDirs; i++ {
		dpath := filepath.Join(dir, fmt.Sprintf("dir_%04d", i))
		if err := os.MkdirAll(dpath, 0755); err != nil {
			t.Fatalf("MkdirAll(%q) failed: %v", dpath, err)
		}
	}

	return numFiles
}

// verifyTreeCopied checks that at least expectedFiles files exist under dir.
func verifyTreeCopied(t *testing.T, dir string, expectedFiles int) {
	t.Helper()
	count := 0
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Walk(%q) failed: %v", dir, err)
	}
	// Allow for ._* Apple Double files created by macOS NFS client
	plainCount := 0
	for _, f := range listAllFiles(t, dir) {
		if !strings.HasPrefix(filepath.Base(f), "._") {
			plainCount++
		}
	}
	if plainCount < expectedFiles {
		t.Errorf("expected at least %d files, got %d (total including ._ files: %d)",
			expectedFiles, plainCount, count)
	}
}

// listAllFiles returns all file paths under dir.
func listAllFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Walk(%q) failed: %v", dir, err)
	}
	return files
}

// dirExists checks if a path exists and is a directory.
func dirExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}
