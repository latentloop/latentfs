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

//go:build darwin

package daemon

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// Mount mounts an SMB share using mount_smbfs
func Mount(port int, shareName, mountPoint string) error {
	// Create mount point if it doesn't exist
	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		return fmt.Errorf("failed to create mount point: %w", err)
	}

	// Build SMB URL
	// Format: //Guest@127.0.0.1:port/shareName
	url := fmt.Sprintf("//Guest@127.0.0.1:%d/%s", port, shareName)

	log.Printf("Mount: running mount_smbfs %s -> %s", url, mountPoint)

	// Run mount_smbfs
	// Options:
	//   -N: Don't prompt for password (guest auth)
	//   -o nobrowse: Don't show on Desktop
	//   -o nostreams: Disable named streams (simplifies implementation)
	cmd := exec.Command("mount_smbfs", "-N", "-o", "nobrowse,nostreams", url, mountPoint)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Mount: mount_smbfs failed: %v, output: %s", err, string(output))
		return fmt.Errorf("mount_smbfs failed: %w, output: %s", err, string(output))
	}

	log.Printf("Mount: mount_smbfs succeeded, output: %s", string(output))

	// Verify mount actually worked
	mountOutput, _ := exec.Command("mount").Output()
	if len(mountOutput) > 0 {
		log.Printf("Mount: current mounts include our target: %v",
			exec.Command("mount").Run() == nil)
	}

	return nil
}

// unmountTimeout is the maximum time to wait for each unmount attempt.
// After the NFS server is shut down, the kernel NFS client may block unmount
// commands while it waits for the server to respond (up to soft timeout).
// 3s is enough for normal unmounts; force unmount always succeeds quickly.
const unmountTimeout = 3 * time.Second

// Unmount unmounts a filesystem
func Unmount(mountPoint string) error {
	log.Printf("Unmount: attempting to unmount %s", mountPoint)

	// Check if actually mounted first
	if !IsMounted(mountPoint) {
		log.Printf("Unmount: %s is not mounted, nothing to do", mountPoint)
		return nil
	}

	// Try diskutil unmount first (macOS preferred method)
	ctx, cancel := context.WithTimeout(context.Background(), unmountTimeout)
	cmd := exec.CommandContext(ctx, "diskutil", "unmount", mountPoint)
	output, err := cmd.CombinedOutput()
	cancel()
	if err == nil {
		log.Printf("Unmount: diskutil unmount succeeded for %s", mountPoint)
		return nil
	}
	log.Printf("Unmount: diskutil unmount failed: %v, output: %s", err, string(output))

	// Fall back to umount
	ctx, cancel = context.WithTimeout(context.Background(), unmountTimeout)
	cmd = exec.CommandContext(ctx, "umount", mountPoint)
	output, err = cmd.CombinedOutput()
	cancel()
	if err == nil {
		log.Printf("Unmount: umount succeeded for %s", mountPoint)
		return nil
	}
	log.Printf("Unmount: umount failed: %v, output: %s", err, string(output))

	// Try force unmount as last resort
	log.Printf("Unmount: trying force unmount for %s", mountPoint)
	ctx, cancel = context.WithTimeout(context.Background(), unmountTimeout)
	cmd = exec.CommandContext(ctx, "umount", "-f", mountPoint)
	output, err = cmd.CombinedOutput()
	cancel()
	if err != nil {
		log.Printf("Unmount: force unmount failed: %v, output: %s", err, string(output))
		return fmt.Errorf("all unmount attempts failed for %s: %w", mountPoint, err)
	}
	log.Printf("Unmount: force unmount succeeded for %s", mountPoint)
	return nil
}

// IsMounted checks if a path is a mount point by checking the mount table
func IsMounted(mountPoint string) bool {
	// Check the actual mount table instead of just stat
	cmd := exec.Command("mount")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	// Resolve symlinks in the mount point path.
	// On macOS, /tmp -> /private/tmp and /var -> /private/var, so paths like
	// /tmp/foo appear as /private/tmp/foo in the mount table.
	realPath, err := filepath.EvalSymlinks(mountPoint)
	if err != nil {
		// Path doesn't exist yet or other error - use original path
		realPath = mountPoint
	}

	// Check if the mount point appears in the mount output
	// NFS mounts appear as "localhost:/ on /path/to/mount"
	return len(output) > 0 && (containsMount(string(output), realPath))
}

// containsMount checks if a mount point is in the mount output
func containsMount(mountOutput, mountPoint string) bool {
	// Look for the mount point in the output
	// Format is typically: "something on /mount/point (type options)"
	lines := []byte(mountOutput)
	for _, line := range bytes.Split(lines, []byte("\n")) {
		if bytes.Contains(line, []byte(" on "+mountPoint+" ")) ||
			bytes.Contains(line, []byte(" on "+mountPoint+"\n")) {
			return true
		}
	}
	return false
}
