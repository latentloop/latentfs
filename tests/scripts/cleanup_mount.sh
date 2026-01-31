#!/bin/bash
# Copyright 2024 LatentFS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# NFS/SMB Mount Cleanup Script for latentfs
# Cleans up all stale latentfs mounts, including zombie kernel NFS mounts.
# Run with: sudo bash tests/scripts/cleanup_mount.sh
#
# Zombie kernel NFS mounts occur when a daemon dies while macOS kernel_task
# still holds the NFS client state. Regular umount -f fails with
# "No such file or directory" or "Resource busy". Only sudo umount -f
# or a reboot can clear these.

set -euo pipefail

CONFIG_DIR="$HOME/.latentfs"

echo "=== latentfs Mount Cleanup ==="
echo ""

# 1. Force unmount ALL latentfs NFS mounts (MUST happen before killing daemons)
echo "Step 1: Force unmounting all latentfs NFS mounts..."
unmounted=0
failed=0
while IFS= read -r line; do
    [ -z "$line" ] && continue
    # Parse: "localhost:/ on /path/to/mount (nfs, ...)"
    mp=$(echo "$line" | sed 's/.* on \(.*\) (.*/\1/')
    case "$mp" in
        */.latentfs/*) ;;
        *) continue ;;
    esac
    echo "  Unmounting: $mp"
    if umount -f "$mp" 2>/dev/null; then
        echo "    OK (umount -f)"
        unmounted=$((unmounted + 1))
    elif diskutil unmount force "$mp" 2>/dev/null; then
        echo "    OK (diskutil force)"
        unmounted=$((unmounted + 1))
    else
        echo "    FAILED (zombie kernel mount)"
        failed=$((failed + 1))
    fi
done < <(mount -t nfs 2>/dev/null || true)
echo "  Unmounted: $unmounted, Failed: $failed"

# 2. Kill ALL latentfs daemon processes
echo ""
echo "Step 2: Killing all latentfs daemon processes..."
pkill -9 -f "latentfs.*daemon" 2>/dev/null && echo "  Killed latentfs daemons" || echo "  No latentfs daemons running"
pkill -9 -f "latentfs2.*daemon" 2>/dev/null && echo "  Killed latentfs2 daemons" || echo "  No latentfs2 daemons running"

# 3. Brief pause for kernel to release NFS client state
sleep 0.5

# 4. Second-pass unmount (mounts held by now-dead daemons may release)
echo ""
echo "Step 3: Second-pass unmount (post-kill)..."
unmounted2=0
failed2=0
while IFS= read -r line; do
    [ -z "$line" ] && continue
    mp=$(echo "$line" | sed 's/.* on \(.*\) (.*/\1/')
    case "$mp" in
        */.latentfs/*) ;;
        *) continue ;;
    esac
    echo "  Unmounting: $mp"
    if umount -f "$mp" 2>/dev/null; then
        echo "    OK"
        unmounted2=$((unmounted2 + 1))
    else
        echo "    FAILED (stuck in kernel)"
        failed2=$((failed2 + 1))
    fi
done < <(mount -t nfs 2>/dev/null || true)
echo "  Unmounted: $unmounted2, Failed: $failed2"

# 5. Remove stale mount directories
echo ""
echo "Step 4: Removing stale mount directories..."
removed=0
if [ -d "$CONFIG_DIR" ]; then
    for dir in "$CONFIG_DIR"/mnt_*; do
        [ -d "$dir" ] || continue
        # Skip if still mounted
        if mount | grep -q " on $dir "; then
            echo "  Skipping (still mounted): $dir"
            continue
        fi
        rm -rf "$dir" && removed=$((removed + 1))
    done
fi
echo "  Removed: $removed directories"

# 6. Remove stale test artifact files
echo ""
echo "Step 5: Removing stale test artifacts..."
artifacts=0
if [ -d "$CONFIG_DIR" ]; then
    for f in "$CONFIG_DIR"/test_*; do
        [ -e "$f" ] || continue
        rm -f "$f" && artifacts=$((artifacts + 1))
    done
fi
echo "  Removed: $artifacts artifacts"

# 7. Final status
echo ""
echo "=== Final Status ==="
remaining=$(mount -t nfs 2>/dev/null | grep '\.latentfs' | wc -l | tr -d ' ')
remaining_procs=$(pgrep -f "latentfs.*daemon" 2>/dev/null | wc -l | tr -d ' ')

if [ "$remaining" -gt 0 ]; then
    echo "WARNING: $remaining zombie NFS mounts remain (stuck in kernel)"
    echo "These mounts show 'No such file or directory' on umount -f."
    echo "Options:"
    echo "  1. Try: sudo automount -vc"
    echo "  2. Try: sudo killall -9 nfsd && sudo nfsd start"
    echo "  3. Last resort: reboot"
    mount -t nfs | grep '\.latentfs'
else
    echo "All latentfs NFS mounts cleared."
fi

if [ "$remaining_procs" -gt 0 ]; then
    echo "WARNING: $remaining_procs latentfs daemon processes still running"
    pgrep -fl "latentfs.*daemon" || true
else
    echo "No latentfs daemon processes running."
fi

exit "$remaining"
