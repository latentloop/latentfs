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

# Test multiple overlay snapshots with restore to different points.
#
# Usage: bash tests/scripts/test_save_restore_overlay_1.sh <source-folder>
#
# Debugging:
#   - Set LOG_LEVEL=trace to get per-operation timing in daemon logs
#   - Set LATENTFS_PRESERVE_DEBUG=1 to skip cleanup and preserve data files
#   - Daemon logs are copied to tests/log/save_restore_overlay_1_trace.log
#   - Use LATENTFS_DAEMON_LOG to customize daemon log location (default: ~/.latentfs/daemon.log)
#
# Test Flow:
#   1. Soft-fork source folder
#   2. Save snapshot 1 (initial state)
#   3. Remove dir_a, create dir_x (with nested files)
#   4. Save snapshot 2
#   5. Remove dir_b, create dir_y (with nested files)
#   6. Save snapshot 3
#   7. Restore to snapshot 1 → verify: dir_a exists, dir_b exists, dir_x gone, dir_y gone
#   8. Restore to snapshot 3 → verify: dir_a gone, dir_b gone, dir_x exists, dir_y exists
#   9. Restore to snapshot 2 → verify: dir_a gone, dir_b exists, dir_x exists, dir_y gone

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <source-folder>"
    exit 1
fi

SOURCE_FOLDER="$(cd "$1" && pwd)"
if [ ! -d "$SOURCE_FOLDER" ]; then
    echo "Error: $SOURCE_FOLDER is not a directory"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$PROJECT_DIR/bin/latentfs"
LOG_DIR="$PROJECT_DIR/tests/log"
TMP_DIR=$(mktemp -d -t latentfs_restore_test)
TARGET_PATH="$TMP_DIR/mount"

# Log level for daemon (trace enables per-operation timing)
LOG_LEVEL="${LOG_LEVEL:-trace}"

FORK_SUCCEEDED=false
DATA_FILE="$TARGET_PATH.latentfs"
DAEMON_LOG="${LATENTFS_DAEMON_LOG:-$HOME/.latentfs/daemon.log}"

# Track what we create/delete for verification
DIR_A=""
DIR_A_COUNT=0
DIR_B=""
DIR_B_COUNT=0
DIR_X=""
DIR_Y=""

cleanup() {
    echo ""
    echo "=== Cleanup ==="
    if $FORK_SUCCEEDED; then
        echo "  Unmounting $TARGET_PATH..."
        "$BIN" unmount "$TARGET_PATH" 2>/dev/null || true
        echo "  Removing data file $DATA_FILE..."
        rm -f "$DATA_FILE" 2>/dev/null || true
    fi
    echo "  Stopping daemon..."
    "$BIN" daemon stop 2>/dev/null || true
    # Copy daemon log for analysis
    mkdir -p "$LOG_DIR"
    if [ -f "$DAEMON_LOG" ]; then
        echo "  Daemon log -> $LOG_DIR/save_restore_overlay_1_trace.log"
        cp "$DAEMON_LOG" "$LOG_DIR/save_restore_overlay_1_trace.log" 2>/dev/null || true
    fi
    if [ -z "${LATENTFS_PRESERVE_DEBUG:-}" ]; then
        echo "  Removing temp dir $TMP_DIR..."
        rm -rf "$TMP_DIR" 2>/dev/null || true
    else
        echo "  LATENTFS_PRESERVE_DEBUG set, preserving: $TMP_DIR"
    fi
    echo "Done."
}
trap cleanup EXIT

# --- Helper functions ---
find_two_dirs() {
    # Find two directories with files, sorted by file count
    local mount_path="$1"
    local result=""
    for dir in "$mount_path"/*/; do
        if [ -d "$dir" ]; then
            local name=$(basename "$dir")
            # Skip hidden dirs and our test dirs
            if [[ "$name" != .* ]] && [[ "$name" != _test_* ]]; then
                local count=$(find "$dir" -type f 2>/dev/null | wc -l | tr -d ' ')
                if [ "$count" -gt 0 ]; then
                    result="$result$count $name\n"
                fi
            fi
        fi
    done
    echo -e "$result" | sort -rn | head -2
}

create_test_dir() {
    local base_path="$1"
    local dir_name="$2"
    local dir_path="$base_path/$dir_name"

    mkdir -p "$dir_path/subdir1"
    mkdir -p "$dir_path/subdir2/nested"
    echo "file1" > "$dir_path/file1.txt"
    echo "file2" > "$dir_path/file2.txt"
    echo "sub1" > "$dir_path/subdir1/sub1.txt"
    echo "sub2" > "$dir_path/subdir2/sub2.txt"
    echo "nested" > "$dir_path/subdir2/nested/nested.txt"

    echo "$dir_path"
}

verify_dir_exists() {
    local dir_path="$1"
    local dir_name="$2"
    local expected_count="$3"

    if [ ! -d "$dir_path" ]; then
        echo "FAILED: $dir_name should exist but doesn't!"
        return 1
    fi
    local actual_count=$(find "$dir_path" -type f 2>/dev/null | wc -l | tr -d ' ')
    if [ "$actual_count" -ne "$expected_count" ]; then
        echo "FAILED: $dir_name has $actual_count files, expected $expected_count"
        return 1
    fi
    echo "    OK: $dir_name exists ($actual_count files)"
    return 0
}

verify_dir_gone() {
    local dir_path="$1"
    local dir_name="$2"

    if [ -d "$dir_path" ]; then
        echo "FAILED: $dir_name should NOT exist but does!"
        ls -la "$dir_path" 2>/dev/null || true
        return 1
    fi
    echo "    OK: $dir_name removed"
    return 0
}

# --- 1. Build ---
echo "=== Step 1: Building latentfs ==="
cd "$PROJECT_DIR"
make build-dev
echo "Binary: $BIN"
echo ""

# --- 2. Restart daemon ---
echo "=== Step 2: Restarting daemon with logging (level=$LOG_LEVEL) ==="
"$BIN" daemon stop 2>/dev/null || echo "Daemon not running"
sleep 0.5
# Configure logging level and truncate old log
"$BIN" daemon config --logging "$LOG_LEVEL"
: > "$DAEMON_LOG" 2>/dev/null || true
"$BIN" daemon start
echo ""

# --- 3. Soft-fork ---
echo "=== Step 3: Soft-forking source folder ==="
echo "  Source: $SOURCE_FOLDER"
echo "  Target: $TARGET_PATH"
"$BIN" fork "$SOURCE_FOLDER" -m "$TARGET_PATH"
FORK_SUCCEEDED=true

# Wait for mount to be ready
echo -n "  Waiting for mount..."
for i in {1..10}; do
    if ls "$TARGET_PATH" >/dev/null 2>&1; then
        echo " ready"
        break
    fi
    sleep 0.5
done
echo ""

# --- 4. Find two directories to delete ---
echo "=== Step 4: Finding two directories to remove ==="
dirs_info=$(find_two_dirs "$TARGET_PATH")
if [ -z "$dirs_info" ]; then
    echo "ERROR: No suitable directories found in $TARGET_PATH"
    exit 1
fi

# Parse first two directories
DIR_A_NAME=$(echo "$dirs_info" | head -1 | awk '{print $2}')
DIR_A_COUNT=$(echo "$dirs_info" | head -1 | awk '{print $1}')
DIR_A="$TARGET_PATH/$DIR_A_NAME"

DIR_B_NAME=$(echo "$dirs_info" | tail -1 | awk '{print $2}')
DIR_B_COUNT=$(echo "$dirs_info" | tail -1 | awk '{print $1}')
DIR_B="$TARGET_PATH/$DIR_B_NAME"

if [ -z "$DIR_A_NAME" ] || [ -z "$DIR_B_NAME" ]; then
    echo "ERROR: Need at least 2 directories with files"
    exit 1
fi

echo "  Dir A: $DIR_A_NAME ($DIR_A_COUNT files)"
echo "  Dir B: $DIR_B_NAME ($DIR_B_COUNT files)"
echo ""

# --- 5. Save snapshot 1 (initial state) ---
echo "=== Step 5: Saving snapshot 1 (initial state) ==="
SNAPSHOT_TAG_1="test-multi-snap1-$$"
"$BIN" data save -d "$DATA_FILE" -t "$SNAPSHOT_TAG_1" -m "Snapshot 1: initial"
echo "  Tag: $SNAPSHOT_TAG_1"
echo ""

# --- 6. Make changes for snapshot 2 ---
echo "=== Step 6: Changes for snapshot 2 ==="
echo "  Removing: $DIR_A_NAME"
rm -rf "$DIR_A"
if [ -d "$DIR_A" ]; then
    echo "ERROR: Failed to remove $DIR_A_NAME"
    exit 1
fi
echo "    Removed: $DIR_A_NAME"

echo "  Creating: _test_dir_x_$$"
DIR_X=$(create_test_dir "$TARGET_PATH" "_test_dir_x_$$")
DIR_X_COUNT=$(find "$DIR_X" -type f 2>/dev/null | wc -l | tr -d ' ')
echo "    Created: $(basename "$DIR_X") ($DIR_X_COUNT files)"
echo ""

# --- 7. Save snapshot 2 ---
echo "=== Step 7: Saving snapshot 2 ==="
SNAPSHOT_TAG_2="test-multi-snap2-$$"
"$BIN" data save -d "$DATA_FILE" -t "$SNAPSHOT_TAG_2" -m "Snapshot 2: removed A, created X"
echo "  Tag: $SNAPSHOT_TAG_2"
echo ""

# --- 8. Make changes for snapshot 3 ---
echo "=== Step 8: Changes for snapshot 3 ==="
echo "  Removing: $DIR_B_NAME"
rm -rf "$DIR_B"
if [ -d "$DIR_B" ]; then
    echo "ERROR: Failed to remove $DIR_B_NAME"
    exit 1
fi
echo "    Removed: $DIR_B_NAME"

echo "  Creating: _test_dir_y_$$"
DIR_Y=$(create_test_dir "$TARGET_PATH" "_test_dir_y_$$")
DIR_Y_COUNT=$(find "$DIR_Y" -type f 2>/dev/null | wc -l | tr -d ' ')
echo "    Created: $(basename "$DIR_Y") ($DIR_Y_COUNT files)"
echo ""

# --- 9. Save snapshot 3 ---
echo "=== Step 9: Saving snapshot 3 ==="
SNAPSHOT_TAG_3="test-multi-snap3-$$"
"$BIN" data save -d "$DATA_FILE" -t "$SNAPSHOT_TAG_3" -m "Snapshot 3: removed B, created Y"
echo "  Tag: $SNAPSHOT_TAG_3"
echo ""

# --- 10. Restore to snapshot 1 ---
echo "=== Step 10: Restore to snapshot 1 (initial state) ==="
echo "  Expected: A exists, B exists, X gone, Y gone"
"$BIN" data restore "$SNAPSHOT_TAG_1" -d "$DATA_FILE" -y
# Force NFS client to refresh cache by listing root directory
# Force NFS client cache refresh: list root dir, stat it, then sleep
ls "$TARGET_PATH" > /dev/null 2>&1 || true
stat "$TARGET_PATH" > /dev/null 2>&1 || true
sleep 0.5
echo ""

echo "  Verifying..."
FAILED=false
verify_dir_exists "$DIR_A" "$DIR_A_NAME" "$DIR_A_COUNT" || FAILED=true
verify_dir_exists "$DIR_B" "$DIR_B_NAME" "$DIR_B_COUNT" || FAILED=true
verify_dir_gone "$DIR_X" "$(basename "$DIR_X")" || FAILED=true
verify_dir_gone "$DIR_Y" "$(basename "$DIR_Y")" || FAILED=true

if $FAILED; then
    echo ""
    echo "FAILED: Restore to snapshot 1 verification failed!"
    exit 1
fi
echo "  Snapshot 1 restore: PASSED"
echo ""

# --- 11. Restore to snapshot 3 ---
echo "=== Step 11: Restore to snapshot 3 (final state) ==="
echo "  Expected: A gone, B gone, X exists, Y exists"
"$BIN" data restore "$SNAPSHOT_TAG_3" -d "$DATA_FILE" -y
# Force NFS client to refresh cache by listing root directory
# Force NFS client cache refresh: list root dir, stat it, then sleep
ls "$TARGET_PATH" > /dev/null 2>&1 || true
stat "$TARGET_PATH" > /dev/null 2>&1 || true
sleep 0.5
echo ""

echo "  Verifying..."
FAILED=false
verify_dir_gone "$DIR_A" "$DIR_A_NAME" || FAILED=true
verify_dir_gone "$DIR_B" "$DIR_B_NAME" || FAILED=true
verify_dir_exists "$DIR_X" "$(basename "$DIR_X")" "$DIR_X_COUNT" || FAILED=true
verify_dir_exists "$DIR_Y" "$(basename "$DIR_Y")" "$DIR_Y_COUNT" || FAILED=true

if $FAILED; then
    echo ""
    echo "FAILED: Restore to snapshot 3 verification failed!"
    exit 1
fi
echo "  Snapshot 3 restore: PASSED"
echo ""

# --- 12. Restore to snapshot 2 ---
echo "=== Step 12: Restore to snapshot 2 (middle state) ==="
echo "  Expected: A gone, B exists, X exists, Y gone"
"$BIN" data restore "$SNAPSHOT_TAG_2" -d "$DATA_FILE" -y
# Force NFS client to refresh cache by listing root directory
# Force NFS client cache refresh: list root dir, stat it, then sleep
ls "$TARGET_PATH" > /dev/null 2>&1 || true
stat "$TARGET_PATH" > /dev/null 2>&1 || true
sleep 0.5
echo ""

echo "  Verifying..."
FAILED=false
verify_dir_gone "$DIR_A" "$DIR_A_NAME" || FAILED=true
verify_dir_exists "$DIR_B" "$DIR_B_NAME" "$DIR_B_COUNT" || FAILED=true
verify_dir_exists "$DIR_X" "$(basename "$DIR_X")" "$DIR_X_COUNT" || FAILED=true
verify_dir_gone "$DIR_Y" "$(basename "$DIR_Y")" || FAILED=true

if $FAILED; then
    echo ""
    echo "FAILED: Restore to snapshot 2 verification failed!"
    exit 1
fi
echo "  Snapshot 2 restore: PASSED"
echo ""

# --- Summary ---
echo "=== ALL TESTS PASSED ==="
echo "  Source folder: $SOURCE_FOLDER"
echo "  Snapshots created:"
echo "    - $SNAPSHOT_TAG_1 (initial: A=$DIR_A_NAME, B=$DIR_B_NAME)"
echo "    - $SNAPSHOT_TAG_2 (A removed, X created)"
echo "    - $SNAPSHOT_TAG_3 (B removed, Y created)"
echo "  Restores verified:"
echo "    - Snapshot 1: A+B exist, X+Y gone"
echo "    - Snapshot 3: A+B gone, X+Y exist"
echo "    - Snapshot 2: A gone, B exists, X exists, Y gone"
