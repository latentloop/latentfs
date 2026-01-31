#!/bin/bash
# Test save/restore full snapshot on a full-forked NFS mount.
#
# Usage: bash tests/scripts/test_save_restore_full_0.sh <source-folder>
#
# Debugging:
#   - Set LOG_LEVEL=trace to get per-operation timing in daemon logs
#   - Set LATENTFS_PRESERVE_DEBUG=1 to skip cleanup and preserve data files
#   - Daemon logs are copied to tests/log/save_restore_full_trace.log
#   - Use LATENTFS_DAEMON_LOG to customize daemon log location (default: ~/.latentfs/daemon.log)
#
# Steps:
#   1. Build the latentfs binary
#   2. Stop then start the daemon with logging (trace level)
#   3. Full-fork the source folder to a temp mount path
#   4. Save a full snapshot
#   5. Find and remove a directory from the mount
#   6. Create new files (should be removed after restore)
#   7. Restore from the snapshot
#   8. Verify the removed directory is back
#   9. Verify created files are removed
#  10. Copy daemon log to tests/log/

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

cleanup() {
    echo ""
    echo "=== Cleanup ==="
    if $FORK_SUCCEEDED; then
        echo "  Unmounting $TARGET_PATH..."
        "$BIN" unmount "$TARGET_PATH" 2>/dev/null || true
        echo "  Removing data file $DATA_FILE..."
        rm -f "$DATA_FILE" 2>/dev/null || true
        echo "  Stopping daemon..."
        "$BIN" daemon stop 2>/dev/null || true
    fi

    # Copy daemon log to tests/log/
    mkdir -p "$LOG_DIR"
    if [ -f "$DAEMON_LOG" ]; then
        cp "$DAEMON_LOG" "$LOG_DIR/save_restore_full_trace.log"
        echo "  Daemon log -> $LOG_DIR/save_restore_full_trace.log"
    fi

    echo "  Removing temp dir $TMP_DIR..."
    rm -rf "$TMP_DIR" 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT

# --- 1. Build ---
echo "=== Step 1: Building latentfs ==="
cd "$PROJECT_DIR"
make build-dev
echo "Binary: $BIN"
echo ""

# --- 2. Restart daemon with logging ---
echo "=== Step 2: Restarting daemon with logging (level=$LOG_LEVEL) ==="
"$BIN" daemon stop 2>/dev/null || true
sleep 0.5
# Configure logging level and truncate old log
"$BIN" daemon config --logging "$LOG_LEVEL"
: > "$DAEMON_LOG" 2>/dev/null || true
"$BIN" daemon start
echo ""

# --- 3. Full fork ---
echo "=== Step 3: Full-forking source folder ==="
echo "  Source: $SOURCE_FOLDER"
echo "  Target: $TARGET_PATH"
"$BIN" fork "$SOURCE_FOLDER" -m "$TARGET_PATH" --type=full
FORK_SUCCEEDED=true

# Wait for mount to be ready
echo -n "  Waiting for mount..."
for i in $(seq 1 30); do
    if ls "$TARGET_PATH" >/dev/null 2>&1; then
        echo " ready"
        break
    fi
    sleep 0.2
done
echo ""

# --- 4. Save full snapshot ---
echo "=== Step 4: Saving full snapshot ==="
SNAPSHOT_TAG="test-full-$(date +%s)"
"$BIN" data save -d "$DATA_FILE" -m "Test full snapshot" -t "$SNAPSHOT_TAG"
echo ""

# --- 5. Find and remove a directory ---
echo "=== Step 5: Finding a directory to remove ==="
# Find first directory with files and total size < 20MB
SIZE_THRESHOLD_MB=20
SIZE_THRESHOLD_BYTES=$((SIZE_THRESHOLD_MB * 1024 * 1024))
target_dir=""
for entry in "$TARGET_PATH"/*/; do
    [ -d "$entry" ] || continue
    name=$(basename "$entry")
    # Skip hidden directories
    [[ "$name" == .* ]] && continue
    count=$(find "$entry" -type f 2>/dev/null | wc -l | tr -d ' ')
    if [ "$count" -gt 0 ]; then
        # Calculate total size in bytes
        total_size=$(find "$entry" -type f -exec stat -f%z {} + 2>/dev/null | awk '{sum+=$1} END {print sum+0}')
        total_size_mb=$(echo "scale=2; $total_size / 1024 / 1024" | bc)
        echo "  $name: $count files, ${total_size_mb}MB"
        if [ "$total_size" -lt "$SIZE_THRESHOLD_BYTES" ]; then
            target_dir="$entry"
            target_name="$name"
            target_count=$count
            target_size=$total_size
            target_size_mb=$total_size_mb
            break
        fi
    fi
done

if [ -z "$target_dir" ]; then
    echo "Error: no subdirectories with files under ${SIZE_THRESHOLD_MB}MB found in mount"
    exit 1
fi

echo "  Selected: $target_name ($target_count files, ${target_size_mb}MB)"
echo "  Path: $target_dir"
echo ""

echo "=== Step 6: Removing directory ==="
echo "  rm -rf $target_dir"
rm -rf "$target_dir"

# Verify removal
if [ -d "$target_dir" ]; then
    echo "ERROR: Directory still exists after rm -rf!"
    exit 1
fi
echo "  Verified: directory removed"
echo ""

# --- 7. Create new files (should be removed after restore) ---
echo "=== Step 7: Creating new files (should be removed after restore) ==="
NEW_FILES_DIR="$TARGET_PATH/_test_new_files_$$"
mkdir -p "$NEW_FILES_DIR"
echo "file1 content" > "$NEW_FILES_DIR/file1.txt"
echo "file2 content" > "$NEW_FILES_DIR/file2.txt"
echo "file3 content" > "$NEW_FILES_DIR/file3.txt"
NEW_FILE_ROOT="$TARGET_PATH/_test_root_file_$$.txt"
echo "root file content" > "$NEW_FILE_ROOT"

# Verify creation
if [ ! -d "$NEW_FILES_DIR" ] || [ ! -f "$NEW_FILE_ROOT" ]; then
    echo "ERROR: Failed to create new files!"
    exit 1
fi
new_files_count=$(find "$NEW_FILES_DIR" -type f 2>/dev/null | wc -l | tr -d ' ')
echo "  Created directory: $NEW_FILES_DIR ($new_files_count files)"
echo "  Created file: $NEW_FILE_ROOT"
echo ""

# --- 8. Restore from snapshot ---
echo "=== Step 8: Restoring from snapshot ==="
echo "  Restoring tag: $SNAPSHOT_TAG"
"$BIN" data restore "$SNAPSHOT_TAG" -d "$DATA_FILE" -y
echo ""

# --- 9. Verify restoration ---
echo "=== Step 9: Verifying restoration ==="

# 9a. Verify removed directory is back
echo "  Checking deleted directory was restored..."
if [ ! -d "$target_dir" ]; then
    echo "FAILED: Directory was NOT restored!"
    echo "  Expected: $target_dir"
    exit 1
fi

restored_count=$(find "$target_dir" -type f 2>/dev/null | wc -l | tr -d ' ')
echo "    Directory exists: $target_name"
echo "    Files restored: $restored_count (expected: $target_count)"

if [ "$restored_count" -ne "$target_count" ]; then
    echo ""
    echo "FAILED: File count mismatch!"
    echo "  Expected: $target_count"
    echo "  Got: $restored_count"
    exit 1
fi

# 9b. Verify created files are removed
echo ""
echo "  Checking created files were removed..."
if [ -d "$NEW_FILES_DIR" ]; then
    echo "FAILED: Created directory should be removed but still exists!"
    echo "  Path: $NEW_FILES_DIR"
    ls -la "$NEW_FILES_DIR" 2>/dev/null || true
    exit 1
fi
echo "    Directory removed: $(basename "$NEW_FILES_DIR")"

if [ -f "$NEW_FILE_ROOT" ]; then
    echo "FAILED: Created file should be removed but still exists!"
    echo "  Path: $NEW_FILE_ROOT"
    exit 1
fi
echo "    File removed: $(basename "$NEW_FILE_ROOT")"

echo ""
echo "=== PASSED ==="
echo "  Full save/restore test completed successfully."
echo "  - Created full fork from: $SOURCE_FOLDER"
echo "  - Saved full snapshot: $SNAPSHOT_TAG"
echo "  - Removed directory: $target_name ($target_count files)"
echo "  - Created new files: $NEW_FILES_DIR ($new_files_count files) + $NEW_FILE_ROOT"
echo "  - Restored from snapshot"
echo "  - Verified: deleted directory restored ($restored_count files)"
echo "  - Verified: created files removed"
