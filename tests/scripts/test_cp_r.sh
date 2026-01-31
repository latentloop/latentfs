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

# Test cp -r performance on a soft-forked NFS mount.
#
# Usage: bash tests/scripts/test_cp_r.sh <source-folder>
#
# Steps:
#   1. Build the latentfs binary
#   2. Stop then start the daemon with logging (trace level)
#   3. Soft-fork the source folder to a temp mount path
#   4. Find the largest directory in the mount
#   5. Time cp -r of that directory to a new location inside the mount
#   6. Copy daemon log to tests/log/

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
TMP_DIR=$(mktemp -d -t latentfs_cpr_test)
TARGET_PATH="$TMP_DIR/mount"

# Log level for daemon (trace enables per-operation timing)
LOG_LEVEL="${LOG_LEVEL:-trace}"

FORK_SUCCEEDED=false
DATA_FILE="$TARGET_PATH.latentfs"
DAEMON_LOG="$HOME/.latentfs/daemon.log"

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
        cp "$DAEMON_LOG" "$LOG_DIR/cp_r_trace.log"
        echo "  Daemon log → $LOG_DIR/cp_r_trace.log"
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

# --- 3. Soft-fork ---
echo "=== Step 3: Soft-forking source folder ==="
echo "  Source: $SOURCE_FOLDER"
echo "  Target: $TARGET_PATH"
"$BIN" fork "$SOURCE_FOLDER" -m "$TARGET_PATH"
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

# --- 4. Find the largest directory ---
echo "=== Step 4: Finding largest directory ==="
# Count files per top-level entry, pick the one with the most
largest_dir=""
largest_count=0
for entry in "$TARGET_PATH"/*/; do
    [ -d "$entry" ] || continue
    name=$(basename "$entry")
    count=$(find "$entry" -type f 2>/dev/null | wc -l | tr -d ' ')
    echo "  $name: $count files"
    if [ "$count" -gt "$largest_count" ]; then
        largest_count=$count
        largest_dir="$entry"
    fi
done

if [ -z "$largest_dir" ]; then
    echo "Error: no subdirectories found in mount"
    exit 1
fi

largest_name=$(basename "$largest_dir")
echo ""
echo "  Largest: $largest_name ($largest_count files)"
echo ""

# --- 5. Time cp -r ---
DEST_DIR="$TARGET_PATH/${largest_name}_copy"
echo "=== Step 5: cp -r $largest_name ==="
echo "  Source: $largest_dir"
echo "  Dest:   $DEST_DIR"
echo "  Files:  $largest_count"
echo ""

start_ts=$(perl -MTime::HiRes=time -e 'printf "%.3f\n", time')
cp -r "$largest_dir" "$DEST_DIR" 2>&1
end_ts=$(perl -MTime::HiRes=time -e 'printf "%.3f\n", time')

elapsed=$(perl -e "printf '%.3f', $end_ts - $start_ts")
if [ "$largest_count" -gt 0 ]; then
    per_file=$(perl -e "printf '%.2f', ($end_ts - $start_ts) * 1000 / $largest_count")
    files_per_sec=$(perl -e "printf '%.1f', $largest_count / ($end_ts - $start_ts)")
else
    per_file="N/A"
    files_per_sec="N/A"
fi

echo "=== Results ==="
echo "  Directory:    $largest_name"
echo "  Files:        $largest_count"
echo "  Total time:   ${elapsed}s"
echo "  Per file:     ${per_file} ms"
echo "  Throughput:   ${files_per_sec} files/s"

# Verify copy
if [ ! -d "$DEST_DIR" ]; then
    echo ""
    echo "ERROR: Destination directory was not created!"
    exit 1
fi

copied_count=$(find "$DEST_DIR" -type f 2>/dev/null | wc -l | tr -d ' ')
if [ "$copied_count" -ne "$largest_count" ]; then
    echo ""
    echo "WARNING: File count mismatch! Expected $largest_count, got $copied_count"
    exit 1
else
    echo "  Verified:     $copied_count files copied"
fi

# --- 6. Summary of trace log ---
echo ""
echo "=== Trace Log Summary ==="
if [ -f "$DAEMON_LOG" ]; then
    total_lines=$(wc -l < "$DAEMON_LOG" | tr -d ' ')
    trace_lines=$(grep -c 'level=trace' "$DAEMON_LOG" 2>/dev/null || echo 0)
    echo "  Total log lines: $total_lines"
    echo "  VFS trace lines: $trace_lines"

    # Extract durations from trace lines and sort by slowest
    # Format: level=trace msg="[VFS] WriteByPath ... (1.234ms)"
    echo ""
    echo "  Top 20 slowest VFS operations:"
    grep 'level=trace.*\[VFS\]' "$DAEMON_LOG" | \
        perl -ne '
            if (/msg="(.+?)\(([0-9.]+)(µs|ms|s)\)"/) {
                my ($op, $val, $unit) = ($1, $2, $3);
                my $us = $unit eq "s" ? $val*1e6 : $unit eq "ms" ? $val*1e3 : $val;
                printf "%12.0f µs  %s\n", $us, $op;
            }
        ' | sort -rn | head -20 | while read line; do echo "    $line"; done || true

    # Aggregate: average duration per operation type
    echo ""
    echo "  Average duration by operation type:"
    grep 'level=trace.*\[VFS\]' "$DAEMON_LOG" | \
        perl -ne '
            if (/msg="\[VFS\] (\w+).+?\(([0-9.]+)(µs|ms|s)\)"/) {
                my ($op, $val, $unit) = ($1, $2, $3);
                my $us = $unit eq "s" ? $val*1e6 : $unit eq "ms" ? $val*1e3 : $val;
                $sum{$op} += $us; $cnt{$op}++;
            }
            END {
                for (sort { $sum{$b}/$cnt{$b} <=> $sum{$a}/$cnt{$a} } keys %cnt) {
                    printf "    %-20s  avg=%8.0f µs  count=%5d  total=%8.0f ms\n",
                        $_, $sum{$_}/$cnt{$_}, $cnt{$_}, $sum{$_}/1000;
                }
            }
        '
fi
