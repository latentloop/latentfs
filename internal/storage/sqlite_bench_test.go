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

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/uptrace/bun"
)

// BenchmarkTombstoneInsert profiles the per-file cost of DeleteDentryAndInodeTx
// (the hot path for rm -rf). Measures individual phases: tx begin, dentry tombstone,
// inode tombstone, tx commit.
func BenchmarkTombstoneInsert(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.latentfs")

	df, err := Create(path)
	if err != nil {
		b.Fatalf("Create failed: %v", err)
	}
	defer df.Close()

	// Set up a write epoch
	epoch, err := df.CreateWriteEpoch("ready", "bench")
	if err != nil {
		b.Fatalf("CreateWriteEpoch failed: %v", err)
	}
	if err := df.SetCurrentWriteEpoch(epoch); err != nil {
		b.Fatalf("SetCurrentWriteEpoch failed: %v", err)
	}
	if err := df.RefreshEpochs(); err != nil {
		b.Fatalf("RefreshEpochs failed: %v", err)
	}

	// Pre-create N files to delete
	N := b.N
	if N < 10 {
		N = 10
	}
	inos := make([]int64, N)
	for i := 0; i < N; i++ {
		ino, err := df.CreateInode(DefaultFileMode)
		if err != nil {
			b.Fatalf("CreateInode failed: %v", err)
		}
		name := fmt.Sprintf("file_%06d", i)
		if err := df.CreateDentry(RootIno, name, ino); err != nil {
			b.Fatalf("CreateDentry failed: %v", err)
		}
		inos[i] = ino
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % N
		name := fmt.Sprintf("file_%06d", idx)
		err := df.RunInTx(ctx, func(ctx context.Context, tx bun.Tx) error {
			return df.DeleteDentryAndInodeTx(ctx, tx, RootIno, name, inos[idx], false)
		})
		if err != nil {
			b.Fatalf("DeleteDentryAndInodeTx failed: %v", err)
		}
	}
	b.StopTimer()
	elapsed := b.Elapsed()
	b.ReportMetric(float64(elapsed.Microseconds())/float64(b.N), "µs/op")
}

// TestSQLiteWriteProfile profiles the individual components of the tombstone write path
// to identify where time is spent.
func TestSQLiteWriteProfile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "profile.latentfs")

	df, err := Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer df.Close()

	// Set up a write epoch
	epoch, err := df.CreateWriteEpoch("ready", "profile")
	if err != nil {
		t.Fatalf("CreateWriteEpoch failed: %v", err)
	}
	if err := df.SetCurrentWriteEpoch(epoch); err != nil {
		t.Fatalf("SetCurrentWriteEpoch failed: %v", err)
	}
	if err := df.RefreshEpochs(); err != nil {
		t.Fatalf("RefreshEpochs failed: %v", err)
	}

	N := 500
	inos := make([]int64, N)
	for i := 0; i < N; i++ {
		ino, err := df.CreateInode(DefaultFileMode)
		if err != nil {
			t.Fatalf("CreateInode failed: %v", err)
		}
		name := fmt.Sprintf("file_%06d", i)
		if err := df.CreateDentry(RootIno, name, ino); err != nil {
			t.Fatalf("CreateDentry failed: %v", err)
		}
		inos[i] = ino
	}

	ctx := context.Background()

	// Profile: individual transaction with tombstone inserts
	var totalRunInTx, totalDentryTombstone, totalInodeTombstone time.Duration
	for i := 0; i < N; i++ {
		name := fmt.Sprintf("file_%06d", i)

		txStart := time.Now()
		err := df.RunInTx(ctx, func(ctx context.Context, tx bun.Tx) error {
			d0 := time.Now()
			if err := df.bunDB.InsertDentryTombstoneWith(tx, ctx, RootIno, name, df.currentWriteEpoch); err != nil {
				return err
			}
			d1 := time.Now()
			totalDentryTombstone += d1.Sub(d0)

			if err := df.bunDB.InsertInodeTombstoneWith(tx, ctx, inos[i], df.currentWriteEpoch); err != nil {
				return err
			}
			d2 := time.Now()
			totalInodeTombstone += d2.Sub(d1)
			return nil
		})
		if err != nil {
			t.Fatalf("RunInTx failed at %d: %v", i, err)
		}
		totalRunInTx += time.Since(txStart)
	}

	avgTotal := totalRunInTx / time.Duration(N)
	avgDentry := totalDentryTombstone / time.Duration(N)
	avgInode := totalInodeTombstone / time.Duration(N)
	avgTxOverhead := avgTotal - avgDentry - avgInode

	t.Logf("=== SQLite Write Profile (N=%d) ===", N)
	t.Logf("  Total per delete (RunInTx):     %v", avgTotal)
	t.Logf("  InsertDentryTombstone:           %v", avgDentry)
	t.Logf("  InsertInodeTombstone:            %v", avgInode)
	t.Logf("  Tx overhead (begin+commit):      %v", avgTxOverhead)
	t.Logf("  --- Breakdown ---")
	t.Logf("  Dentry %%: %.1f%%", float64(totalDentryTombstone)/float64(totalRunInTx)*100)
	t.Logf("  Inode  %%: %.1f%%", float64(totalInodeTombstone)/float64(totalRunInTx)*100)
	t.Logf("  Tx     %%: %.1f%%", float64(totalRunInTx-totalDentryTombstone-totalInodeTombstone)/float64(totalRunInTx)*100)

	// Test 2: Raw SQL (bypass Bun ORM) for comparison
	t.Logf("")
	t.Logf("=== Raw SQL Comparison ===")

	// Create more files for raw SQL test
	rawInos := make([]int64, N)
	for i := 0; i < N; i++ {
		ino, err := df.CreateInode(DefaultFileMode)
		if err != nil {
			t.Fatalf("CreateInode failed: %v", err)
		}
		name := fmt.Sprintf("raw_%06d", i)
		if err := df.CreateDentry(RootIno, name, ino); err != nil {
			t.Fatalf("CreateDentry failed: %v", err)
		}
		rawInos[i] = ino
	}

	var totalRawTx time.Duration
	now := time.Now().Unix()
	for i := 0; i < N; i++ {
		name := fmt.Sprintf("raw_%06d", i)
		txStart := time.Now()

		tx, err := df.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO dentries (parent_ino, name, write_epoch, ino) VALUES (?, ?, ?, 0)
			 ON CONFLICT (parent_ino, name, write_epoch) DO UPDATE SET ino = 0`,
			RootIno, name, df.currentWriteEpoch)
		if err != nil {
			tx.Rollback()
			t.Fatalf("raw dentry tombstone failed: %v", err)
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO inodes (ino, write_epoch, mode, uid, gid, size, atime, mtime, ctime, nlink)
			 VALUES (?, ?, 0, 0, 0, 0, ?, ?, ?, 0)
			 ON CONFLICT (ino, write_epoch) DO UPDATE SET mode = 0, ctime = EXCLUDED.ctime`,
			rawInos[i], df.currentWriteEpoch, now, now, now)
		if err != nil {
			tx.Rollback()
			t.Fatalf("raw inode tombstone failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		totalRawTx += time.Since(txStart)
	}

	avgRaw := totalRawTx / time.Duration(N)
	t.Logf("  Raw SQL per delete:             %v", avgRaw)
	t.Logf("  Bun ORM per delete:             %v", avgTotal)
	t.Logf("  ORM overhead:                   %v (%.1f%%)", avgTotal-avgRaw, float64(avgTotal-avgRaw)/float64(avgTotal)*100)

	// Test 3: Batched transaction (N deletes in one tx)
	t.Logf("")
	t.Logf("=== Batched Transaction (all %d deletes in 1 tx) ===", N)

	batchInos := make([]int64, N)
	for i := 0; i < N; i++ {
		ino, err := df.CreateInode(DefaultFileMode)
		if err != nil {
			t.Fatalf("CreateInode failed: %v", err)
		}
		name := fmt.Sprintf("batch_%06d", i)
		if err := df.CreateDentry(RootIno, name, ino); err != nil {
			t.Fatalf("CreateDentry failed: %v", err)
		}
		batchInos[i] = ino
	}

	batchStart := time.Now()
	tx, err := df.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	for i := 0; i < N; i++ {
		name := fmt.Sprintf("batch_%06d", i)
		_, err = tx.ExecContext(ctx,
			`INSERT INTO dentries (parent_ino, name, write_epoch, ino) VALUES (?, ?, ?, 0)
			 ON CONFLICT (parent_ino, name, write_epoch) DO UPDATE SET ino = 0`,
			RootIno, name, df.currentWriteEpoch)
		if err != nil {
			tx.Rollback()
			t.Fatalf("batch dentry tombstone failed: %v", err)
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO inodes (ino, write_epoch, mode, uid, gid, size, atime, mtime, ctime, nlink)
			 VALUES (?, ?, 0, 0, 0, 0, ?, ?, ?, 0)
			 ON CONFLICT (ino, write_epoch) DO UPDATE SET mode = 0, ctime = EXCLUDED.ctime`,
			batchInos[i], df.currentWriteEpoch, now, now, now)
		if err != nil {
			tx.Rollback()
			t.Fatalf("batch inode tombstone failed: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("batch Commit failed: %v", err)
	}
	batchElapsed := time.Since(batchStart)
	t.Logf("  Batched total (%d deletes):     %v", N, batchElapsed)
	t.Logf("  Batched per delete:             %v", batchElapsed/time.Duration(N))
	t.Logf("  Speedup vs 1-per-tx (raw):      %.1fx", float64(totalRawTx)/float64(batchElapsed))
	t.Logf("  Speedup vs 1-per-tx (Bun):      %.1fx", float64(totalRunInTx)/float64(batchElapsed))

	// Test 4: PRAGMA tuning effects
	t.Logf("")
	t.Logf("=== PRAGMA Tuning Effects ===")

	pragmas := []struct {
		name    string
		set     string
		reset   string
	}{
		{"cache_size=-65536 (64MB)", "PRAGMA cache_size = -65536", "PRAGMA cache_size = -2000"},
		{"temp_store=MEMORY", "PRAGMA temp_store = MEMORY", "PRAGMA temp_store = DEFAULT"},
		{"wal_autocheckpoint=0 (disable)", "PRAGMA wal_autocheckpoint = 0", "PRAGMA wal_autocheckpoint = 1000"},
	}

	for _, p := range pragmas {
		// Create test files
		pInos := make([]int64, N)
		prefix := fmt.Sprintf("p%s_", p.name[:3])
		for i := 0; i < N; i++ {
			ino, err := df.CreateInode(DefaultFileMode)
			if err != nil {
				t.Fatalf("CreateInode failed: %v", err)
			}
			name := fmt.Sprintf("%s%06d", prefix, i)
			if err := df.CreateDentry(RootIno, name, ino); err != nil {
				t.Fatalf("CreateDentry failed: %v", err)
			}
			pInos[i] = ino
		}

		// Set pragma
		if _, err := df.db.ExecContext(ctx, p.set); err != nil {
			t.Logf("  SKIP %s: %v", p.name, err)
			continue
		}

		var pTotal time.Duration
		for i := 0; i < N; i++ {
			name := fmt.Sprintf("%s%06d", prefix, i)
			s := time.Now()
			ptx, err := df.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("BeginTx failed: %v", err)
			}
			_, err = ptx.ExecContext(ctx,
				`INSERT INTO dentries (parent_ino, name, write_epoch, ino) VALUES (?, ?, ?, 0)
				 ON CONFLICT (parent_ino, name, write_epoch) DO UPDATE SET ino = 0`,
				RootIno, name, df.currentWriteEpoch)
			if err != nil {
				ptx.Rollback()
				t.Fatalf("failed: %v", err)
			}
			_, err = ptx.ExecContext(ctx,
				`INSERT INTO inodes (ino, write_epoch, mode, uid, gid, size, atime, mtime, ctime, nlink)
				 VALUES (?, ?, 0, 0, 0, 0, ?, ?, ?, 0)
				 ON CONFLICT (ino, write_epoch) DO UPDATE SET mode = 0, ctime = EXCLUDED.ctime`,
				pInos[i], df.currentWriteEpoch, now, now, now)
			if err != nil {
				ptx.Rollback()
				t.Fatalf("failed: %v", err)
			}
			if err := ptx.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}
			pTotal += time.Since(s)
		}

		avgP := pTotal / time.Duration(N)
		t.Logf("  %s: %v/op (%.1f%% vs baseline %v)",
			p.name, avgP,
			(1-float64(avgP)/float64(avgRaw))*100, avgRaw)

		// Reset pragma
		df.db.ExecContext(ctx, p.reset)
	}

	// Test 5: Check current page size
	var pageSize int
	row := df.db.QueryRowContext(ctx, "PRAGMA page_size")
	row.Scan(&pageSize)
	t.Logf("")
	t.Logf("=== Current Settings ===")
	t.Logf("  page_size: %d", pageSize)

	var cacheSize int
	row = df.db.QueryRowContext(ctx, "PRAGMA cache_size")
	row.Scan(&cacheSize)
	t.Logf("  cache_size: %d", cacheSize)

	var walAutoCheckpoint int
	row = df.db.QueryRowContext(ctx, "PRAGMA wal_autocheckpoint")
	row.Scan(&walAutoCheckpoint)
	t.Logf("  wal_autocheckpoint: %d", walAutoCheckpoint)

	var journalMode string
	row = df.db.QueryRowContext(ctx, "PRAGMA journal_mode")
	row.Scan(&journalMode)
	t.Logf("  journal_mode: %s", journalMode)

	var syncMode int
	row = df.db.QueryRowContext(ctx, "PRAGMA synchronous")
	row.Scan(&syncMode)
	t.Logf("  synchronous: %d (0=OFF, 1=NORMAL, 2=FULL)", syncMode)

	// Test 6: synchronous=OFF comparison
	t.Logf("")
	t.Logf("=== synchronous=OFF (unsafe but fast — baseline for max speed) ===")

	offInos := make([]int64, N)
	for i := 0; i < N; i++ {
		ino, err := df.CreateInode(DefaultFileMode)
		if err != nil {
			t.Fatalf("CreateInode failed: %v", err)
		}
		name := fmt.Sprintf("off_%06d", i)
		if err := df.CreateDentry(RootIno, name, ino); err != nil {
			t.Fatalf("CreateDentry failed: %v", err)
		}
		offInos[i] = ino
	}

	df.db.ExecContext(ctx, "PRAGMA synchronous = OFF")
	var totalOff time.Duration
	for i := 0; i < N; i++ {
		name := fmt.Sprintf("off_%06d", i)
		s := time.Now()
		otx, err := df.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}
		_, _ = otx.ExecContext(ctx,
			`INSERT INTO dentries (parent_ino, name, write_epoch, ino) VALUES (?, ?, ?, 0)
			 ON CONFLICT (parent_ino, name, write_epoch) DO UPDATE SET ino = 0`,
			RootIno, name, df.currentWriteEpoch)
		_, _ = otx.ExecContext(ctx,
			`INSERT INTO inodes (ino, write_epoch, mode, uid, gid, size, atime, mtime, ctime, nlink)
			 VALUES (?, ?, 0, 0, 0, 0, ?, ?, ?, 0)
			 ON CONFLICT (ino, write_epoch) DO UPDATE SET mode = 0, ctime = EXCLUDED.ctime`,
			offInos[i], df.currentWriteEpoch, now, now, now)
		otx.Commit()
		totalOff += time.Since(s)
	}
	df.db.ExecContext(ctx, "PRAGMA synchronous = NORMAL")

	avgOff := totalOff / time.Duration(N)
	t.Logf("  synchronous=OFF per delete:     %v", avgOff)
	t.Logf("  synchronous=NORMAL per delete:  %v", avgRaw)
	t.Logf("  Speedup:                        %.1fx", float64(totalRawTx)/float64(totalOff))

	// Summary
	t.Logf("")
	t.Logf("=== SUMMARY ===")
	t.Logf("  Bun ORM, 1-per-tx:             %v/op", avgTotal)
	t.Logf("  Raw SQL, 1-per-tx:             %v/op", avgRaw)
	t.Logf("  Raw SQL, batched (%d/tx):     %v/op", N, batchElapsed/time.Duration(N))
	t.Logf("  Raw SQL, sync=OFF, 1-per-tx:   %v/op", avgOff)

	_ = os.Getenv("") // avoid unused import
}
