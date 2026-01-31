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
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/tursodatabase/go-libsql"
)

// TestSQLiteForkEmptyMount verifies SQLite operations work correctly on a fork-empty mount.
// This tests that:
// - A SQLite database can be created inside a latentfs mount
// - SQL operations (CREATE TABLE, INSERT) work correctly
// - Data persists after closing and reopening the connection
// - The daemon remains stable throughout the operations
func TestSQLiteForkEmptyMount(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	env := NewTestEnv(t, "sqlite_mount")
	defer env.Cleanup()

	// 1. Create fork-empty mount
	env.InitDataFile()

	// Verify mount is accessible
	if !env.FileExists(".") {
		t.Fatal("mount point is not accessible")
	}

	// 2. Create SQLite database inside the mount
	dbPath := filepath.Join(env.Mount, "test.db")
	dsn := "file:" + dbPath

	db, err := sql.Open("libsql", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// 3. Create table and insert data
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT NOT NULL
	)`)
	if err != nil {
		db.Close()
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO users (name, email) VALUES (?, ?)`, "Alice", "alice@example.com")
	if err != nil {
		db.Close()
		t.Fatalf("failed to insert row 1: %v", err)
	}

	_, err = db.Exec(`INSERT INTO users (name, email) VALUES (?, ?)`, "Bob", "bob@example.com")
	if err != nil {
		db.Close()
		t.Fatalf("failed to insert row 2: %v", err)
	}

	// Verify data was inserted
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count)
	if err != nil {
		db.Close()
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 2 {
		db.Close()
		t.Fatalf("expected 2 rows, got %d", count)
	}

	t.Log("Initial data insertion successful")

	// 4. Close the connection
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	t.Log("Database connection closed")

	// 5. Verify daemon is still running
	result := env.RunCLI("daemon", "status")
	if !result.Contains("running") || result.Contains("not running") {
		t.Fatalf("daemon should still be running, status: %s", result.Combined)
	}

	t.Log("Daemon still running after first connection close")

	// 6. Reconnect to the database
	db2, err := sql.Open("libsql", dsn)
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer db2.Close()

	// 7. Verify data persisted
	var persistedCount int
	err = db2.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&persistedCount)
	if err != nil {
		t.Fatalf("failed to count rows after reconnect: %v", err)
	}
	if persistedCount != 2 {
		t.Fatalf("expected 2 rows after reconnect, got %d", persistedCount)
	}

	// Verify specific data
	var name, email string
	err = db2.QueryRow(`SELECT name, email FROM users WHERE id = 1`).Scan(&name, &email)
	if err != nil {
		t.Fatalf("failed to query user 1: %v", err)
	}
	if name != "Alice" || email != "alice@example.com" {
		t.Fatalf("expected Alice/alice@example.com, got %s/%s", name, email)
	}

	t.Log("Data persistence verified after reconnect")

	// 8. Verify daemon is still running and mount is healthy
	result = env.RunCLI("daemon", "status")
	if !result.Contains("running") || result.Contains("not running") {
		t.Fatalf("daemon should still be running after all operations, status: %s", result.Combined)
	}

	// Verify mount is still accessible
	if !env.FileExists("test.db") {
		t.Fatal("database file should exist in mount")
	}

	t.Log("SQLite fork-empty mount test successful")
}

// TestSQLiteMultipleOperations tests more complex SQLite operations on a mount
func TestSQLiteMultipleOperations(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	env := NewTestEnv(t, "sqlite_ops")
	defer env.Cleanup()

	env.InitDataFile()

	dbPath := filepath.Join(env.Mount, "multi.db")
	dsn := "file:" + dbPath

	db, err := sql.Open("libsql", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create multiple tables
	_, err = db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			price REAL NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("failed to create products table: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			product_id INTEGER,
			quantity INTEGER,
			FOREIGN KEY (product_id) REFERENCES products(id)
		)
	`)
	if err != nil {
		t.Fatalf("failed to create orders table: %v", err)
	}

	// Insert data
	_, err = db.Exec(`INSERT INTO products (name, price) VALUES ('Widget', 9.99)`)
	if err != nil {
		t.Fatalf("failed to insert product 1: %v", err)
	}
	_, err = db.Exec(`INSERT INTO products (name, price) VALUES ('Gadget', 19.99)`)
	if err != nil {
		t.Fatalf("failed to insert product 2: %v", err)
	}

	_, err = db.Exec(`INSERT INTO orders (product_id, quantity) VALUES (1, 5)`)
	if err != nil {
		t.Fatalf("failed to insert order 1: %v", err)
	}
	_, err = db.Exec(`INSERT INTO orders (product_id, quantity) VALUES (2, 3)`)
	if err != nil {
		t.Fatalf("failed to insert order 2: %v", err)
	}
	_, err = db.Exec(`INSERT INTO orders (product_id, quantity) VALUES (1, 2)`)
	if err != nil {
		t.Fatalf("failed to insert order 3: %v", err)
	}

	// Test a join query
	rows, err := db.Query(`
		SELECT p.name, SUM(o.quantity) as total
		FROM products p
		JOIN orders o ON p.id = o.product_id
		GROUP BY p.id
		ORDER BY p.name
	`)
	if err != nil {
		t.Fatalf("failed to run join query: %v", err)
	}
	defer rows.Close()

	results := make(map[string]int)
	for rows.Next() {
		var name string
		var total int
		if err := rows.Scan(&name, &total); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		results[name] = total
	}

	if results["Widget"] != 7 {
		t.Errorf("expected Widget total 7, got %d", results["Widget"])
	}
	if results["Gadget"] != 3 {
		t.Errorf("expected Gadget total 3, got %d", results["Gadget"])
	}

	// Update data
	_, err = db.Exec(`UPDATE products SET price = 12.99 WHERE name = 'Widget'`)
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	// Delete data
	_, err = db.Exec(`DELETE FROM orders WHERE quantity < 3`)
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Verify deletion
	var orderCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM orders`).Scan(&orderCount)
	if err != nil {
		t.Fatalf("failed to count orders: %v", err)
	}
	if orderCount != 2 {
		t.Errorf("expected 2 orders after delete, got %d", orderCount)
	}

	t.Log("SQLite multiple operations test successful")
}
