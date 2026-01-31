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

package daemon

import (
	"log"
	"path/filepath"
	"time"

	"latentfs/internal/storage"
)

// handleProtect adds a data file to the protected list and keeps it open
func (d *Daemon) handleProtect(req *Request) *Response {
	if req.DataFile == "" {
		return &Response{Success: false, Error: "data_file is required"}
	}

	// Resolve to absolute path
	absPath, err := filepath.Abs(req.DataFile)
	if err != nil {
		return &Response{Success: false, Error: "failed to resolve path: " + err.Error()}
	}

	// Check if already protected
	if _, loaded := d.protectedFiles.Load(absPath); loaded {
		return &Response{Success: true, Message: "already protected: " + absPath}
	}

	// Open the data file to keep it protected
	df, err := storage.OpenWithContext(absPath, storage.DBContextDaemon)
	if err != nil {
		return &Response{Success: false, Error: "failed to open data file: " + err.Error()}
	}

	// Store in memory map
	d.protectedFiles.Store(absPath, df)

	// Persist to meta file
	if d.centralMetaFile != nil {
		if err := d.centralMetaFile.AddProtectedFile(absPath); err != nil {
			log.Printf("protect: failed to persist to meta file: %v", err)
			// Continue anyway - in-memory protection is active
		}
	}

	log.Printf("protect: protected %s", absPath)
	return &Response{Success: true, Message: "protected: " + absPath}
}

// handleRelease removes a data file from the protected list
func (d *Daemon) handleRelease(req *Request) *Response {
	if req.DataFile == "" {
		return &Response{Success: false, Error: "data_file is required"}
	}

	// Resolve to absolute path
	absPath, err := filepath.Abs(req.DataFile)
	if err != nil {
		return &Response{Success: false, Error: "failed to resolve path: " + err.Error()}
	}

	// Load and remove from map
	val, loaded := d.protectedFiles.LoadAndDelete(absPath)
	if !loaded {
		return &Response{Success: false, Error: "not protected: " + absPath}
	}

	// Close the data file
	if df, ok := val.(*storage.DataFile); ok {
		df.Close()
	}

	// Remove from meta file
	if d.centralMetaFile != nil {
		if err := d.centralMetaFile.RemoveProtectedFile(absPath); err != nil {
			log.Printf("release: failed to remove from meta file: %v", err)
			// Continue anyway - in-memory protection is removed
		}
	}

	log.Printf("release: released %s", absPath)
	return &Response{Success: true, Message: "released: " + absPath}
}

// handleListProtect returns the list of protected data files
func (d *Daemon) handleListProtect() *Response {
	var files []ProtectedFile

	d.protectedFiles.Range(func(key, value any) bool {
		path := key.(string)
		files = append(files, ProtectedFile{
			DataFile:  path,
			CreatedAt: time.Now().Unix(), // We don't track creation time in memory; use now as placeholder
		})
		return true
	})

	// If we have a meta file, use its timestamps
	if d.centralMetaFile != nil {
		entries, err := d.centralMetaFile.ListProtectedFiles()
		if err == nil {
			// Build a map for quick lookup
			entryMap := make(map[string]int64)
			for _, e := range entries {
				entryMap[e.DataFile] = e.CreatedAt.Unix()
			}
			// Update timestamps
			for i := range files {
				if ts, ok := entryMap[files[i].DataFile]; ok {
					files[i].CreatedAt = ts
				}
			}
		}
	}

	return &Response{
		Success:        true,
		ProtectedFiles: files,
	}
}

// loadProtectedFiles loads protected files from the meta file on daemon start
func (d *Daemon) loadProtectedFiles() {
	if d.centralMetaFile == nil {
		return
	}

	entries, err := d.centralMetaFile.ListProtectedFiles()
	if err != nil {
		log.Printf("loadProtectedFiles: failed to list: %v", err)
		return
	}

	for _, entry := range entries {
		// Try to open the data file
		df, err := storage.OpenWithContext(entry.DataFile, storage.DBContextDaemon)
		if err != nil {
			log.Printf("loadProtectedFiles: skipping %s (failed to open: %v)", entry.DataFile, err)
			// Remove from meta file since it can't be opened
			d.centralMetaFile.RemoveProtectedFile(entry.DataFile)
			continue
		}

		d.protectedFiles.Store(entry.DataFile, df)
		log.Printf("loadProtectedFiles: restored protection for %s", entry.DataFile)
	}
}

// closeProtectedFiles closes all protected files (called on daemon stop)
func (d *Daemon) closeProtectedFiles() {
	d.protectedFiles.Range(func(key, value any) bool {
		if df, ok := value.(*storage.DataFile); ok {
			df.Close()
		}
		d.protectedFiles.Delete(key)
		return true
	})
}
