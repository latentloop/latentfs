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

//go:build !smb

package daemon

import (
	"fmt"
	"log"

	"latentfs/internal/storage"
	latentfs "latentfs/internal/vfs"
)

// createServerForLatentFS creates a network filesystem server for LatentFS
func createServerForLatentFS(fs *latentfs.LatentFS, shareName string) (NetFSServer, error) {
	return NewNFSServer(fs, shareName), nil
}

// createServerForMetaFS creates a network filesystem server for MetaFS
func createServerForMetaFS(fs *latentfs.MetaFS, shareName string) (NetFSServer, error) {
	return NewNFSServerForMetaFS(fs, shareName), nil
}

// mountNetFS mounts the network filesystem
func mountNetFS(ip string, port int, shareName string, mountPath string) error {
	return NFSMount(ip, port, shareName, mountPath)
}

// openDataFileAndCreateServer opens a data file and creates a server for it
func openDataFileAndCreateServer(dataFilePath, shareName string) (*storage.DataFile, NetFSServer, error) {
	dataFile, err := storage.OpenWithContext(dataFilePath, storage.DBContextDaemon)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open data file: %w", err)
	}

	fs := latentfs.NewLatentFS(dataFile)
	srv, err := createServerForLatentFS(fs, shareName)
	if err != nil {
		dataFile.Close()
		return nil, nil, err
	}

	return dataFile, srv, nil
}

// logServerType logs what type of server is being used
func logServerType() {
	log.Printf("Using NFS server")
}
