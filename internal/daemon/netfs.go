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

// NetFSServer abstracts the network filesystem server (SMB or NFS)
type NetFSServer interface {
	// Serve starts the server on the given address (e.g., "127.0.0.1:12345")
	Serve(addr string) error

	// Shutdown stops the server
	Shutdown()
}

// NetFSType returns the type of network filesystem in use ("smb" or "nfs")
// Implemented via build tags in server_smb.go or server_nfs.go
func NetFSType() string {
	return netFSTypeName
}

// netFSTypeName is set by build-tagged files
var netFSTypeName string
