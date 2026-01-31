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
