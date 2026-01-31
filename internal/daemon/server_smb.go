//go:build smb

package daemon

import (
	"fmt"

	smb2 "github.com/macos-fuse-t/go-smb2/server"
	"github.com/macos-fuse-t/go-smb2/vfs"
)

func init() {
	netFSTypeName = "smb"
}

// SMBServer wraps the go-smb2 server
type SMBServer struct {
	server *smb2.Server
}

// NewSMBServer creates a new SMB server for the given VFS
func NewSMBServer(fs vfs.VFSFileSystem, shareName string) *SMBServer {
	smbCfg := &smb2.ServerConfig{
		AllowGuest:  true,
		MaxIOReads:  4,
		MaxIOWrites: 4,
	}

	shares := map[string]vfs.VFSFileSystem{
		shareName: fs,
	}

	auth := &smb2.NTLMAuthenticator{
		NbDomain:   "WORKGROUP",
		NbName:     "LATENTFS",
		DnsName:    "latentfs.local",
		DnsDomain:  ".local",
		AllowGuest: true,
	}

	return &SMBServer{
		server: smb2.NewServer(smbCfg, auth, shares),
	}
}

// Serve starts the SMB server
func (s *SMBServer) Serve(addr string) error {
	return s.server.Serve(addr)
}

// Shutdown stops the SMB server
func (s *SMBServer) Shutdown() {
	s.server.Shutdown()
}

// SMBMount mounts an SMB share at the given path (macOS specific)
func SMBMount(port int, shareName string, mountPath string) error {
	return Mount(port, shareName, mountPath)
}

// CreateNetFSServer creates an SMB server (used by daemon.go)
func CreateNetFSServer(fs vfs.VFSFileSystem, shareName string) NetFSServer {
	return NewSMBServer(fs, shareName)
}

// MountNetFS mounts the network filesystem
func MountNetFS(ip string, port int, shareName string, mountPath string) error {
	return fmt.Errorf("SMB mount: use Mount() from mount_darwin.go")
}
