package daemon

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestConstants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{"RequestMount", RequestMount},
		{"RequestUnmount", RequestUnmount},
		{"RequestStatus", RequestStatus},
		{"RequestStop", RequestStop},
	}

	t.Run("all constants are non-empty", func(t *testing.T) {
		t.Parallel()
		for _, tt := range tests {
			assert.NotEmpty(t, tt.value, "%s should not be empty", tt.name)
		}
	})

	t.Run("all constants are unique", func(t *testing.T) {
		t.Parallel()
		seen := make(map[string]bool)
		for _, tt := range tests {
			assert.False(t, seen[tt.value], "duplicate request type: %s", tt.value)
			seen[tt.value] = true
		}
	})
}

func TestRequest_Fields(t *testing.T) {
	t.Parallel()

	req := &Request{
		Type:     RequestMount,
		DataFile: "/data.latentfs",
		Target:   "/target",
		All:      false,
	}

	assert.Equal(t, RequestMount, req.Type)
	assert.Equal(t, "/data.latentfs", req.DataFile)
	assert.Equal(t, "/target", req.Target)
	assert.False(t, req.All)
}

func TestMountStatus_Fields(t *testing.T) {
	t.Parallel()

	status := &MountStatus{
		DataFile: "/data.latentfs",
		Source:   "/source",
		Target:   "/target",
		UUID:     "abc-123",
	}

	assert.Equal(t, "/data.latentfs", status.DataFile)
	assert.Equal(t, "/source", status.Source)
	assert.Equal(t, "/target", status.Target)
	assert.Equal(t, "abc-123", status.UUID)
}

func TestResponse(t *testing.T) {
	t.Parallel()

	t.Run("success response", func(t *testing.T) {
		t.Parallel()
		resp := &Response{
			Success: true,
			Message: "Operation completed",
			PID:     1234,
			Mounts: []MountStatus{
				{DataFile: "/a.latentfs", Target: "/mnt/a"},
			},
		}

		assert.True(t, resp.Success)
		assert.Equal(t, "Operation completed", resp.Message)
		assert.Equal(t, 1234, resp.PID)
		assert.Len(t, resp.Mounts, 1)
	})

	t.Run("error response", func(t *testing.T) {
		t.Parallel()
		resp := &Response{
			Success: false,
			Error:   "Something went wrong",
		}

		assert.False(t, resp.Success)
		assert.Equal(t, "Something went wrong", resp.Error)
	})
}

func TestNewServer(t *testing.T) {
	t.Parallel()

	handler := func(req *Request) *Response {
		return &Response{Success: true}
	}

	server := NewServer(handler)
	require.NotNil(t, server)
	assert.NotNil(t, server.handler)
}

func TestServerStartStop(t *testing.T) {
	tmpDir := t.TempDir()
	original := os.Getenv("LATENTFS_CONFIG_DIR")
	os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
	defer os.Setenv("LATENTFS_CONFIG_DIR", original)

	handler := func(req *Request) *Response {
		return &Response{Success: true, Message: "test response"}
	}

	server := NewServer(handler)
	err := server.Start()
	require.NoError(t, err)

	_, err = os.Stat(SocketPath())
	assert.NoError(t, err, "socket file should be created")

	server.Stop()
	time.Sleep(100 * time.Millisecond)

	_, err = os.Stat(SocketPath())
	assert.True(t, os.IsNotExist(err), "socket should be removed after Stop()")
}

func TestClientServerCommunication(t *testing.T) {
	tmpDir := t.TempDir()
	original := os.Getenv("LATENTFS_CONFIG_DIR")
	os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
	defer os.Setenv("LATENTFS_CONFIG_DIR", original)

	handler := func(req *Request) *Response {
		return &Response{
			Success: true,
			Message: "received: " + req.Type,
			PID:     os.Getpid(),
		}
	}

	server := NewServer(handler)
	require.NoError(t, server.Start())
	defer server.Stop()

	client, err := Connect()
	require.NoError(t, err)
	defer client.Close()

	resp, err := client.Send(&Request{Type: RequestStatus})
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "received: status", resp.Message)
}

func TestClient_Unmount(t *testing.T) {
	tmpDir := t.TempDir()
	original := os.Getenv("LATENTFS_CONFIG_DIR")
	os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
	defer os.Setenv("LATENTFS_CONFIG_DIR", original)

	var receivedReq *Request
	handler := func(req *Request) *Response {
		receivedReq = req
		return &Response{Success: true}
	}

	server := NewServer(handler)
	server.Start()
	defer server.Stop()

	client, _ := Connect()
	defer client.Close()

	_, err := client.Unmount("/mnt/target", false)
	require.NoError(t, err)

	assert.Equal(t, RequestUnmount, receivedReq.Type)
	assert.Equal(t, "/mnt/target", receivedReq.Target)
	assert.False(t, receivedReq.All)
}

func TestClient_UnmountAll(t *testing.T) {
	tmpDir := t.TempDir()
	original := os.Getenv("LATENTFS_CONFIG_DIR")
	os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
	defer os.Setenv("LATENTFS_CONFIG_DIR", original)

	var receivedReq *Request
	handler := func(req *Request) *Response {
		receivedReq = req
		return &Response{Success: true}
	}

	server := NewServer(handler)
	server.Start()
	defer server.Stop()

	client, _ := Connect()
	defer client.Close()

	_, err := client.Unmount("", true)
	require.NoError(t, err)
	assert.True(t, receivedReq.All)
}

func TestClient_Status(t *testing.T) {
	tmpDir := t.TempDir()
	original := os.Getenv("LATENTFS_CONFIG_DIR")
	os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
	defer os.Setenv("LATENTFS_CONFIG_DIR", original)

	var receivedReq *Request
	handler := func(req *Request) *Response {
		receivedReq = req
		return &Response{
			Success: true,
			PID:     12345,
			Mounts: []MountStatus{
				{DataFile: "/a.latentfs", Target: "/mnt/a"},
			},
		}
	}

	server := NewServer(handler)
	server.Start()
	defer server.Stop()

	client, _ := Connect()
	defer client.Close()

	resp, err := client.Status()
	require.NoError(t, err)

	assert.Equal(t, RequestStatus, receivedReq.Type)
	assert.Equal(t, 12345, resp.PID)
	assert.Len(t, resp.Mounts, 1)
}

func TestClient_Mount(t *testing.T) {
	tmpDir := t.TempDir()
	original := os.Getenv("LATENTFS_CONFIG_DIR")
	os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
	defer os.Setenv("LATENTFS_CONFIG_DIR", original)

	var receivedReq *Request
	handler := func(req *Request) *Response {
		receivedReq = req
		return &Response{Success: true}
	}

	server := NewServer(handler)
	server.Start()
	defer server.Stop()

	client, err := Connect()
	require.NoError(t, err)
	_, err = client.Mount("/data.latentfs", "/target")
	client.Close()
	require.NoError(t, err)

	assert.Equal(t, RequestMount, receivedReq.Type)
	assert.Equal(t, "/data.latentfs", receivedReq.DataFile)
	assert.Equal(t, "/target", receivedReq.Target)
}

func TestClient_Stop(t *testing.T) {
	tmpDir := t.TempDir()
	original := os.Getenv("LATENTFS_CONFIG_DIR")
	os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
	defer os.Setenv("LATENTFS_CONFIG_DIR", original)

	var receivedReq *Request
	handler := func(req *Request) *Response {
		receivedReq = req
		return &Response{Success: true}
	}

	server := NewServer(handler)
	server.Start()
	defer server.Stop()

	client, _ := Connect()
	defer client.Close()

	_, err := client.Stop()
	require.NoError(t, err)
	assert.Equal(t, RequestStop, receivedReq.Type)
}

func TestIsDaemonRunning(t *testing.T) {
	t.Run("returns false when not running", func(t *testing.T) {
		tmpDir := t.TempDir()
		original := os.Getenv("LATENTFS_CONFIG_DIR")
		os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
		defer os.Setenv("LATENTFS_CONFIG_DIR", original)

		assert.False(t, IsDaemonRunning())
	})

	t.Run("returns true when running", func(t *testing.T) {
		// Use short path to avoid Unix socket path length limit (~104 chars on macOS)
		tmpDir, err := os.MkdirTemp("/tmp", "lfs")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		original := os.Getenv("LATENTFS_CONFIG_DIR")
		os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
		defer os.Setenv("LATENTFS_CONFIG_DIR", original)

		handler := func(req *Request) *Response {
			return &Response{Success: true}
		}

		server := NewServer(handler)
		err = server.Start()
		require.NoError(t, err, "server.Start() failed")
		defer server.Stop()

		// Wait for server to be ready to accept connections
		time.Sleep(50 * time.Millisecond)

		assert.True(t, IsDaemonRunning())
	})
}

func TestConnect_NotRunning(t *testing.T) {
	tmpDir := t.TempDir()
	original := os.Getenv("LATENTFS_CONFIG_DIR")
	os.Setenv("LATENTFS_CONFIG_DIR", tmpDir)
	defer os.Setenv("LATENTFS_CONFIG_DIR", original)

	_, err := Connect()
	assert.Error(t, err)
}
