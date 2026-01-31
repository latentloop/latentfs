//go:build !smb

package daemon

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"time"

	billy "github.com/go-git/go-billy/v5"
	log "github.com/sirupsen/logrus"
	nfs "github.com/willscott/go-nfs"
	nfsfile "github.com/willscott/go-nfs/file"
	nfshelper "github.com/willscott/go-nfs/helpers"

	latentfs "latentfs/internal/vfs"
)

// vfsHandle is the handle type used by the VFS layer
// Uses the type exported from internal/vfs to avoid importing SMB packages
type vfsHandle = latentfs.NfsVfsHandle

func init() {
	netFSTypeName = "nfs"
}

// NFSServer wraps the go-nfs server
type NFSServer struct {
	listener net.Listener
	server   *nfs.Server
	handler  nfs.Handler
	cancel   context.CancelFunc
	done     chan struct{}
}

// NewNFSServer creates a new NFS server for the given VFS
func NewNFSServer(fs *latentfs.LatentFS, shareName string) *NFSServer {
	// Set go-nfs log level to match daemon's log level
	if log.IsLevelEnabled(log.TraceLevel) {
		nfs.Log.SetLevel(nfs.TraceLevel)
	} else if log.IsLevelEnabled(log.DebugLevel) {
		nfs.Log.SetLevel(nfs.DebugLevel)
	}
	billyFS := NewBillyAdapter(fs)
	handler := nfshelper.NewNullAuthHandler(billyFS)
	cacheHelper := nfshelper.NewCachingHandler(handler, 65536)

	ctx, cancel := context.WithCancel(context.Background())
	server := &nfs.Server{
		Handler: cacheHelper,
		Context: ctx,
	}

	return &NFSServer{
		server:  server,
		handler: cacheHelper,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
}

// NewNFSServerForMetaFS creates a new NFS server for MetaFS
func NewNFSServerForMetaFS(fs *latentfs.MetaFS, shareName string) *NFSServer {
	// Set go-nfs log level to match daemon's log level
	if log.IsLevelEnabled(log.TraceLevel) {
		nfs.Log.SetLevel(nfs.TraceLevel)
	} else if log.IsLevelEnabled(log.DebugLevel) {
		nfs.Log.SetLevel(nfs.DebugLevel)
	}
	billyFS := NewBillyMetaAdapter(fs)
	handler := nfshelper.NewNullAuthHandler(billyFS)
	cacheHelper := nfshelper.NewCachingHandler(handler, 65536)

	ctx, cancel := context.WithCancel(context.Background())
	server := &nfs.Server{
		Handler: cacheHelper,
		Context: ctx,
	}

	return &NFSServer{
		server:  server,
		handler: cacheHelper,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
}

// Serve starts the NFS server
func (s *NFSServer) Serve(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = listener

	return s.server.Serve(listener)
}

// Shutdown stops the NFS server gracefully
func (s *NFSServer) Shutdown() {
	// Close the listener first to stop accepting new connections
	if s.listener != nil {
		s.listener.Close()
	}

	// Settle time for in-flight NFS operations to complete after listener close.
	// The central mount is unmounted BEFORE this shutdown call, so the kernel
	// NFS client has already disconnected. 100ms is sufficient for any
	// residual in-flight requests given soft,timeo=20 mount options.
	time.Sleep(100 * time.Millisecond)

	// Cancel context to signal handlers to stop
	if s.cancel != nil {
		s.cancel()
	}

	close(s.done)
}

// NFSMount mounts an NFS share at the given path (macOS specific)
func NFSMount(ip string, port int, shareName string, mountPath string) error {
	// Ensure mount point exists
	if err := os.MkdirAll(mountPath, 0755); err != nil {
		return fmt.Errorf("failed to create mount point: %w", err)
	}

	// Mount using macOS mount_nfs command
	// NFS mount format: mount_nfs -o port=<port>,mountport=<port>,tcp localhost:/ <mountpath>
	// Note: noac disables attribute caching to ensure changes to source files are immediately visible
	// Note: soft,timeo=50 (5 seconds per attempt), retrans=3 gives ~20s before the kernel
	//   marks the mount VQ_DEAD. This prevents zombie kernel mounts that can only be cleared
	//   by reboot, while allowing enough time for the NFS server to respond under CPU contention
	//   (e.g., parallel test runs with multiple daemon processes).
	//   When daemon shuts down gracefully, it unmounts first so soft timeout is never hit
	//   in normal operation.
	// Note: nobrowse hides mount from Finder/Desktop and prevents Spotlight indexing.
	//   This significantly improves performance for operations like cp -r by avoiding
	//   concurrent Spotlight/mds access that causes 50%+ extra VFS operations.
	// rsize/wsize=65536 (64KB) is the maximum supported by macOS NFS client.
	// Larger buffers reduce RPC overhead for large file transfers.
	cmd := exec.Command("mount_nfs",
		"-o", fmt.Sprintf("port=%d,mountport=%d,tcp,nolocks,vers=3,rsize=65536,wsize=65536,noac,soft,timeo=50,retrans=3,nobrowse", port, port),
		fmt.Sprintf("%s:/", ip),
		mountPath,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount_nfs failed: %w: %s", err, string(output))
	}

	return nil
}

// MountNetFS mounts the network filesystem (NFS version)
func MountNetFS(ip string, port int, shareName string, mountPath string) error {
	return NFSMount(ip, port, shareName, mountPath)
}

// BillyAdapter adapts LatentFS to Billy filesystem interface
type BillyAdapter struct {
	fs  *latentfs.LatentFS
	uid uint32 // cached os.Getuid() — avoids syscall per BillyFileInfo.Sys()
	gid uint32 // cached os.Getgid() — avoids syscall per BillyFileInfo.Sys()
}

// NewBillyAdapter creates a Billy adapter for LatentFS
func NewBillyAdapter(fs *latentfs.LatentFS) *BillyAdapter {
	return &BillyAdapter{
		fs:  fs,
		uid: uint32(os.Getuid()),
		gid: uint32(os.Getgid()),
	}
}

func (b *BillyAdapter) Create(filename string) (billy.File, error) {
	handle, err := b.fs.Open(filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &BillyFile{
		adapter: b,
		handle:  handle,
		name:    filename,
		flags:   os.O_CREATE | os.O_RDWR | os.O_TRUNC,
	}, nil
}

func (b *BillyAdapter) Open(filename string) (billy.File, error) {
	return b.OpenFile(filename, os.O_RDONLY, 0)
}

func (b *BillyAdapter) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	handle, err := b.fs.Open(filename, flag, int(perm))
	if err != nil {
		return nil, err
	}
	return &BillyFile{
		adapter: b,
		handle:  handle,
		name:    filename,
		flags:   flag,
	}, nil
}

func (b *BillyAdapter) Stat(filename string) (os.FileInfo, error) {
	attrs, err := b.fs.GetAttrByPath(filename)
	if err != nil {
		return nil, err
	}
	return &BillyFileInfo{
		name:    path.Base(filename),
		attrs:   attrs,
		adapter: b,
	}, nil
}

func (b *BillyAdapter) Rename(oldpath, newpath string) error {
	handle, err := b.fs.OpenAny(oldpath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer b.fs.Close(handle)
	return b.fs.Rename(handle, path.Base(newpath), 0)
}

func (b *BillyAdapter) Remove(filename string) error {
	return b.fs.UnlinkByPath(filename)
}

func (b *BillyAdapter) Join(elem ...string) string {
	return path.Join(elem...)
}

func (b *BillyAdapter) TempFile(dir, prefix string) (billy.File, error) {
	return nil, os.ErrInvalid
}

func (b *BillyAdapter) ReadDir(dirname string) ([]os.FileInfo, error) {
	handle, err := b.fs.OpenDir(dirname)
	if err != nil {
		return nil, err
	}
	defer b.fs.Close(handle)

	entries, err := b.fs.ReadDir(handle, 0, 0)
	if err != nil {
		return nil, err
	}

	var result []os.FileInfo
	for _, e := range entries {
		if e.Name == "." || e.Name == ".." {
			continue
		}
		result = append(result, &BillyFileInfo{
			name:    e.Name,
			dirInfo: &e,
			adapter: b,
		})
	}
	return result, nil
}

func (b *BillyAdapter) MkdirAll(filename string, perm os.FileMode) error {
	_, err := b.fs.Mkdir(filename, int(perm))
	return err
}

func (b *BillyAdapter) Lstat(filename string) (os.FileInfo, error) {
	// Lstat and Stat are identical for LatentFS (no symlink-following distinction at VFS level)
	attrs, err := b.fs.GetAttrByPath(filename)
	if err != nil {
		return nil, err
	}
	return &BillyFileInfo{
		name:    path.Base(filename),
		attrs:   attrs,
		adapter: b,
	}, nil
}

func (b *BillyAdapter) Symlink(target, link string) error {
	// Create the link file first
	handle, err := b.fs.Open(link, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return err
	}
	defer b.fs.Close(handle)

	_, err = b.fs.Symlink(handle, target, 0777)
	return err
}

func (b *BillyAdapter) Readlink(link string) (string, error) {
	handle, err := b.fs.Open(link, os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	defer b.fs.Close(handle)
	return b.fs.Readlink(handle)
}

func (b *BillyAdapter) Chroot(path string) (billy.Filesystem, error) {
	return nil, os.ErrInvalid
}

func (b *BillyAdapter) Root() string {
	return "/"
}

// billy.Change interface
func (b *BillyAdapter) Chmod(name string, mode os.FileMode) error {
	handle, err := b.fs.OpenAny(name, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer b.fs.Close(handle)

	attrs := latentfs.NewAttrsWithMode(uint32(mode) & 0777)
	_, err = b.fs.SetAttr(handle, attrs)
	return err
}

func (b *BillyAdapter) Lchown(name string, uid, gid int) error            { return nil }
func (b *BillyAdapter) Chown(name string, uid, gid int) error             { return nil }
func (b *BillyAdapter) Chtimes(name string, atime, mtime time.Time) error { return nil }

func (b *BillyAdapter) Capabilities() billy.Capability {
	return billy.WriteCapability | billy.ReadCapability |
		billy.ReadAndWriteCapability | billy.SeekCapability | billy.TruncateCapability
}

type BillyFile struct {
	adapter *BillyAdapter
	handle  interface{} // vfs.VfsHandle
	name    string
	flags   int
	offset  int64
}

func (f *BillyFile) Name() string {
	return f.name
}

func (f *BillyFile) Write(p []byte) (n int, err error) {
	n, err = f.adapter.fs.Write(f.handle.(vfsHandle), p, uint64(f.offset), 0)
	if err == nil {
		f.offset += int64(n)
	}
	return
}

func (f *BillyFile) Read(p []byte) (n int, err error) {
	n, err = f.adapter.fs.Read(f.handle.(vfsHandle), p, uint64(f.offset), 0)
	if err == nil {
		f.offset += int64(n)
	}
	return
}

func (f *BillyFile) ReadAt(p []byte, off int64) (n int, err error) {
	return f.adapter.fs.Read(f.handle.(vfsHandle), p, uint64(off), 0)
}

func (f *BillyFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		attrs, err := f.adapter.fs.GetAttr(f.handle.(vfsHandle))
		if err != nil {
			return 0, err
		}
		size, _ := attrs.GetSizeBytes()
		f.offset = int64(size) + offset
	}
	return f.offset, nil
}

func (f *BillyFile) Close() error {
	return f.adapter.fs.Close(f.handle.(vfsHandle))
}

func (f *BillyFile) Lock() error {
	return nil
}

func (f *BillyFile) Unlock() error {
	return nil
}

func (f *BillyFile) Truncate(size int64) error {
	return f.adapter.fs.Truncate(f.handle.(vfsHandle), uint64(size))
}

type BillyFileInfo struct {
	name    string
	attrs   interface{}   // *vfs.Attributes
	dirInfo interface{}   // *vfs.DirInfo
	adapter *BillyAdapter // cached uid/gid source (nil falls back to syscall)
}

func (fi *BillyFileInfo) Name() string {
	return fi.name
}

func (fi *BillyFileInfo) Size() int64 {
	if a := latentfs.WrapAttrs(fi.attrs); a != nil {
		size, _ := a.GetSizeBytes()
		return int64(size)
	}
	if d := latentfs.WrapDirInfo(fi.dirInfo); d != nil {
		size, _ := d.GetSizeBytes()
		return int64(size)
	}
	return 0
}

func (fi *BillyFileInfo) Mode() os.FileMode {
	// Determine base mode from file type
	var baseMode os.FileMode
	if fi.IsDir() {
		baseMode = os.ModeDir
	} else if fi.IsSymlink() {
		baseMode = os.ModeSymlink
	}

	// Try to get actual permissions from stored attributes
	if a := latentfs.WrapAttrs(fi.attrs); a != nil {
		if mode, ok := a.GetUnixMode(); ok {
			return baseMode | os.FileMode(mode&0777)
		}
	}
	if d := latentfs.WrapDirInfo(fi.dirInfo); d != nil {
		if mode, ok := d.GetUnixMode(); ok {
			return baseMode | os.FileMode(mode&0777)
		}
	}

	// Fallback to defaults if mode not available
	if fi.IsDir() {
		return os.ModeDir | 0755
	}
	if fi.IsSymlink() {
		return os.ModeSymlink | 0777
	}
	return 0644
}

func (fi *BillyFileInfo) IsSymlink() bool {
	if a := latentfs.WrapAttrs(fi.attrs); a != nil {
		return a.GetFileType() == latentfs.FileTypeSymlink
	}
	if d := latentfs.WrapDirInfo(fi.dirInfo); d != nil {
		return d.GetFileType() == latentfs.FileTypeSymlink
	}
	return false
}

func (fi *BillyFileInfo) ModTime() time.Time {
	if a := latentfs.WrapAttrs(fi.attrs); a != nil {
		t, _ := a.GetLastDataModificationTime()
		return t
	}
	return time.Now()
}

func (fi *BillyFileInfo) IsDir() bool {
	if a := latentfs.WrapAttrs(fi.attrs); a != nil {
		return a.GetFileType() == latentfs.FileTypeDirectory
	}
	if d := latentfs.WrapDirInfo(fi.dirInfo); d != nil {
		return d.GetFileType() == latentfs.FileTypeDirectory
	}
	return false
}

func (fi *BillyFileInfo) Sys() interface{} {
	// Return file.FileInfo from go-nfs/file package - this is critical for NFS to work!
	// go-nfs's GetInfo() only recognizes file.FileInfo or *file.FileInfo types
	uid, gid := fi.getUIDGID()

	if a := latentfs.WrapAttrs(fi.attrs); a != nil {
		return &nfsfile.FileInfo{
			Nlink:  1,
			UID:    uid,
			GID:    gid,
			Fileid: a.GetInodeNumber(),
		}
	}
	if d := latentfs.WrapDirInfo(fi.dirInfo); d != nil {
		return &nfsfile.FileInfo{
			Nlink:  1,
			UID:    uid,
			GID:    gid,
			Fileid: d.GetInodeNumber(),
		}
	}
	// Fallback with a default inode
	return &nfsfile.FileInfo{
		Nlink:  1,
		UID:    uid,
		GID:    gid,
		Fileid: 1,
	}
}

// getUIDGID returns cached uid/gid from the adapter if available, otherwise falls back to syscall.
func (fi *BillyFileInfo) getUIDGID() (uint32, uint32) {
	if fi.adapter != nil {
		return fi.adapter.uid, fi.adapter.gid
	}
	return uint32(os.Getuid()), uint32(os.Getgid())
}

// BillyMetaAdapter adapts MetaFS to Billy filesystem interface.
// Note: Attribute caching is now handled at the LatentFS layer with fine-grained
// invalidation. BillyMetaAdapter is a thin wrapper that delegates to MetaFS.
type BillyMetaAdapter struct {
	fs *latentfs.MetaFS
}

// NewBillyMetaAdapter creates a Billy adapter for MetaFS
func NewBillyMetaAdapter(fs *latentfs.MetaFS) *BillyMetaAdapter {
	return &BillyMetaAdapter{fs: fs}
}

func (b *BillyMetaAdapter) Create(filename string) (billy.File, error) {
	handle, err := b.fs.Open(filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &BillyMetaFile{
		adapter: b,
		handle:  handle,
		name:    filename,
		flags:   os.O_CREATE | os.O_RDWR | os.O_TRUNC,
	}, nil
}

func (b *BillyMetaAdapter) Open(filename string) (billy.File, error) {
	return b.OpenFile(filename, os.O_RDONLY, 0)
}

func (b *BillyMetaAdapter) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	handle, err := b.fs.Open(filename, flag, int(perm))
	if err != nil {
		return nil, err
	}
	return &BillyMetaFile{
		adapter: b,
		handle:  handle,
		name:    filename,
		flags:   flag,
	}, nil
}

func (b *BillyMetaAdapter) Stat(filename string) (os.FileInfo, error) {
	log.Debugf("[BillyMetaAdapter.Stat] filename=%q", filename)
	handle, err := b.fs.Open(filename, os.O_RDONLY, 0)
	if err != nil {
		handle, err = b.fs.OpenDir(filename)
		if err != nil {
			return nil, err
		}
	}
	defer b.fs.Close(handle)

	attrs, err := b.fs.GetAttr(handle)
	if err != nil {
		return nil, err
	}

	return &BillyFileInfo{
		name:  path.Base(filename),
		attrs: attrs,
	}, nil
}

func (b *BillyMetaAdapter) Rename(oldpath, newpath string) error {
	// Try as file first, then as directory
	handle, err := b.fs.Open(oldpath, os.O_RDONLY, 0)
	if err != nil {
		// Try as directory
		handle, err = b.fs.OpenDir(oldpath)
		if err != nil {
			return err
		}
	}
	defer b.fs.Close(handle)
	return b.fs.Rename(handle, path.Base(newpath), 0)
}

func (b *BillyMetaAdapter) Remove(filename string) error {
	log.Debugf("[BillyMetaAdapter.Remove] filename=%q", filename)
	// Try as file first, then as directory
	handle, err := b.fs.Open(filename, os.O_RDONLY, 0)
	if err != nil {
		log.Debugf("[BillyMetaAdapter.Remove] Open failed: %v, trying OpenDir", err)
		// Try as directory
		handle, err = b.fs.OpenDir(filename)
		if err != nil {
			log.Warnf("[BillyMetaAdapter.Remove] OpenDir also failed: %v", err)
			return err
		}
	}
	log.Debugf("[BillyMetaAdapter.Remove] handle=%d", handle)
	defer b.fs.Close(handle)
	err = b.fs.Unlink(handle)
	if err != nil {
		log.Warnf("[BillyMetaAdapter.Remove] Unlink failed: %v", err)
	}
	return err
}

func (b *BillyMetaAdapter) Join(elem ...string) string {
	return path.Join(elem...)
}

func (b *BillyMetaAdapter) TempFile(dir, prefix string) (billy.File, error) {
	return nil, os.ErrInvalid
}

func (b *BillyMetaAdapter) ReadDir(dirname string) ([]os.FileInfo, error) {
	handle, err := b.fs.OpenDir(dirname)
	if err != nil {
		return nil, err
	}
	defer b.fs.Close(handle)

	entries, err := b.fs.ReadDir(handle, 0, 0)
	if err != nil {
		return nil, err
	}

	var result []os.FileInfo
	for _, e := range entries {
		if e.Name == "." || e.Name == ".." {
			continue
		}
		result = append(result, &BillyFileInfo{
			name:    e.Name,
			dirInfo: &e,
		})
	}
	return result, nil
}

func (b *BillyMetaAdapter) MkdirAll(filename string, perm os.FileMode) error {
	_, err := b.fs.Mkdir(filename, int(perm))
	return err
}

func (b *BillyMetaAdapter) Lstat(filename string) (os.FileInfo, error) {
	// Attribute caching is now handled at the LatentFS layer with fine-grained invalidation.
	attrs, err := b.fs.GetAttrByPath(filename)
	if err != nil {
		return nil, err
	}
	return &BillyFileInfo{
		name:  path.Base(filename),
		attrs: attrs,
	}, nil
}

func (b *BillyMetaAdapter) Symlink(target, link string) error {
	handle, err := b.fs.Open(link, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return err
	}
	defer b.fs.Close(handle)

	_, err = b.fs.Symlink(handle, target, 0777)
	return err
}

func (b *BillyMetaAdapter) Readlink(link string) (string, error) {
	handle, err := b.fs.Open(link, os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	defer b.fs.Close(handle)
	return b.fs.Readlink(handle)
}

func (b *BillyMetaAdapter) Chroot(path string) (billy.Filesystem, error) {
	return nil, os.ErrInvalid
}

func (b *BillyMetaAdapter) Root() string {
	return "/"
}

// billy.Change interface
func (b *BillyMetaAdapter) Chmod(name string, mode os.FileMode) error {
	// Try to open as file first, then as directory
	handle, err := b.fs.Open(name, os.O_RDONLY, 0)
	if err != nil {
		// Try as directory
		handle, err = b.fs.OpenDir(name)
		if err != nil {
			return err
		}
	}
	defer b.fs.Close(handle)

	// Create attributes with the new mode and call SetAttr
	attrs := latentfs.NewAttrsWithMode(uint32(mode) & 0777)
	_, err = b.fs.SetAttr(handle, attrs)
	return err
}
func (b *BillyMetaAdapter) Lchown(name string, uid, gid int) error            { return nil }
func (b *BillyMetaAdapter) Chown(name string, uid, gid int) error             { return nil }
func (b *BillyMetaAdapter) Chtimes(name string, atime, mtime time.Time) error { return nil }

func (b *BillyMetaAdapter) Capabilities() billy.Capability {
	return billy.WriteCapability | billy.ReadCapability |
		billy.ReadAndWriteCapability | billy.SeekCapability | billy.TruncateCapability
}

type BillyMetaFile struct {
	adapter *BillyMetaAdapter
	handle  interface{} // vfs.VfsHandle
	name    string
	flags   int
	offset  int64
}

func (f *BillyMetaFile) Name() string { return f.name }

func (f *BillyMetaFile) Write(p []byte) (n int, err error) {
	n, err = f.adapter.fs.Write(f.handle.(vfsHandle), p, uint64(f.offset), 0)
	if err == nil {
		f.offset += int64(n)
	}
	return
}

func (f *BillyMetaFile) Read(p []byte) (n int, err error) {
	n, err = f.adapter.fs.Read(f.handle.(vfsHandle), p, uint64(f.offset), 0)
	if err == nil {
		f.offset += int64(n)
	}
	return
}

func (f *BillyMetaFile) ReadAt(p []byte, off int64) (n int, err error) {
	return f.adapter.fs.Read(f.handle.(vfsHandle), p, uint64(off), 0)
}

func (f *BillyMetaFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		attrs, err := f.adapter.fs.GetAttr(f.handle.(vfsHandle))
		if err != nil {
			return 0, err
		}
		size, _ := attrs.GetSizeBytes()
		f.offset = int64(size) + offset
	}
	return f.offset, nil
}

func (f *BillyMetaFile) Close() error {
	return f.adapter.fs.Close(f.handle.(vfsHandle))
}

func (f *BillyMetaFile) Lock() error   { return nil }
func (f *BillyMetaFile) Unlock() error { return nil }

func (f *BillyMetaFile) Truncate(size int64) error {
	return f.adapter.fs.Truncate(f.handle.(vfsHandle), uint64(size))
}

var (
	_ billy.Filesystem = (*BillyAdapter)(nil)
	_ billy.Change     = (*BillyAdapter)(nil)
	_ billy.Filesystem = (*BillyMetaAdapter)(nil)
	_ billy.Change     = (*BillyMetaAdapter)(nil)
	_ billy.File       = (*BillyFile)(nil)
	_ billy.File       = (*BillyMetaFile)(nil)
	_ context.Context  = context.Background() // just to use context import
)
