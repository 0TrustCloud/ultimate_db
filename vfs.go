package ultimate_db

import (
	"io"
	"os"
	"sync"
	"time"
)

// ============================================================================
// 1. Production Disk Implementation (OS File System)
// ============================================================================

// OSFileDevice implements BlockDevice using actual operating system files.
type OSFileDevice struct {
	file *os.File
	path string
}

// NewOSFileDevice initializes a file-backed physical block device.
func NewOSFileDevice(path string) (*OSFileDevice, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	return &OSFileDevice{file: file, path: path}, nil
}

// Path returns the absolute or relative filesystem path used for this device.
func (d *OSFileDevice) Path() string {
	if d == nil {
		return ""
	}
	return d.path
}

func (d *OSFileDevice) ReadAt(p []byte, off int64) (int, error) {
	return d.file.ReadAt(p, off)
}

func (d *OSFileDevice) WriteAt(p []byte, off int64) (int, error) {
	return d.file.WriteAt(p, off)
}

func (d *OSFileDevice) Sync() error {
	return d.file.Sync()
}

func (d *OSFileDevice) Stat() (os.FileInfo, error) {
	return d.file.Stat()
}

func (d *OSFileDevice) Close() error {
	return d.file.Close()
}

// ============================================================================
// 2. High-Performance Testing Implementation (In-Memory Virtual Disk)
// ============================================================================

// MemDevice implements a fully concurrent virtual BlockDevice in memory.
type MemDevice struct {
	mu dataMutex
}

type dataMutex struct {
	sync.RWMutex
	data []byte
}

// NewMemDevice initializes an empty, virtualized block storage device.
func NewMemDevice() *MemDevice {
	return &MemDevice{
		mu: dataMutex{data: make([]byte, 0)},
	}
}

func (m *MemDevice) ReadAt(p []byte, off int64) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if off >= int64(len(m.mu.data)) {
		return 0, io.EOF
	}

	n := copy(p, m.mu.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *MemDevice) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	endOffset := off + int64(len(p))
	// If writing beyond the current size, grow the internal byte matrix dynamically
	if endOffset > int64(len(m.mu.data)) {
		newData := make([]byte, endOffset)
		copy(newData, m.mu.data)
		m.mu.data = newData
	}

	n := copy(m.mu.data[off:], p)
	return n, nil
}

func (m *MemDevice) Sync() error {
	// In-memory writes are instantly modified in the runtime space; no physical sync required.
	return nil
}

func (m *MemDevice) Stat() (os.FileInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return &memFileInfo{size: int64(len(m.mu.data))}, nil
}

func (m *MemDevice) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.data = nil
	return nil
}

// ============================================================================
// 3. Virtual File Information Companion
// ============================================================================

// memFileInfo satisfies the standard os.FileInfo interface for virtual file descriptors.
type memFileInfo struct {
	size int64
}

func (f *memFileInfo) Name() string       { return "memory_device.db" }
func (f *memFileInfo) Size() int64        { return f.size }
func (f *memFileInfo) Mode() os.FileMode  { return os.ModeTemporary }
func (f *memFileInfo) ModTime() time.Time { return time.Now() }
func (f *memFileInfo) IsDir() bool        { return false }
func (f *memFileInfo) Sys() any          { return nil }
