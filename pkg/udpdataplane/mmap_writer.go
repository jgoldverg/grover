package udpdataplane

import (
	"fmt"
	"os"
	"syscall"
)

// MmapWriter provides an io.WriterAt backed by a memory-mapped file.
// Call Close when finished so the mapping is flushed and released.
type MmapWriter struct {
	file *os.File
	data []byte
}

// NewMmapWriter maps the provided file with at least length bytes available.
func NewMmapWriter(f *os.File, length int64) (*MmapWriter, error) {
	if f == nil {
		return nil, fmt.Errorf("nil file for mmap writer")
	}
	if length <= 0 {
		return nil, fmt.Errorf("mmap length must be positive")
	}
	if err := f.Truncate(length); err != nil {
		return nil, err
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(length), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &MmapWriter{file: f, data: data}, nil
}

// WriteAt copies p into the mapped region at the requested offset.
func (m *MmapWriter) WriteAt(p []byte, off int64) (int, error) {
	if m == nil || len(m.data) == 0 {
		return 0, fmt.Errorf("mmap writer not initialized")
	}
	if off < 0 {
		return 0, fmt.Errorf("negative offset %d", off)
	}
	end := off + int64(len(p))
	if end > int64(len(m.data)) {
		return 0, fmt.Errorf("write exceeds mapped region: end=%d cap=%d", end, len(m.data))
	}
	copy(m.data[off:end], p)
	return len(p), nil
}

// Close flushes and unmaps the region. The underlying file remains open.
func (m *MmapWriter) Close() error {
	if m == nil {
		return nil
	}
	var err error
	if len(m.data) > 0 {
		err = syscall.Munmap(m.data)
		m.data = nil
	}
	return err
}
