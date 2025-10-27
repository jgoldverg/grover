package udpclient

import (
	"sync"
)

// BufferSink accumulates incoming chunks into an internal byte slice.
type BufferSink struct {
	mu   sync.Mutex
	buf  []byte
	size int64
}

func NewBufferSink(initialSize int64) *BufferSink {
	if initialSize < 0 {
		initialSize = 0
	}
	return &BufferSink{
		buf:  make([]byte, initialSize),
		size: initialSize,
	}
}

func (b *BufferSink) Callbacks() StreamCallbacks {
	return StreamCallbacks{
		OnChunk:    b.onChunk,
		OnComplete: nil,
	}
}

func (b *BufferSink) onChunk(offset uint64, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	end := int64(offset) + int64(len(data))
	if end > int64(len(b.buf)) {
		newBuf := make([]byte, end)
		copy(newBuf, b.buf)
		b.buf = newBuf
	}
	copy(b.buf[int(offset):end], data)
	if end > b.size {
		b.size = end
	}
	return nil
}

func (b *BufferSink) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf[:b.size]...)
}
