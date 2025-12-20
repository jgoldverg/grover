package gclient

import (
	"context"
	"io"
	"sync"
)

type RingBuffer struct {
	buf      []txPacket
	head     int
	size     int
	mu       sync.Mutex
	notFull  *sync.Cond
	notEmpty *sync.Cond
	closed   bool
}

func NewRingBuffer(size int) *RingBuffer {
	rb := RingBuffer{
		buf:    make([]txPacket, size),
		closed: false,
	}
	rb.size = 0
	rb.head = 0
	rb.notFull = sync.NewCond(&rb.mu)
	rb.notEmpty = sync.NewCond(&rb.mu)
	return &rb
}

func (rb *RingBuffer) Enqueue(ctx context.Context, pkt txPacket) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for !rb.closed && rb.size == len(rb.buf) {
		if err := ctx.Err(); err != nil {
			return err
		}
		rb.notFull.Wait()
	}
	if rb.closed {
		return io.EOF
	}
	idx := (rb.head + rb.size) % len(rb.buf)
	rb.buf[idx] = pkt
	rb.size++
	rb.notEmpty.Signal()

	return nil
}

func (rb *RingBuffer) Dequeue(ctx context.Context) (*txPacket, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for rb.size == 0 {
		if rb.closed {
			return nil, io.EOF
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rb.notEmpty.Wait()
	}

	pkt := rb.buf[rb.head]
	rb.head = (rb.head + 1) % len(rb.buf)
	rb.size--
	rb.notFull.Signal()
	return &pkt, nil
}

func (rb *RingBuffer) Close() {
	rb.mu.Lock()

	if rb.closed {
		rb.mu.Unlock()
		return
	}
	rb.closed = true

	rb.mu.Unlock()
	rb.notFull.Broadcast()
	rb.notEmpty.Broadcast()
}
