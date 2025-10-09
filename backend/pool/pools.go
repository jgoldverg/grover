package pool

import "sync"

type BufferPool struct {
	bufferPool sync.Pool
}

func NewBufferPool(bufferSize uint64) *BufferPool {
	return &BufferPool{
		bufferPool: sync.Pool{
			New: func() any {
				return make([]byte, 0, bufferSize)
			},
		},
	}
}

func (bp *BufferPool) GetBuffer() []byte {
	return bp.bufferPool.Get().([]byte)
}

func (bp *BufferPool) PutBuffer(buffer []byte) {
	bp.bufferPool.Put(buffer)
}
