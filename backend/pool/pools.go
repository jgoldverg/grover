package pool

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

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

type WorkerPool[T any] struct {
	ingressChan chan T
	errorChan   chan error

	wg sync.WaitGroup

	maxWorker   int
	concurrency atomic.Int32

	resizeMu sync.Mutex
	resizeCh chan struct{}

	pauseAll atomic.Bool
	pauseMu  sync.Mutex
	pauseCh  chan struct{}
}

func NewWorkerPool[T any](maxWorkers, initial int) *WorkerPool[T] {
	if maxWorkers <= 0 {
		maxWorkers = runtime.NumCPU() * 2
	}
	if initial < 0 {
		initial = 0
	}
	wp := &WorkerPool[T]{
		ingressChan: make(chan T, 64),
		errorChan:   make(chan error, 64),
		maxWorker:   maxWorkers,
		resizeCh:    make(chan struct{}),
		pauseCh:     make(chan struct{}),
	}
	close(wp.pauseCh) // start unpaused
	wp.concurrency.Store(int32(initial))
	return wp
}

func (wp *WorkerPool[T]) SetSize(n int) {
	if n < 0 {
		n = 0
	}
	wp.concurrency.Store(int32(n))
	wp.resizeMu.Lock()
	close(wp.resizeCh)
	wp.resizeCh = make(chan struct{})
	wp.resizeMu.Unlock()
}

func (wp *WorkerPool[T]) Pause() {
	if wp.pauseAll.Swap(true) {
		return
	}
}

func (wp *WorkerPool[T]) Resume() {
	if !wp.pauseAll.Swap(false) {
		return
	}
	wp.pauseMu.Lock()
	close(wp.pauseCh)
	wp.pauseCh = make(chan struct{})
	wp.pauseMu.Unlock()
}

func (wp *WorkerPool[T]) Run(ctx context.Context, handler func(context.Context, T)) {
	for i := 0; i < wp.maxWorker; i++ {
		wp.startWorker(ctx, i, handler)
	}
	wp.wg.Wait()
}

func (wp *WorkerPool[T]) startWorker(ctx context.Context, id int, handler func(context.Context, T)) {
	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		for {
			for int32(id) >= wp.concurrency.Load() {
				wp.resizeMu.Lock()
				ch := wp.resizeCh
				wp.resizeMu.Unlock()
				select {
				case <-ctx.Done():
					return
				case <-ch:
				}
			}

			for wp.pauseAll.Load() {
				wp.pauseMu.Lock()
				pz := wp.pauseCh
				wp.pauseMu.Unlock()
				select {
				case <-ctx.Done():
					return
				case <-pz:
				}
			}

			select {
			case <-ctx.Done():
				return
			case task, ok := <-wp.ingressChan:
				if !ok {
					return
				}
				handler(ctx, task)
			}
		}
	}()
}

func (wp *WorkerPool[T]) Ingress() chan<- T {
	return wp.ingressChan
}

func (wp *WorkerPool[T]) CloseIngress() {
	close(wp.ingressChan)
}

func (wp *WorkerPool[T]) Errors() <-chan error {
	return wp.errorChan
}

func (wp *WorkerPool[T]) PublishError(err error) {
	select {
	case wp.errorChan <- err:
	default:
	}
}
