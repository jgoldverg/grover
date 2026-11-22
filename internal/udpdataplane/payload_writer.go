package udpdataplane

import (
	"fmt"
	"io"
	"slices"
)

// PayloadWriter is the boundary between UDP packet handling and durable payload
// writes. The receiver owns packet decode/ACK state; writers own byte-range
// accounting and filesystem behavior.
type PayloadWriter interface {
	WritePayload(offset uint64, payload []byte) (newBytes uint64, done bool, err error)
	Flush() error
	DurableBytes() uint64
}

type directPayloadWriter struct {
	dst      io.WriterAt
	progress *rangeTracker
	total    uint64
}

func newDirectPayloadWriter(dst io.WriterAt, expected uint64) (*directPayloadWriter, error) {
	if dst == nil {
		return nil, fmt.Errorf("nil destination writer")
	}
	return &directPayloadWriter{
		dst:      dst,
		progress: newRangeTracker(expected),
	}, nil
}

func (w *directPayloadWriter) WritePayload(offset uint64, payload []byte) (uint64, bool, error) {
	if w == nil {
		return 0, false, fmt.Errorf("nil payload writer")
	}
	payloadLen := len(payload)
	if payloadLen > 0 {
		n, err := w.dst.WriteAt(payload, int64(offset))
		if err != nil {
			return 0, false, fmt.Errorf("write payload: %w", err)
		}
		if n != payloadLen {
			return 0, false, io.ErrShortWrite
		}
	}

	end := offset + uint64(payloadLen)
	added := uint64(0)
	done := false
	if w.progress != nil {
		added = w.progress.addCovered(offset, end)
		done = w.progress.complete()
		w.total = w.progress.covered
	} else if end > w.total {
		added = uint64(payloadLen)
		w.total = end
	}
	return added, done, nil
}

func (w *directPayloadWriter) Flush() error {
	return nil
}

func (w *directPayloadWriter) DurableBytes() uint64 {
	if w == nil {
		return 0
	}
	return w.total
}

type payloadSegment struct {
	start uint64
	data  []byte
}

func (s payloadSegment) end() uint64 {
	return s.start + uint64(len(s.data))
}

// coalescingPayloadWriter batches small packet payloads into larger WriteAt
// calls. Segments are written as soon as they reach flushBytes, wherever they
// sit in the file. If out-of-order data piles up past maxBufferedBytes, every
// buffered segment is written immediately; loss never turns into an error.
type coalescingPayloadWriter struct {
	dst              io.WriterAt
	progress         *rangeTracker // bytes accepted
	flushed          *rangeTracker // bytes written to dst
	flushedBytes     uint64        // used when the expected size is unknown
	buffered         uint64
	flushBytes       uint64
	maxBufferedBytes uint64
	pending          []payloadSegment // sorted, non-overlapping
}

func newPayloadWriter(dst io.WriterAt, expected, flushBytes, maxBufferedBytes uint64) (PayloadWriter, error) {
	if flushBytes == 0 {
		return newDirectPayloadWriter(dst, expected)
	}
	return newCoalescingPayloadWriter(dst, expected, flushBytes, maxBufferedBytes)
}

func newCoalescingPayloadWriter(dst io.WriterAt, expected, flushBytes, maxBufferedBytes uint64) (*coalescingPayloadWriter, error) {
	if dst == nil {
		return nil, fmt.Errorf("nil destination writer")
	}
	if flushBytes == 0 {
		return nil, fmt.Errorf("flush bytes must be greater than zero")
	}
	if maxBufferedBytes < flushBytes*8 {
		maxBufferedBytes = flushBytes * 8
	}
	return &coalescingPayloadWriter{
		dst:              dst,
		progress:         newRangeTracker(expected),
		flushed:          newRangeTracker(expected),
		flushBytes:       flushBytes,
		maxBufferedBytes: maxBufferedBytes,
	}, nil
}

func (w *coalescingPayloadWriter) WritePayload(offset uint64, payload []byte) (uint64, bool, error) {
	if len(payload) == 0 {
		return 0, w.complete(), nil
	}
	end := offset + uint64(len(payload))
	var added uint64
	if w.progress != nil {
		added = w.progress.addCovered(offset, end)
	} else {
		added = uint64(len(payload))
	}
	done := w.complete()
	if added == 0 {
		return 0, done, nil
	}
	idx := w.insert(offset, payload)
	switch {
	case done || w.buffered > w.maxBufferedBytes:
		if err := w.flushAll(); err != nil {
			return added, done, err
		}
	case uint64(len(w.pending[idx].data)) >= w.flushBytes:
		if err := w.flushSegment(idx); err != nil {
			return added, done, err
		}
	}
	return added, done, nil
}

func (w *coalescingPayloadWriter) Flush() error { return w.flushAll() }

func (w *coalescingPayloadWriter) DurableBytes() uint64 {
	if w.flushed != nil {
		return w.flushed.covered
	}
	return w.flushedBytes
}

func (w *coalescingPayloadWriter) complete() bool {
	return w.progress != nil && w.progress.complete()
}

// insert adds a payload, merging it with touching or overlapping segments,
// and returns the index of the resulting segment.
func (w *coalescingPayloadWriter) insert(offset uint64, payload []byte) int {
	seg := payloadSegment{start: offset, data: slices.Clone(payload)}
	i := 0
	for i < len(w.pending) && w.pending[i].end() < seg.start {
		i++
	}
	j := i
	for j < len(w.pending) && w.pending[j].start <= seg.end() {
		seg = mergePayloadSegments(w.pending[j], seg)
		w.buffered -= uint64(len(w.pending[j].data))
		j++
	}
	w.pending = slices.Replace(w.pending, i, j, seg)
	w.buffered += uint64(len(seg.data))
	return i
}

// mergePayloadSegments overlays b on a; b wins where they overlap.
func mergePayloadSegments(a, b payloadSegment) payloadSegment {
	start := minUint64(a.start, b.start)
	end := maxUint64(a.end(), b.end())
	merged := make([]byte, end-start)
	copy(merged[a.start-start:], a.data)
	copy(merged[b.start-start:], b.data)
	return payloadSegment{start: start, data: merged}
}

func (w *coalescingPayloadWriter) flushSegment(i int) error {
	seg := w.pending[i]
	n, err := w.dst.WriteAt(seg.data, int64(seg.start))
	if err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	if n != len(seg.data) {
		return io.ErrShortWrite
	}
	if w.flushed != nil {
		w.flushed.addCovered(seg.start, seg.end())
	} else {
		w.flushedBytes += uint64(n)
	}
	w.buffered -= uint64(len(seg.data))
	w.pending = slices.Delete(w.pending, i, i+1)
	return nil
}

func (w *coalescingPayloadWriter) flushAll() error {
	for len(w.pending) > 0 {
		if err := w.flushSegment(0); err != nil {
			return err
		}
	}
	return nil
}
