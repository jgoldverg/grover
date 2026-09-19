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

type coalescingPayloadWriter struct {
	dst              io.WriterAt
	progress         *rangeTracker
	flushOffset      uint64
	durable          uint64
	buffered         uint64
	flushBytes       uint64
	maxBufferedBytes uint64
	pending          []payloadSegment
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
	if maxBufferedBytes == 0 {
		maxBufferedBytes = flushBytes * 4
	}
	if maxBufferedBytes < flushBytes {
		maxBufferedBytes = flushBytes
	}
	return &coalescingPayloadWriter{
		dst:              dst,
		progress:         newRangeTracker(expected),
		flushBytes:       flushBytes,
		maxBufferedBytes: maxBufferedBytes,
		pending:          make([]payloadSegment, 0, 16),
	}, nil
}

func (w *coalescingPayloadWriter) WritePayload(offset uint64, payload []byte) (uint64, bool, error) {
	if w == nil {
		return 0, false, fmt.Errorf("nil payload writer")
	}
	if len(payload) == 0 {
		done := w.progress != nil && w.progress.complete()
		return 0, done, nil
	}
	end := offset + uint64(len(payload))
	if end <= w.flushOffset {
		return 0, w.complete(), nil
	}
	if offset < w.flushOffset {
		payload = payload[w.flushOffset-offset:]
		offset = w.flushOffset
		end = offset + uint64(len(payload))
	}

	added := uint64(0)
	done := false
	if w.progress != nil {
		added = w.progress.addCovered(offset, end)
		done = w.progress.complete()
	} else if end > w.durable {
		added = uint64(len(payload))
	}
	if added == 0 {
		return 0, done, nil
	}

	w.insertSegment(offset, payload)
	if w.buffered > w.maxBufferedBytes {
		return added, done, fmt.Errorf("udp receive write buffer exceeded: buffered=%d max=%d", w.buffered, w.maxBufferedBytes)
	}
	if done || w.contiguousBufferedBytes() >= w.flushBytes {
		if err := w.flushContiguous(done); err != nil {
			return added, done, err
		}
	}
	return added, done, nil
}

func (w *coalescingPayloadWriter) Flush() error {
	if w == nil {
		return nil
	}
	return w.flushContiguous(true)
}

func (w *coalescingPayloadWriter) DurableBytes() uint64 {
	if w == nil {
		return 0
	}
	if w.progress != nil {
		return minUint64(w.durable, w.progress.covered)
	}
	return w.durable
}

func (w *coalescingPayloadWriter) complete() bool {
	return w.progress != nil && w.progress.complete()
}

func (w *coalescingPayloadWriter) insertSegment(offset uint64, payload []byte) {
	cp := slices.Clone(payload)
	next := payloadSegment{start: offset, data: cp}
	inserted := false
	segments := make([]payloadSegment, 0, len(w.pending)+1)
	for _, cur := range w.pending {
		if cur.end() < next.start {
			segments = append(segments, cur)
			continue
		}
		if next.end() < cur.start {
			if !inserted {
				segments = append(segments, next)
				inserted = true
			}
			segments = append(segments, cur)
			continue
		}
		next = mergePayloadSegments(next, cur)
	}
	if !inserted {
		segments = append(segments, next)
	}
	w.pending = segments
	w.recountBuffered()
}

func mergePayloadSegments(a, b payloadSegment) payloadSegment {
	start := minUint64(a.start, b.start)
	end := maxUint64(a.end(), b.end())
	merged := make([]byte, end-start)
	copy(merged[a.start-start:], a.data)
	copy(merged[b.start-start:], b.data)
	return payloadSegment{start: start, data: merged}
}

func (w *coalescingPayloadWriter) contiguousBufferedBytes() uint64 {
	if len(w.pending) == 0 || w.pending[0].start > w.flushOffset {
		return 0
	}
	end := w.flushOffset
	for _, seg := range w.pending {
		if seg.start > end {
			break
		}
		if seg.end() > end {
			end = seg.end()
		}
	}
	return end - w.flushOffset
}

func (w *coalescingPayloadWriter) flushContiguous(flushAll bool) error {
	for len(w.pending) > 0 {
		seg := w.pending[0]
		if seg.start > w.flushOffset {
			return nil
		}
		if seg.end() <= w.flushOffset {
			w.dropFirstSegment()
			continue
		}
		startInSegment := w.flushOffset - seg.start
		available := uint64(len(seg.data)) - startInSegment
		if !flushAll && available < w.flushBytes {
			return nil
		}
		toWrite := available
		if !flushAll {
			toWrite = (available / w.flushBytes) * w.flushBytes
		}
		if toWrite == 0 {
			return nil
		}
		payload := seg.data[startInSegment : startInSegment+toWrite]
		n, err := w.dst.WriteAt(payload, int64(w.flushOffset))
		if err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
		if n != len(payload) {
			return io.ErrShortWrite
		}
		w.flushOffset += uint64(n)
		w.durable = w.flushOffset
		if w.flushOffset >= seg.end() {
			w.dropFirstSegment()
			continue
		}
		w.pending[0].data = seg.data[w.flushOffset-seg.start:]
		w.pending[0].start = w.flushOffset
		w.recountBuffered()
	}
	return nil
}

func (w *coalescingPayloadWriter) dropFirstSegment() {
	if len(w.pending) == 0 {
		return
	}
	w.buffered -= uint64(len(w.pending[0].data))
	copy(w.pending, w.pending[1:])
	w.pending = w.pending[:len(w.pending)-1]
}

func (w *coalescingPayloadWriter) recountBuffered() {
	var total uint64
	for _, seg := range w.pending {
		total += uint64(len(seg.data))
	}
	w.buffered = total
}
