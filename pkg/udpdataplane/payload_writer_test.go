package udpdataplane

import (
	"errors"
	"io"
	"testing"
)

type recordingWriterAt struct {
	writes []payloadWrite
	data   map[uint64][]byte
	err    error
	short  bool
}

type payloadWrite struct {
	offset uint64
	data   string
}

func newRecordingWriterAt() *recordingWriterAt {
	return &recordingWriterAt{data: make(map[uint64][]byte)}
}

func (w *recordingWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.short {
		return len(p) - 1, nil
	}
	cp := append([]byte(nil), p...)
	w.writes = append(w.writes, payloadWrite{offset: uint64(off), data: string(cp)})
	w.data[uint64(off)] = cp
	return len(p), nil
}

func TestDirectPayloadWriterWritesAndTracksCompletion(t *testing.T) {
	dst := newRecordingWriterAt()
	writer, err := newDirectPayloadWriter(dst, 10)
	if err != nil {
		t.Fatal(err)
	}

	added, done, err := writer.WritePayload(0, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if added != 5 || done {
		t.Fatalf("first write added=%d done=%v, want 5 false", added, done)
	}

	added, done, err = writer.WritePayload(5, []byte("world"))
	if err != nil {
		t.Fatal(err)
	}
	if added != 5 || !done {
		t.Fatalf("second write added=%d done=%v, want 5 true", added, done)
	}
	if writer.DurableBytes() != 10 {
		t.Fatalf("durable bytes = %d, want 10", writer.DurableBytes())
	}
	if len(dst.writes) != 2 {
		t.Fatalf("writes = %d, want 2", len(dst.writes))
	}
}

func TestDirectPayloadWriterDedupesCoverage(t *testing.T) {
	dst := newRecordingWriterAt()
	writer, err := newDirectPayloadWriter(dst, 10)
	if err != nil {
		t.Fatal(err)
	}

	if added, _, err := writer.WritePayload(0, []byte("hello")); err != nil || added != 5 {
		t.Fatalf("initial write added=%d err=%v, want 5 nil", added, err)
	}
	if added, done, err := writer.WritePayload(0, []byte("hello")); err != nil || added != 0 || done {
		t.Fatalf("duplicate write added=%d done=%v err=%v, want 0 false nil", added, done, err)
	}
	if writer.DurableBytes() != 5 {
		t.Fatalf("durable bytes = %d, want 5", writer.DurableBytes())
	}
}

func TestDirectPayloadWriterHandlesOutOfOrderCoverage(t *testing.T) {
	dst := newRecordingWriterAt()
	writer, err := newDirectPayloadWriter(dst, 10)
	if err != nil {
		t.Fatal(err)
	}

	added, done, err := writer.WritePayload(5, []byte("world"))
	if err != nil {
		t.Fatal(err)
	}
	if added != 5 || done {
		t.Fatalf("out-of-order write added=%d done=%v, want 5 false", added, done)
	}

	added, done, err = writer.WritePayload(0, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if added != 5 || !done {
		t.Fatalf("hole-filling write added=%d done=%v, want 5 true", added, done)
	}
}

func TestDirectPayloadWriterPropagatesWriteErrors(t *testing.T) {
	want := errors.New("disk failed")
	dst := newRecordingWriterAt()
	dst.err = want
	writer, err := newDirectPayloadWriter(dst, 10)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = writer.WritePayload(0, []byte("hello"))
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want wrapping %v", err, want)
	}
}

func TestDirectPayloadWriterDetectsShortWrites(t *testing.T) {
	dst := newRecordingWriterAt()
	dst.short = true
	writer, err := newDirectPayloadWriter(dst, 10)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = writer.WritePayload(0, []byte("hello"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("error = %v, want short write", err)
	}
}

func TestCoalescingPayloadWriterFlushesContiguousPayloads(t *testing.T) {
	dst := newRecordingWriterAt()
	writer, err := newCoalescingPayloadWriter(dst, 12, 12, 64)
	if err != nil {
		t.Fatal(err)
	}

	if added, done, err := writer.WritePayload(0, []byte("hello ")); err != nil || added != 6 || done {
		t.Fatalf("first write added=%d done=%v err=%v, want 6 false nil", added, done, err)
	}
	if len(dst.writes) != 0 {
		t.Fatalf("writes = %d, want 0 before flush threshold", len(dst.writes))
	}
	if added, done, err := writer.WritePayload(6, []byte("world!")); err != nil || added != 6 || !done {
		t.Fatalf("second write added=%d done=%v err=%v, want 6 true nil", added, done, err)
	}
	if len(dst.writes) != 1 {
		t.Fatalf("writes = %d, want 1 coalesced write", len(dst.writes))
	}
	if dst.writes[0].offset != 0 || dst.writes[0].data != "hello world!" {
		t.Fatalf("write = %+v, want offset=0 data=hello world!", dst.writes[0])
	}
	if writer.DurableBytes() != 12 {
		t.Fatalf("durable bytes = %d, want 12", writer.DurableBytes())
	}
}

func TestCoalescingPayloadWriterBuffersOutOfOrderUntilHoleArrives(t *testing.T) {
	dst := newRecordingWriterAt()
	writer, err := newCoalescingPayloadWriter(dst, 12, 12, 64)
	if err != nil {
		t.Fatal(err)
	}

	if added, done, err := writer.WritePayload(6, []byte("world!")); err != nil || added != 6 || done {
		t.Fatalf("out-of-order write added=%d done=%v err=%v, want 6 false nil", added, done, err)
	}
	if len(dst.writes) != 0 {
		t.Fatalf("writes = %d, want 0 before contiguous data", len(dst.writes))
	}
	if added, done, err := writer.WritePayload(0, []byte("hello ")); err != nil || added != 6 || !done {
		t.Fatalf("hole write added=%d done=%v err=%v, want 6 true nil", added, done, err)
	}
	if len(dst.writes) != 1 {
		t.Fatalf("writes = %d, want 1", len(dst.writes))
	}
	if dst.writes[0].data != "hello world!" {
		t.Fatalf("write data = %q, want hello world!", dst.writes[0].data)
	}
}

func TestCoalescingPayloadWriterFlushesThresholdSizedChunks(t *testing.T) {
	dst := newRecordingWriterAt()
	writer, err := newCoalescingPayloadWriter(dst, 16, 8, 64)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := writer.WritePayload(0, []byte("abcdefgh")); err != nil {
		t.Fatal(err)
	}
	if len(dst.writes) != 1 || dst.writes[0].data != "abcdefgh" {
		t.Fatalf("writes = %+v, want one 8 byte flush", dst.writes)
	}
	if _, done, err := writer.WritePayload(8, []byte("ijklmnop")); err != nil || !done {
		t.Fatalf("second write done=%v err=%v, want true nil", done, err)
	}
	if len(dst.writes) != 2 || dst.writes[1].data != "ijklmnop" {
		t.Fatalf("writes = %+v, want second 8 byte flush", dst.writes)
	}
}

func TestCoalescingPayloadWriterRejectsUnboundedBufferGrowth(t *testing.T) {
	dst := newRecordingWriterAt()
	writer, err := newCoalescingPayloadWriter(dst, 128, 64, 64)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := writer.WritePayload(64, []byte("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm")); err == nil {
		t.Fatal("expected buffer limit error")
	}
}
