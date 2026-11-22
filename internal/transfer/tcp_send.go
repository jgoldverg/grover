package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/jgoldverg/grover/internal/wire"
)

// tcpExecutor sends a file as Parallelism byte ranges, each over a pooled
// connection. A connection that fails before the destination replied is
// discarded and the range is retried on a fresh one; errors the destination
// reports (disk full, bad path) are not retried.
type tcpExecutor struct{}

const tcpRangeAttempts = 3

func (tcpExecutor) TransferFile(ctx context.Context, t *FileTask) error {
	ranges := splitRanges(int64(t.Plan.Size), int(t.Tuning.Parallelism))
	if len(ranges) == 1 {
		return sendTCPRange(ctx, t, 1, ranges[0])
	}
	var wg sync.WaitGroup
	errs := make([]error, len(ranges))
	for i, r := range ranges {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = sendTCPRange(ctx, t, uint32(i+1), r)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func sendTCPRange(ctx context.Context, t *FileTask, streamID uint32, r byteRange) (err error) {
	stream := t.Progress.StartStream(streamID, uint64(r.offset), uint64(r.length))
	defer func() { stream.Done(err) }()
	hdr, err := wire.RangeHeader{
		JobID:        t.JobID,
		RouteID:      t.RouteID,
		Destination:  t.Destination,
		RelativePath: t.Plan.RelativePath,
		StreamID:     streamID,
		FileSize:     t.Plan.Size,
		Offset:       uint64(r.offset),
		Length:       uint64(r.length),
	}.Encode()
	if err != nil {
		return err
	}
	buf := make([]byte, chunkSize(t.Tuning))
	for attempt := 1; ; attempt++ {
		sent, err := sendRangeOnce(ctx, t, hdr, r, buf, stream)
		if err == nil {
			return nil
		}
		stream.Add(-sent) // the retry resends these bytes
		if ctx.Err() != nil || errors.Is(err, wire.ErrRemote) || attempt >= tcpRangeAttempts {
			return err
		}
		slog.Debug("retrying tcp range on a new connection", "job_id", t.JobID, "path", t.Plan.RelativePath, "stream", streamID, "attempt", attempt, "error", err)
	}
}

// sendRangeOnce returns the payload bytes it wrote, so a retry can undo them.
func sendRangeOnce(ctx context.Context, t *FileTask, hdr []byte, r byteRange, buf []byte, stream StreamProgress) (int64, error) {
	src, err := openRange(t.Plan, r.offset, r.length)
	if err != nil {
		return 0, err
	}
	defer src.Close()
	conn, err := t.Target.conns.Get(ctx)
	if err != nil {
		return 0, err
	}
	// Unblock reads/writes if the job is cancelled mid-range.
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Unix(1, 0)) })
	defer stop()

	readsDisk := !IsSynthetic(t.Plan.SourcePath)
	var sent int64
	w := &countingWriter{w: conn, onWrite: func(n int) {
		sent += int64(n)
		t.Counters.ObserveSend(n, false)
		stream.Add(int64(n))
	}}
	reader := &countingReader{r: src, onRead: func(n int) {
		if readsDisk {
			t.Counters.ObserveDiskRead(n)
		}
	}}
	err = func() error {
		if _, err := conn.Write(hdr); err != nil {
			return err
		}
		n, err := io.CopyBuffer(w, reader, buf)
		if err != nil {
			return err
		}
		if n != r.length {
			return fmt.Errorf("source file shrank: sent %d of %d bytes", n, r.length)
		}
		return wire.ReadRangeReply(conn)
	}()
	switch {
	case err == nil:
		t.Target.conns.Put(conn)
	case errors.Is(err, wire.ErrRemote):
		t.Target.conns.Put(conn) // the connection is still in sync
	default:
		t.Target.conns.Discard(conn)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}
	return sent, err
}

var _ connSource = (*dialPool)(nil)
var _ connSource = (*reversePool)(nil)
