package transfer

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/jgoldverg/grover/internal/metrics"
	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/tuning"
)

// FileTask is everything an executor needs to move one file. Tuning is a
// snapshot taken when the file starts; later updates apply to later files.
type FileTask struct {
	JobID       string
	RouteID     string
	Plan        FilePlan
	Destination string  // absolute directory on the destination node
	Target      *target // nil means copy locally on this node
	Tuning      tuning.Tuning
	Counters    *metrics.Counters
	Progress    Progress
}

// Progress receives per-stream progress for one file.
type Progress interface {
	StartStream(id uint32, offset, size uint64) StreamProgress
}

// StreamProgress tracks one stream. Add may be negative to undo bytes from a
// failed attempt that is being retried.
type StreamProgress interface {
	Add(n int64)
	Done(err error)
}

// FileExecutor moves one file. Executors own protocol details; the Manager
// owns scheduling, state, and history.
type FileExecutor interface {
	TransferFile(ctx context.Context, task *FileTask) error
}

// target is where a session's data goes. Exactly one of dial/reverse is used
// for TCP; UDP always sends to data.
type target struct {
	protocol pb.DataProtocol
	data     *pb.DataEndpoint // nil for destination-origin sessions
	token    []byte
	conns    connSource // TCP connections (dialed or destination-originated)
}

// dispatchExecutor picks the executor for a task.
type dispatchExecutor struct {
	local, tcp, udp FileExecutor
}

func defaultExecutor() FileExecutor {
	return dispatchExecutor{local: localExecutor{}, tcp: tcpExecutor{}, udp: udpExecutor{}}
}

func (d dispatchExecutor) TransferFile(ctx context.Context, t *FileTask) error {
	switch {
	case t.Target == nil:
		return d.local.TransferFile(ctx, t)
	case t.Target.protocol == pb.DataProtocol_DATA_PROTOCOL_UDP:
		return d.udp.TransferFile(ctx, t)
	default:
		return d.tcp.TransferFile(ctx, t)
	}
}

// localExecutor copies a file between two paths on this node.
type localExecutor struct{}

func (localExecutor) TransferFile(ctx context.Context, t *FileTask) (err error) {
	stream := t.Progress.StartStream(1, 0, t.Plan.Size)
	defer func() { stream.Done(err) }()
	dstPath, err := DestinationPath(t.Destination, t.Plan.RelativePath)
	if err != nil {
		return err
	}
	src, err := openRange(t.Plan, 0, int64(t.Plan.Size))
	if err != nil {
		return err
	}
	defer src.Close()
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	readsDisk := !IsSynthetic(t.Plan.SourcePath)
	w := &countingWriter{w: dst, onWrite: func(n int) {
		if readsDisk {
			t.Counters.ObserveDiskRead(n)
		}
		t.Counters.ObserveDiskWrite(n)
		stream.Add(int64(n))
	}}
	if _, err := copyWithContext(ctx, w, src, make([]byte, chunkSize(t.Tuning))); err != nil {
		_ = dst.Close()
		return err
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		return err
	}
	return dst.Close()
}

func chunkSize(t tuning.Tuning) int {
	if t.ChunkSizeBytes == 0 {
		return int(tuning.Defaults().ChunkSizeBytes)
	}
	return int(t.ChunkSizeBytes)
}

// copyWithContext is io.CopyBuffer that stops when ctx is cancelled.
func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader, buf []byte) (int64, error) {
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, rerr := src.Read(buf)
		if n > 0 {
			w, werr := dst.Write(buf[:n])
			total += int64(w)
			if werr != nil {
				return total, werr
			}
			if w != n {
				return total, io.ErrShortWrite
			}
		}
		if errors.Is(rerr, io.EOF) {
			return total, nil
		}
		if rerr != nil {
			return total, rerr
		}
	}
}

type countingWriter struct {
	w       io.Writer
	onWrite func(int)
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 {
		c.onWrite(n)
	}
	return n, err
}

type countingReader struct {
	r      io.Reader
	onRead func(int)
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.onRead(n)
	}
	return n, err
}
