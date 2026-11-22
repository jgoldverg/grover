package transfer

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/wire"
)

var copyBufPool = sync.Pool{New: func() any { b := make([]byte, 1<<20); return &b }}

// serveRanges receives ranges on an already-authenticated connection until
// the source closes it. Errors that leave the stream in sync (bad path, disk
// errors) are reported back and the connection stays usable; anything else
// closes it.
func (r *receiver) serveRanges(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReaderSize(conn, 256<<10)
	for {
		h, err := wire.ReadRangeHeader(br)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Debug("tcp data connection closed", "remote", conn.RemoteAddr().String(), "error", err)
			}
			return
		}
		rangeErr, connErr := r.receiveRange(br, h)
		if connErr != nil {
			slog.Warn("tcp receive failed", "job_id", h.JobID, "path", h.RelativePath, "stream", h.StreamID, "remote", conn.RemoteAddr().String(), "error", connErr)
			return
		}
		if rangeErr != nil {
			slog.Warn("tcp range rejected", "job_id", h.JobID, "path", h.RelativePath, "stream", h.StreamID, "error", rangeErr)
		}
		if err := wire.WriteRangeReply(conn, rangeErr); err != nil {
			return
		}
	}
}

// receiveRange consumes exactly h.Length payload bytes. rangeErr is a
// per-range failure to report to the source; connErr means the connection
// can no longer be trusted.
func (r *receiver) receiveRange(src io.Reader, h wire.RangeHeader) (rangeErr, connErr error) {
	handle := r.begin(receiveInfo{
		JobID: h.JobID, RouteID: h.RouteID, Destination: h.Destination, RelativePath: h.RelativePath,
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP, FileSize: h.FileSize, StreamID: h.StreamID,
		Offset: h.Offset, Length: h.Length,
	})
	payload := io.LimitReader(src, int64(h.Length))
	f, err := openDestination(h.Destination, h.RelativePath, h.FileSize)
	if err != nil {
		handle.Done(err)
		if _, derr := io.Copy(io.Discard, payload); derr != nil {
			return err, derr
		}
		return err, nil
	}
	bufp := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bufp)
	w := &countingWriter{w: fileErrWriter{io.NewOffsetWriter(f, int64(h.Offset))}, onWrite: handle.Add}
	n, copyErr := io.CopyBuffer(w, payload, *bufp)
	if copyErr == nil && n != int64(h.Length) {
		copyErr = io.ErrUnexpectedEOF
	}
	if copyErr != nil {
		_ = f.Close()
		// A write error leaves unread payload; drain it to stay in sync.
		var werr *writeError
		if errors.As(copyErr, &werr) {
			if _, derr := io.Copy(io.Discard, payload); derr == nil {
				handle.Done(werr.err)
				return werr.err, nil
			}
		}
		handle.Done(copyErr)
		return nil, copyErr
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	handle.Done(err)
	return err, nil
}

// openDestination opens (creating as needed) the destination file and sizes it.
func openDestination(destination, rel string, size uint64) (*os.File, error) {
	path, err := DestinationPath(destination, rel)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	// Every stream of a file sets the final size; extending or re-setting the
	// same size never discards bytes another stream already wrote.
	if info, err := f.Stat(); err == nil && uint64(info.Size()) != size {
		if err := f.Truncate(int64(size)); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("size %s: %w", path, err)
		}
	}
	return f, nil
}

// writeError marks failures on the destination file (as opposed to the socket).
type writeError struct{ err error }

func (e *writeError) Error() string { return e.err.Error() }

// fileErrWriter tags write errors so they can be told apart from socket
// read errors inside io.CopyBuffer.
type fileErrWriter struct{ w io.Writer }

func (f fileErrWriter) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	if err != nil {
		err = &writeError{err}
	}
	return n, err
}
