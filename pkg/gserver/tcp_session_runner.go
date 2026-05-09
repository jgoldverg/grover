package gserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

type tcpSessionRunner struct {
	server  *ServerSessions
	session *ServerSession
}

func newTCPSessionRunner(sm *ServerSessions, session *ServerSession) *tcpSessionRunner {
	return &tcpSessionRunner{server: sm, session: session}
}

func (r *tcpSessionRunner) run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var err error
	switch r.session.Mode {
	case pb.OpenSessionRequest_READ:
		if err = r.serveDownloadRequests(ctx); err != nil {
			internal.Error("failed to stream file over tcp", internal.Fields{
				internal.FieldError: err.Error(),
				"session_id":        r.session.ID.String(),
			})
		}
	case pb.OpenSessionRequest_WRITE:
		if err = r.receiveFile(ctx); err != nil {
			internal.Error("failed to receive file over tcp", internal.Fields{
				internal.FieldError: err.Error(),
				"session_id":        r.session.ID.String(),
			})
		}
	default:
		internal.Debug("unsupported tcp session mode", internal.Fields{
			"session_id": r.session.ID.String(),
			"mode":       r.session.Mode.String(),
		})
	}
	r.session.FinishData(err)
}

func (r *tcpSessionRunner) serveDownloadRequests(ctx context.Context) error {
	if r.session.file == nil {
		return errors.New("session missing source file")
	}
	if r.session.TotalSize == 0 || len(r.session.StreamIDs) <= 1 {
		conn, err := r.awaitConnAndHello()
		if err != nil {
			return err
		}
		defer conn.Close()
		return r.sendFile(conn)
	}

	var served atomic.Uint64
	errCh := make(chan error, len(r.session.StreamIDs))
	var wg sync.WaitGroup
	for range r.session.StreamIDs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		conn, err := r.awaitConnAndHello()
		if err != nil {
			return err
		}
		wg.Add(1)
		go func(c *net.TCPConn) {
			defer wg.Done()
			defer c.Close()
			if err := r.serveRequestedRange(c, &served); err != nil {
				errCh <- err
			}
		}(conn)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	if served.Load() != r.session.TotalSize {
		return fmt.Errorf("served %d bytes, expected %d", served.Load(), r.session.TotalSize)
	}
	return nil
}

func (r *tcpSessionRunner) awaitConnAndHello() (*net.TCPConn, error) {
	if r.session.tcpListener == nil {
		return nil, errors.New("session missing tcp listener")
	}
	_ = r.session.tcpListener.SetDeadline(time.Now().Add(helloTimeout))
	defer r.session.tcpListener.SetDeadline(time.Time{})

	conn, err := r.session.tcpListener.AcceptTCP()
	if err != nil {
		return nil, err
	}
	if err := r.readAndValidateHello(conn); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func (r *tcpSessionRunner) readAndValidateHello(conn *net.TCPConn) error {
	_ = conn.SetReadDeadline(time.Now().Add(helloTimeout))
	defer conn.SetReadDeadline(time.Time{})

	var helloLen uint16
	if err := binary.Read(conn, binary.BigEndian, &helloLen); err != nil {
		return fmt.Errorf("read hello length: %w", err)
	}
	buf := make([]byte, helloLen)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return fmt.Errorf("read hello payload: %w", err)
	}
	var hp udpwire.HelloPacket
	if _, err := hp.Decode(buf); err != nil {
		return fmt.Errorf("decode hello: %w", err)
	}
	if !bytes.Equal(hp.SessionID, r.session.ID[:]) {
		return errors.New("hello session id mismatch")
	}
	if !bytes.Equal(hp.Token, r.session.Token) {
		return errors.New("hello token mismatch")
	}
	return nil
}

func (r *tcpSessionRunner) sendFile(conn *net.TCPConn) error {
	if r.session.file == nil {
		return errors.New("session missing source file")
	}
	if _, err := r.session.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek file: %w", err)
	}
	_, err := io.Copy(conn, r.session.file)
	return err
}

func (r *tcpSessionRunner) serveRequestedRange(conn *net.TCPConn, served *atomic.Uint64) error {
	offset, length, err := readChunkHeader(conn)
	if err != nil {
		return err
	}
	end := offset + length
	if end > r.session.TotalSize {
		return fmt.Errorf("requested range out of bounds: off=%d len=%d total=%d", offset, length, r.session.TotalSize)
	}
	sr := io.NewSectionReader(r.session.file, int64(offset), int64(length))
	n, err := io.Copy(conn, sr)
	if n > 0 {
		served.Add(uint64(n))
	}
	if uint64(n) != length && err == nil {
		err = fmt.Errorf("served short range: offset=%d length=%d wrote=%d", offset, length, n)
	}
	return err
}

func (r *tcpSessionRunner) receiveFile(ctx context.Context) error {
	if r.session.file == nil {
		return errors.New("session missing destination file")
	}
	if r.session.TotalSize == 0 || len(r.session.StreamIDs) <= 1 {
		conn, err := r.awaitConnAndHello()
		if err != nil {
			return err
		}
		defer conn.Close()
		if r.session.TotalSize > 0 {
			_, err = io.CopyN(r.session.file, conn, int64(r.session.TotalSize))
			return err
		}
		_, err = io.Copy(r.session.file, conn)
		return err
	}

	var written atomic.Int64
	errCh := make(chan error, len(r.session.StreamIDs))
	var wg sync.WaitGroup
	for range r.session.StreamIDs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		conn, err := r.awaitConnAndHello()
		if err != nil {
			return err
		}
		wg.Add(1)
		go func(c *net.TCPConn) {
			defer wg.Done()
			defer c.Close()
			if err := r.receiveChunk(c, &written); err != nil {
				errCh <- err
			}
		}(conn)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	if written.Load() != int64(r.session.TotalSize) {
		return fmt.Errorf("received %d bytes, expected %d", written.Load(), r.session.TotalSize)
	}
	return nil
}

func (r *tcpSessionRunner) receiveChunk(conn *net.TCPConn, written *atomic.Int64) error {
	offset, length, err := readChunkHeader(conn)
	if err != nil {
		return err
	}
	end := int64(offset) + int64(length)
	if end < 0 || end > int64(r.session.TotalSize) {
		return fmt.Errorf("chunk out of bounds: offset=%d length=%d total=%d", offset, length, r.session.TotalSize)
	}
	ow := &offsetWriter{f: r.session.file, off: int64(offset)}
	n, err := io.CopyN(ow, conn, int64(length))
	if n > 0 {
		written.Add(n)
	}
	return err
}

type offsetWriter struct {
	f   *os.File
	off int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	n, err := w.f.WriteAt(p, w.off)
	w.off += int64(n)
	return n, err
}

func readChunkHeader(conn *net.TCPConn) (uint64, uint64, error) {
	var hdr [16]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return 0, 0, err
	}
	offset := binary.BigEndian.Uint64(hdr[0:8])
	length := binary.BigEndian.Uint64(hdr[8:16])
	if length == 0 {
		return 0, 0, errors.New("chunk length must be > 0")
	}
	return offset, length, nil
}
