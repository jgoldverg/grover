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
	"strings"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"github.com/jgoldverg/grover/pkg/udpdataplane"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

// udpSessionRunner encapsulates the UDP control + data loops for a single session.
// This keeps the handshake and transfer logic together rather than scattered across
// multiple files.
type udpSessionRunner struct {
	server  *ServerSessions
	session *ServerSession
}

func newUDPSessionRunner(sm *ServerSessions, session *ServerSession) *udpSessionRunner {
	return &udpSessionRunner{
		server:  sm,
		session: session,
	}
}

func (r *udpSessionRunner) run() {
	switch r.session.Mode {
	case pb.OpenSessionRequest_READ:
		r.runDownload()
	case pb.OpenSessionRequest_WRITE:
		r.runUpload()
	default:
		internal.Debug("session mode not implemented yet", internal.Fields{
			internal.FieldMsg: "mode not supported for udp transfer",
			"action":          "noop",
			"mode":            r.session.Mode.String(),
			"session_id":      r.session.ID.String(),
		})
	}
}

func (r *udpSessionRunner) runDownload() {
	r.runSession(
		"completed udp file stream",
		"failed to stream file over udp",
		func(ctx context.Context, addr *net.UDPAddr) error {
			return r.sendFile(ctx, addr)
		},
	)
}

func (r *udpSessionRunner) runUpload() {
	r.runSession(
		"completed udp file upload",
		"failed to receive file over udp",
		func(ctx context.Context, addr *net.UDPAddr) error {
			return r.receiveFile(ctx, addr)
		},
	)
}

func (r *udpSessionRunner) runSession(
	successMsg string,
	failureMsg string,
	handler func(context.Context, *net.UDPAddr) error,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sessionID := r.session.ID.String()
	defer r.server.CloseSession(sessionID)

	addr, err := r.awaitHello()
	if err != nil {
		internal.Error("failed to receive udp hello", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        sessionID,
		})
		return
	}

	if err := handler(ctx, addr); err != nil {
		fields := internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        sessionID,
		}
		internal.Error(failureMsg, fields)
		return
	}

	internal.Info(successMsg, internal.Fields{
		"session_id": sessionID,
	})
}

func (r *udpSessionRunner) awaitHello() (*net.UDPAddr, error) {
	conn := r.session.Conn()
	if conn == nil {
		return nil, errors.New("nil udp connection for session")
	}

	buf := make([]byte, 1024)
	_ = conn.SetReadDeadline(time.Now().Add(helloTimeout))
	defer conn.SetReadDeadline(time.Time{})
	var hp udpwire.HelloPacket
	listenAddr := ""
	if conn.LocalAddr() != nil {
		listenAddr = conn.LocalAddr().String()
	}
	internal.Info("waiting for udp hello", internal.Fields{
		"session_id":  r.session.ID.String(),
		"listen_addr": listenAddr,
		"token_len":   len(r.session.Token),
	})

	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			continue
		}
		if _, err := hp.Decode(buf[:n]); err != nil {
			continue
		}
		if !bytes.Equal(hp.SessionID, r.session.ID[:]) {
			continue
		}
		if !bytes.Equal(hp.Token, r.session.Token) {
			continue
		}

		r.session.SetRemoteAddr(addr)
		internal.Info("received udp hello", internal.Fields{
			"session_id":  r.session.ID.String(),
			"remote_addr": addr.String(),
			"token_len":   len(hp.Token),
		})
		return addr, nil
	}
}

func (r *udpSessionRunner) receiveFile(ctx context.Context, addr *net.UDPAddr) error {
	conn := r.session.Conn()
	if conn == nil {
		return errors.New("nil udp connection for session")
	}
	file := r.session.file
	if file == nil {
		return errors.New("session missing destination file")
	}
	remote := addr
	if remote == nil {
		remote = r.session.RemoteAddr()
	}
	sessionKey := binary.BigEndian.Uint32(r.session.ID[:4])
	cfg := udpdataplane.ReceiveConfig{
		Transport:  udpdataplane.NewUDPConnTransport(conn),
		RemoteAddr: remote,
		SessionID:  r.session.ID.String(),
		SessionKey: sessionKey,
		StreamID:   r.session.StreamID,
		BufferSize: r.recvBufferSize(),
		Collector:  nil,
		OnRemoteAddr: func(a *net.UDPAddr) {
			if a != nil {
				r.session.SetRemoteAddr(a)
			}
		},
	}
	_, err := udpdataplane.Receive(ctx, cfg, file)
	return err
}

func (r *udpSessionRunner) sendFile(ctx context.Context, addr *net.UDPAddr) error {
	conn := r.session.Conn()
	if conn == nil {
		return errors.New("nil udp connection for session")
	}
	if r.session.StreamID == 0 {
		return errors.New("no stream ID configured for session")
	}
	remote := addr
	if remote == nil {
		remote = r.session.RemoteAddr()
	}
	if remote == nil {
		return errors.New("missing remote address for session")
	}
	file := r.session.file
	if file == nil {
		return fmt.Errorf("stream %d has no bound file", r.session.StreamID)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek file: %w", err)
	}
	sessionKey := binary.BigEndian.Uint32(r.session.ID[:4])
	_, err := udpdataplane.Send(ctx, udpdataplane.SendConfig{
		Transport:  udpdataplane.NewUDPConnTransport(conn),
		RemoteAddr: remote,
		SessionID:  r.session.ID.String(),
		SessionKey: sessionKey,
		StreamID:   r.session.StreamID,
		MTU:        int(r.session.MTU),
		Collector:  nil,
	}, file)
	return err
}

func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrClosed) {
		return true
	}
	// Fallback for platforms that wrap the error string.
	return strings.Contains(err.Error(), "use of closed network connection")
}

func describeSackRanges(r []udpwire.SackRange) string {
	if len(r) == 0 {
		return ""
	}
	var b strings.Builder
	for i, rng := range r {
		if i > 0 {
			b.WriteByte(',')
		}
		if rng.Start == rng.End {
			fmt.Fprintf(&b, "%d", rng.Start)
			continue
		}
		fmt.Fprintf(&b, "%d-%d", rng.Start, rng.End)
	}
	return b.String()
}

func (r *udpSessionRunner) recvBufferSize() int {
	if r.server != nil && r.server.cfg != nil && r.server.cfg.UDPReadBufferSize > 0 {
		return r.server.cfg.UDPReadBufferSize
	}
	return 64 * 1024
}
