package gserver

import (
	"bytes"
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
	"github.com/jgoldverg/grover/pkg/udpwire"
)

// udpSessionRunner encapsulates the UDP control + data loops for a single session.
// This keeps the handshake and transfer logic together rather than scattered across
// multiple files.
type udpSessionRunner struct {
	server  *ServerSessions
	session *ServerSession
}

const maxStatusSackRanges = udpwire.MaxSackRanges

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
		func(addr *net.UDPAddr) error {
			return r.sendFile(addr)
		},
	)
}

func (r *udpSessionRunner) runUpload() {
	r.runSession(
		"completed udp file upload",
		"failed to receive file over udp",
		func(_ *net.UDPAddr) error {
			return r.receiveFile()
		},
	)
}

func (r *udpSessionRunner) runSession(
	successMsg string,
	failureMsg string,
	handler func(*net.UDPAddr) error,
) {
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

	if err := handler(addr); err != nil {
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

func (r *udpSessionRunner) receiveFile() error {
	conn := r.session.Conn()
	if conn == nil {
		return errors.New("nil udp connection for session")
	}
	if r.session.file == nil {
		return errors.New("session missing destination file")
	}

	buf := make([]byte, r.recvBufferSize())
	statusBuf := make([]byte, udpwire.StatusHeaderLen+maxStatusSackRanges*udpwire.SackBlockLen)
	var sackScratch []udpwire.SackRange
	var packet udpwire.DataPacket
	sessionKey := binary.BigEndian.Uint32(r.session.ID[:4])
	streamID := r.session.StreamID

	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if isClosedNetworkError(err) {
				return nil
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
		if n == 0 {
			continue
		}
		if !udpwire.IsDataPacket(buf[:n]) {
			continue
		}
		if _, err := packet.Decode(buf[:n]); err != nil {
			continue
		}
		if packet.StreamID != streamID {
			continue
		}
		file := r.session.file
		internal.Info("server udp data rx", internal.Fields{
			"session": r.session.ID.String(),
			"stream":  streamID,
			"seq":     packet.Seq,
			"bytes":   len(packet.Payload),
		})

		if packet.Offset != r.session.TotalSize {
			if packet.Offset < r.session.TotalSize {
				continue
			}
			if _, err := file.Seek(int64(packet.Offset), io.SeekStart); err != nil {
				return fmt.Errorf("seek file: %w", err)
			}
			r.session.TotalSize = packet.Offset
		}

		if _, err := file.Write(packet.Payload); err != nil {
			return fmt.Errorf("write file: %w", err)
		}
		r.session.TotalSize += uint64(len(packet.Payload))

		if st := r.session.tracker; st != nil && st.OnPacket(packet.Seq) {
			sackScratch = r.emitStatusPacket(conn, addr, st, streamID, sessionKey, sackScratch, statusBuf)
		}
	}
}

func (r *udpSessionRunner) sendFile(addr *net.UDPAddr) error {
	conn := r.session.Conn()
	if conn == nil {
		return errors.New("nil udp connection for session")
	}
	if r.session.StreamID == 0 {
		return errors.New("no stream ID configured for session")
	}
	if addr == nil {
		return errors.New("nil remote address for session")
	}

	payloadSize := r.payloadSize()
	if payloadSize <= 0 {
		payloadSize = 1024
	}

	payloadBuf := make([]byte, payloadSize)
	packetBuf := make([]byte, udpwire.DataHeaderLen+payloadSize+4)
	statusBuf := make([]byte, r.recvBufferSize())
	var statusPkt udpwire.StatusPacket
	sessionID32 := binary.BigEndian.Uint32(r.session.ID[:4])
	streamID := r.session.StreamID
	file := r.session.file
	if file == nil {
		return fmt.Errorf("stream %d has no bound file", streamID)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek file: %w", err)
	}

	var seq uint32
	var offset uint64

	for {
		n, readErr := file.Read(payloadBuf)
		if n > 0 {
			seq++
			dp := udpwire.DataPacket{
				SessionID: sessionID32,
				StreamID:  streamID,
				Seq:       seq,
				Offset:    offset,
				Payload:   payloadBuf[:n],
			}
			pktLen, err := dp.Encode(packetBuf)
			if err != nil {
				return fmt.Errorf("encode data packet: %w", err)
			}
			if _, err := conn.WriteToUDP(packetBuf[:pktLen], addr); err != nil {
				return fmt.Errorf("write udp packet: %w", err)
			}
			internal.Info("server udp data tx", internal.Fields{
				"session": r.session.ID.String(),
				"stream":  streamID,
				"seq":     seq,
				"bytes":   n,
			})
			offset += uint64(n)

			r.drainStatusPackets(conn, statusBuf, &statusPkt)
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return fmt.Errorf("read file: %w", readErr)
		}
	}
	return nil
}

func (r *udpSessionRunner) payloadSize() int {
	mtu := int(r.session.MTU)
	if mtu <= 0 {
		mtu = defaultMTU
	}
	payload := mtu - udpwire.DataHeaderLen - 4 // checksum trailer
	if payload < 256 {
		payload = 256
	}
	return payload
}

func (r *udpSessionRunner) recvBufferSize() int {
	if r.server != nil && r.server.cfg != nil && r.server.cfg.UDPReadBufferSize > 0 {
		return r.server.cfg.UDPReadBufferSize
	}
	return 64 * 1024
}

func (r *udpSessionRunner) emitStatusPacket(
	conn *net.UDPConn,
	addr *net.UDPAddr,
	st *udpwire.SackTracker,
	streamID uint32,
	sessionKey uint32,
	scratch []udpwire.SackRange,
	statusBuf []byte,
) []udpwire.SackRange {
	if st == nil || conn == nil || len(statusBuf) < udpwire.StatusHeaderLen {
		if scratch == nil {
			return nil
		}
		return scratch[:0]
	}

	ackSeq, sacks := st.Snapshot(maxStatusSackRanges, scratch)
	sp := udpwire.StatusPacket{
		SessionID: sessionKey,
		StreamID:  streamID,
		AckSeq:    ackSeq,
		Sacks:     sacks,
	}

	n, err := sp.Encode(statusBuf)
	if err != nil {
		return sacks[:0]
	}

	target := addr
	if target == nil {
		target = r.session.RemoteAddr()
	} else {
		r.session.SetRemoteAddr(target)
	}
	if target == nil {
		return sacks[:0]
	}

	if _, err := conn.WriteToUDP(statusBuf[:n], target); err != nil {
		internal.Info("failed to send udp status", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        r.session.ID.String(),
			"stream_id":         streamID,
		})
	} else {
		fields := internal.Fields{
			"session": r.session.ID.String(),
			"stream":  streamID,
			"ack":     ackSeq,
		}
		if desc := describeSackRanges(sacks); desc != "" {
			fields["sacks"] = desc
		}
		internal.Info("server udp status tx", fields)
	}
	return sacks[:0]
}

func (r *udpSessionRunner) drainStatusPackets(conn *net.UDPConn, buf []byte, sp *udpwire.StatusPacket) {
	if conn == nil || len(buf) == 0 || sp == nil {
		return
	}

	for {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Millisecond))
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				break
			}
			if isClosedNetworkError(err) {
				break
			}
			internal.Info("failed reading udp status", internal.Fields{
				internal.FieldError: err.Error(),
				"session":           r.session.ID.String(),
			})
			break
		}
		if n == 0 {
			continue
		}

		kind, ok := udpwire.PeekKind(buf[:n])
		if !ok || kind != udpwire.KindStatus {
			continue
		}
		if _, err := sp.Decode(buf[:n]); err != nil {
			continue
		}
		fields := internal.Fields{
			"session": r.session.ID.String(),
			"stream":  sp.StreamID,
			"ack":     sp.AckSeq,
		}
		if desc := describeSackRanges(sp.Sacks); desc != "" {
			fields["sacks"] = desc
		}
		internal.Info("server udp status rx", fields)
	}
	_ = conn.SetReadDeadline(time.Time{})
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
