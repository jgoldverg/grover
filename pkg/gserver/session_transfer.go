package gserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

func (sm *ServerSessions) runDownload(session *ServerSession) {
	if session.File() == nil {
		internal.Error("download session missing source file", internal.Fields{
			"session_id": session.ID.String(),
			"path":       session.Path,
		})
		sm.CloseSession(session.ID.String())
		return
	}

	sm.runSession(
		session,
		"completed udp file stream",
		"failed to stream file over udp",
		func(sess *ServerSession, addr *net.UDPAddr) error {
			go sm.recvLoop(sess)
			return sm.sendFile(sess, addr)
		},
		func(sess *ServerSession) internal.Fields {
			return internal.Fields{"bytes": sess.FileSize()}
		},
	)
}

func (sm *ServerSessions) runUpload(session *ServerSession) {
	if session.File() == nil {
		internal.Error("upload session missing destination file", internal.Fields{
			"session_id": session.ID.String(),
			"path":       session.Path,
		})
		sm.CloseSession(session.ID.String())
		return
	}

	sm.runSession(
		session,
		"completed udp file upload",
		"failed to receive file over udp",
		func(sess *ServerSession, _ *net.UDPAddr) error {
			if err := sm.receiveFile(sess); err != nil {
				return err
			}
			if f := sess.File(); f != nil {
				if err := f.Sync(); err != nil {
					return fmt.Errorf("sync file: %w", err)
				}
			}
			return nil
		},
		func(sess *ServerSession) internal.Fields {
			return internal.Fields{"bytes": sess.FileSize()}
		},
	)
}

func (sm *ServerSessions) runSession(
	session *ServerSession,
	successMsg string,
	failureMsg string,
	handler func(*ServerSession, *net.UDPAddr) error,
	successFields func(*ServerSession) internal.Fields,
) {
	sessionID := session.ID.String()
	defer sm.CloseSession(sessionID)

	addr, err := sm.awaitHello(session)
	if err != nil {
		internal.Error("failed to receive udp hello", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        sessionID,
			"path":              session.Path,
		})
		return
	}

	if err := handler(session, addr); err != nil {
		fields := internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        sessionID,
			"path":              session.Path,
		}
		internal.Error(failureMsg, fields)
		return
	}

	fields := internal.Fields{
		"session_id": sessionID,
		"path":       session.Path,
	}
	if successFields != nil {
		for k, v := range successFields(session) {
			fields[k] = v
		}
	}

	internal.Info(successMsg, fields)
}

func (sm *ServerSessions) receiveFile(session *ServerSession) error {
	conn := session.Conn()
	if conn == nil {
		return errors.New("nil udp connection for session")
	}
	file := session.File()
	if file == nil {
		return errors.New("no file attached to upload session")
	}
	if len(session.StreamIDs) == 0 {
		return errors.New("no stream IDs configured for upload session")
	}

	validStreams := make(map[uint32]struct{}, len(session.StreamIDs))
	for _, sid := range session.StreamIDs {
		validStreams[sid] = struct{}{}
	}

	buf := make([]byte, sm.recvBufferSize())
	var packet udpwire.DataPacket
	var offset uint64

	for {
		n, _, err := conn.ReadFromUDP(buf)
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
		if _, ok := validStreams[packet.StreamID]; !ok {
			continue
		}

		if packet.Offset != offset {
			if packet.Offset < offset {
				continue
			}
			if _, err := file.Seek(int64(packet.Offset), io.SeekStart); err != nil {
				return fmt.Errorf("seek file: %w", err)
			}
			offset = packet.Offset
		}

		if _, err := file.Write(packet.Payload); err != nil {
			return fmt.Errorf("write file: %w", err)
		}
		offset += uint64(len(packet.Payload))
		session.SetFileSize(int64(offset))
	}
}

func (sm *ServerSessions) sendFile(session *ServerSession, addr *net.UDPAddr) error {
	conn := session.Conn()
	if conn == nil {
		return errors.New("nil udp connection for session")
	}
	file := session.File()
	if file == nil {
		return errors.New("no file attached to download session")
	}
	if len(session.StreamIDs) == 0 {
		return errors.New("no stream IDs configured for session")
	}
	if addr == nil {
		return errors.New("nil remote address for session")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek file: %w", err)
	}

	payloadSize := sm.payloadSize(session)
	if payloadSize <= 0 {
		payloadSize = 1024
	}

	payloadBuf := make([]byte, payloadSize)
	packetBuf := make([]byte, udpwire.DataHeaderLen+payloadSize+4)
	sessionID32 := binary.BigEndian.Uint32(session.ID[:4])
	streamID := session.StreamIDs[0]

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
			offset += uint64(n)
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

func (sm *ServerSessions) payloadSize(session *ServerSession) int {
	mtu := int(session.MTU)
	if mtu <= 0 {
		mtu = defaultMTU
	}
	payload := mtu - udpwire.DataHeaderLen - 4 // checksum trailer
	if payload < 256 {
		payload = 256
	}
	return payload
}

func (sm *ServerSessions) recvBufferSize() int {
	if sm.cfg != nil && sm.cfg.UDPReadBufferSize > 0 {
		return sm.cfg.UDPReadBufferSize
	}
	return 64 * 1024
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
