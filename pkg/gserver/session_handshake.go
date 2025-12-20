package gserver

import (
	"bytes"
	"errors"
	"net"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

func (sm *ServerSessions) awaitHello(session *ServerSession) (*net.UDPAddr, error) {
	conn := session.Conn()
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
		"session_id":  session.ID.String(),
		"path":        session.Path,
		"listen_addr": listenAddr,
		"token_len":   len(session.Token),
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
		if !bytes.Equal(hp.SessionID, session.ID[:]) {
			continue
		}
		if !bytes.Equal(hp.Token, session.Token) {
			continue
		}

		session.SetRemoteAddr(addr)
		internal.Info("received udp hello", internal.Fields{
			"session_id":  session.ID.String(),
			"path":        session.Path,
			"remote_addr": addr.String(),
			"token_len":   len(hp.Token),
		})
		return addr, nil
	}
}

func (sm *ServerSessions) recvLoop(session *ServerSession) {
	conn := session.Conn()
	if conn == nil {
		return
	}
	buf := make([]byte, sm.recvBufferSize())
	var sp udpwire.StatusPacket

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		if n == 0 {
			continue
		}

		kind, ok := udpwire.PeekKind(buf[:n])
		if !ok {
			continue
		}

		if kind != udpwire.KindStatus {
			continue
		}
		if _, err := sp.Decode(buf[:n]); err != nil {
			continue
		}
		// TODO: feed ACK/SACK information into congestion control once implemented.
	}
}
