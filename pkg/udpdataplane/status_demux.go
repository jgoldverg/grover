package udpdataplane

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jgoldverg/grover/pkg/udpwire"
)

type demuxPacket struct {
	data []byte
	addr *net.UDPAddr
}

type DemuxedTransport struct {
	base   Transport
	stream uint32
	ch     <-chan demuxPacket
	mu     sync.Mutex
	readDL time.Time
}

func StartStatusDemux(ctx context.Context, base Transport, streamIDs []uint32, bufferSize int, channelSize int) map[uint32]*DemuxedTransport {
	if bufferSize <= 0 {
		bufferSize = 64 * 1024
	}
	if channelSize <= 0 {
		channelSize = 128
	}
	channels := make(map[uint32]chan demuxPacket, len(streamIDs))
	transports := make(map[uint32]*DemuxedTransport, len(streamIDs))
	for _, streamID := range streamIDs {
		if streamID == 0 {
			continue
		}
		ch := make(chan demuxPacket, channelSize)
		channels[streamID] = ch
		transports[streamID] = &DemuxedTransport{
			base:   base,
			stream: streamID,
			ch:     ch,
		}
	}
	go func() {
		defer func() {
			for _, ch := range channels {
				close(ch)
			}
		}()
		buf := make([]byte, bufferSize)
		var status udpwire.StatusPacket
		for {
			if err := ctx.Err(); err != nil {
				return
			}
			_ = base.SetReadDeadline(time.Now().Add(defaultReadTimeout))
			n, addr, err := base.ReadPacket(buf)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				if isClosedNetworkError(err) || err == io.EOF {
					return
				}
				continue
			}
			if n == 0 || !udpwire.IsStatusPacket(buf[:n]) {
				continue
			}
			if _, err := status.Decode(buf[:n]); err != nil {
				continue
			}
			ch := channels[status.StreamID]
			if ch == nil {
				continue
			}
			packet := make([]byte, n)
			copy(packet, buf[:n])
			select {
			case ch <- demuxPacket{data: packet, addr: addr}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return transports
}

func (t *DemuxedTransport) WritePacket(packet []byte, remote *net.UDPAddr) (int, error) {
	return t.base.WritePacket(packet, remote)
}

func (t *DemuxedTransport) ReadPacket(buf []byte) (int, *net.UDPAddr, error) {
	t.mu.Lock()
	deadline := t.readDL
	t.mu.Unlock()
	if deadline.IsZero() {
		pkt, ok := <-t.ch
		if !ok {
			return 0, nil, io.EOF
		}
		n := copy(buf, pkt.data)
		return n, pkt.addr, nil
	}
	wait := time.Until(deadline)
	if wait <= 0 {
		return 0, nil, timeoutError{}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case pkt, ok := <-t.ch:
		if !ok {
			return 0, nil, io.EOF
		}
		n := copy(buf, pkt.data)
		return n, pkt.addr, nil
	case <-timer.C:
		return 0, nil, timeoutError{}
	}
}

func (t *DemuxedTransport) SetReadDeadline(ts time.Time) error {
	t.mu.Lock()
	t.readDL = ts
	t.mu.Unlock()
	return nil
}

func (t *DemuxedTransport) SetWriteDeadline(ts time.Time) error {
	return t.base.SetWriteDeadline(ts)
}

func (t *DemuxedTransport) RemoteAddr() *net.UDPAddr {
	return t.base.RemoteAddr()
}

type timeoutError struct{}

func (timeoutError) Error() string {
	return "i/o timeout"
}

func (timeoutError) Timeout() bool {
	return true
}

func (timeoutError) Temporary() bool {
	return true
}
