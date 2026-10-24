package udpdataplane

import (
	"context"
	"net"
	"time"

	"github.com/jgoldverg/grover/pkg/udpwire"
)

// statusReader owns feedback I/O and reusable decoding buffers. A nil status
// with no error means an unrelated or malformed datagram was ignored. Returned
// status data is borrowed until the next read. ACK accounting happens elsewhere.
type statusReader struct {
	transport Transport
	remote    *net.UDPAddr
	streamID  uint32
	buffer    []byte
	packet    udpwire.StatusPacket
}

func newStatusReader(transport Transport, remote *net.UDPAddr, streamID uint32) statusReader {
	return statusReader{transport: transport, remote: remote, streamID: streamID,
		buffer: make([]byte, udpwire.StatusHeaderLen+udpwire.MaxSackRanges*udpwire.SackBlockLen)}
}

func (r *statusReader) read(ctx context.Context, timeout time.Duration, poll bool) (*udpwire.StatusPacket, time.Time, error) {
	var err error
	if poll {
		err = r.transport.SetReadDeadline(time.Now())
	} else if timeout > 0 {
		err = r.transport.SetReadDeadline(time.Now().Add(timeout))
	} else {
		err = setReadDeadline(ctx, r.transport)
	}
	if err != nil {
		return nil, time.Time{}, err
	}
	// Preserve the existing sampling point for this structural refactor.
	sampledAt := time.Now()
	n, addr, err := r.transport.ReadPacket(r.buffer)
	if err != nil {
		return nil, sampledAt, err
	}
	if r.remote == nil {
		r.remote = addr
	} else if addr != nil && !udpAddrEqual(addr, r.remote) {
		return nil, sampledAt, nil
	}
	if n == 0 || !udpwire.IsStatusPacket(r.buffer[:n]) {
		return nil, sampledAt, nil
	}
	if _, err := r.packet.Decode(r.buffer[:n]); err != nil {
		return nil, sampledAt, nil
	}
	if r.packet.StreamID != r.streamID {
		return nil, sampledAt, nil
	}
	return &r.packet, sampledAt, nil
}
