package udpdataplane

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

// Transport abstracts the underlying UDP connection mechanics so the data plane
// logic can be exercised without depending directly on net.UDPConn.
type Transport interface {
	WritePacket(packet []byte, remote *net.UDPAddr) (int, error)
	ReadPacket(buf []byte) (int, *net.UDPAddr, error)
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	RemoteAddr() *net.UDPAddr
}

// UDPConnTransport adapts a *net.UDPConn to the Transport interface.
type UDPConnTransport struct {
	conn *net.UDPConn
}

// NewUDPConnTransport wraps conn so it satisfies Transport.
func NewUDPConnTransport(conn *net.UDPConn) *UDPConnTransport {
	if conn == nil {
		return nil
	}
	return &UDPConnTransport{conn: conn}
}

func (t *UDPConnTransport) WritePacket(packet []byte, remote *net.UDPAddr) (int, error) {
	if t.conn == nil {
		return 0, net.ErrClosed
	}
	if t.conn.RemoteAddr() != nil {
		return t.conn.Write(packet)
	}
	if remote != nil {
		return t.conn.WriteToUDP(packet, remote)
	}
	return 0, fmt.Errorf("udp transport requires remote address for unconnected socket")
}

func (t *UDPConnTransport) ReadPacket(buf []byte) (int, *net.UDPAddr, error) {
	if t.conn.RemoteAddr() != nil {
		n, err := t.conn.Read(buf)
		if err != nil {
			return n, nil, err
		}
		if addr, ok := t.conn.RemoteAddr().(*net.UDPAddr); ok {
			return n, addr, nil
		}
		return n, nil, nil
	}
	return t.conn.ReadFromUDP(buf)
}

func (t *UDPConnTransport) SetReadDeadline(ts time.Time) error {
	return t.conn.SetReadDeadline(ts)
}

func (t *UDPConnTransport) SetWriteDeadline(ts time.Time) error {
	return t.conn.SetWriteDeadline(ts)
}

func (t *UDPConnTransport) RemoteAddr() *net.UDPAddr {
	if addr, ok := t.conn.RemoteAddr().(*net.UDPAddr); ok {
		return addr
	}
	return nil
}

func setReadDeadline(ctx context.Context, transport Transport) error {
	if transport == nil {
		return nil
	}
	if deadline, ok := ctx.Deadline(); ok {
		return transport.SetReadDeadline(deadline)
	}
	return transport.SetReadDeadline(time.Now().Add(defaultReadTimeout))
}

func setWriteDeadline(ctx context.Context, transport Transport) error {
	if transport == nil {
		return nil
	}
	if deadline, ok := ctx.Deadline(); ok {
		return transport.SetWriteDeadline(deadline)
	}
	return transport.SetWriteDeadline(time.Now().Add(defaultWriteTimeout))
}

func payloadSizeFromMTU(mtu int) int {
	if mtu <= 0 {
		mtu = 1500
	}
	payload := mtu - udpwire.DataHeaderLen - 4
	if payload < 256 {
		payload = 256
	}
	return payload
}

func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrClosed) {
		return true
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

func writePacketWithRetry(ctx context.Context, transport Transport, remote *net.UDPAddr, packet []byte) error {
	for {
		if err := setWriteDeadline(ctx, transport); err != nil {
			return err
		}
		if _, err := transport.WritePacket(packet, remote); err != nil {
			if isNoBufferSpaceErr(err) {
				internal.Debug("udp write hit ENOBUFS, backing off", internal.Fields{
					internal.FieldError: err.Error(),
				})
				if err := waitForRetry(ctx, enobufsRetryInterval); err != nil {
					return err
				}
				continue
			}
			return err
		}
		return nil
	}
}

func waitForRetry(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		d = time.Millisecond
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isNoBufferSpaceErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ENOBUFS) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if errors.Is(opErr.Err, syscall.ENOBUFS) {
			return true
		}
		var sysErr *os.SyscallError
		if errors.As(opErr.Err, &sysErr) {
			return errors.Is(sysErr.Err, syscall.ENOBUFS)
		}
	}
	return strings.Contains(strings.ToLower(err.Error()), "no buffer space")
}

func udpAddrEqual(a, b *net.UDPAddr) bool {
	if a == nil || b == nil {
		return false
	}
	return a.IP.Equal(b.IP) && a.Port == b.Port
}
