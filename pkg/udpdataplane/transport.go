package udpdataplane

import (
	"fmt"
	"net"
	"time"
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
