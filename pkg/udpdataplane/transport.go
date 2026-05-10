package udpdataplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/udpwire"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
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

type PacketBuffer struct {
	Bytes []byte
	Addr  *net.UDPAddr
	N     int
}

type BatchTransport interface {
	ReadBatch([]PacketBuffer) (int, error)
	WriteBatch([]PacketBuffer, *net.UDPAddr) (int, error)
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

func (t *UDPConnTransport) WriteBatch(buffers []PacketBuffer, remote *net.UDPAddr) (int, error) {
	if t.conn == nil {
		return 0, net.ErrClosed
	}
	if runtime.GOOS != "linux" {
		return t.writeBatchLoop(buffers, remote)
	}
	connectedRemote, _ := t.conn.RemoteAddr().(*net.UDPAddr)
	messages := make([]ipv4.Message, 0, len(buffers))
	for _, buf := range buffers {
		if len(buf.Bytes) == 0 {
			continue
		}
		addr := buf.Addr
		if addr == nil {
			addr = remote
		}
		if addr == nil && connectedRemote != nil {
			addr = connectedRemote
		}
		if t.conn.RemoteAddr() == nil && addr == nil {
			return 0, fmt.Errorf("udp transport requires remote address for unconnected socket")
		}
		messages = append(messages, ipv4.Message{
			Buffers: [][]byte{buf.Bytes},
			Addr:    addr,
		})
	}
	if len(messages) == 0 {
		return 0, nil
	}

	written := 0
	for written < len(messages) {
		n, err := t.writeBatchMessages(messages[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrNoProgress
		}
	}
	return written, nil
}

func (t *UDPConnTransport) writeBatchLoop(buffers []PacketBuffer, remote *net.UDPAddr) (int, error) {
	written := 0
	for i := range buffers {
		if len(buffers[i].Bytes) == 0 {
			continue
		}
		addr := buffers[i].Addr
		if addr == nil {
			addr = remote
		}
		n, err := t.WritePacket(buffers[i].Bytes, addr)
		if n > 0 {
			written++
		}
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func (t *UDPConnTransport) ReadBatch(buffers []PacketBuffer) (int, error) {
	if t.conn == nil {
		return 0, net.ErrClosed
	}
	if runtime.GOOS != "linux" {
		return t.readBatchLoop(buffers)
	}
	messages := make([]ipv4.Message, 0, len(buffers))
	indexes := make([]int, 0, len(buffers))
	for i := range buffers {
		if len(buffers[i].Bytes) == 0 {
			continue
		}
		messages = append(messages, ipv4.Message{Buffers: [][]byte{buffers[i].Bytes}})
		indexes = append(indexes, i)
	}
	if len(messages) == 0 {
		return 0, nil
	}

	n, err := t.readBatchMessages(messages)
	for i := 0; i < n && i < len(indexes); i++ {
		idx := indexes[i]
		buffers[idx].N = messages[i].N
		if addr, ok := messages[i].Addr.(*net.UDPAddr); ok {
			buffers[idx].Addr = addr
		}
	}
	return n, err
}

func (t *UDPConnTransport) readBatchLoop(buffers []PacketBuffer) (int, error) {
	read := 0
	for i := range buffers {
		if len(buffers[i].Bytes) == 0 {
			continue
		}
		n, addr, err := t.ReadPacket(buffers[i].Bytes)
		if n > 0 {
			buffers[i].N = n
			buffers[i].Addr = addr
			read++
		}
		if err != nil {
			return read, err
		}
	}
	return read, nil
}

func (t *UDPConnTransport) writeBatchMessages(messages []ipv4.Message) (int, error) {
	if t.useIPv6(messages) {
		msgs := make([]ipv6.Message, len(messages))
		for i := range messages {
			msgs[i] = ipv6.Message(messages[i])
		}
		return ipv6.NewPacketConn(t.conn).WriteBatch(msgs, 0)
	}
	return ipv4.NewPacketConn(t.conn).WriteBatch(messages, 0)
}

func (t *UDPConnTransport) readBatchMessages(messages []ipv4.Message) (int, error) {
	if t.useIPv6(messages) {
		msgs := make([]ipv6.Message, len(messages))
		for i := range messages {
			msgs[i] = ipv6.Message(messages[i])
		}
		n, err := ipv6.NewPacketConn(t.conn).ReadBatch(msgs, 0)
		for i := 0; i < n && i < len(messages); i++ {
			messages[i] = ipv4.Message(msgs[i])
		}
		return n, err
	}
	return ipv4.NewPacketConn(t.conn).ReadBatch(messages, 0)
}

func (t *UDPConnTransport) useIPv6(messages []ipv4.Message) bool {
	if addr, ok := t.conn.RemoteAddr().(*net.UDPAddr); ok && addr != nil {
		return addr.IP.To4() == nil
	}
	for i := range messages {
		if addr, ok := messages[i].Addr.(*net.UDPAddr); ok && addr != nil {
			return addr.IP.To4() == nil
		}
	}
	if addr, ok := t.conn.LocalAddr().(*net.UDPAddr); ok && addr != nil {
		return addr.IP.To4() == nil
	}
	return false
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

func writeBatchWithRetry(ctx context.Context, transport Transport, remote *net.UDPAddr, packets []PacketBuffer) error {
	for len(packets) > 0 {
		if err := setWriteDeadline(ctx, transport); err != nil {
			return err
		}
		if bt, ok := transport.(BatchTransport); ok {
			n, err := bt.WriteBatch(packets, remote)
			if n > 0 {
				packets = packets[n:]
			}
			if err != nil {
				if isNoBufferSpaceErr(err) {
					internal.Debug("udp batch write hit ENOBUFS, backing off", internal.Fields{
						internal.FieldError: err.Error(),
					})
					if err := waitForRetry(ctx, enobufsRetryInterval); err != nil {
						return err
					}
					continue
				}
				return err
			}
			if n == 0 {
				return io.ErrNoProgress
			}
			continue
		}
		if _, err := transport.WritePacket(packets[0].Bytes, firstNonNilAddr(packets[0].Addr, remote)); err != nil {
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
		packets = packets[1:]
	}
	return nil
}

func firstNonNilAddr(addr, fallback *net.UDPAddr) *net.UDPAddr {
	if addr != nil {
		return addr
	}
	return fallback
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
