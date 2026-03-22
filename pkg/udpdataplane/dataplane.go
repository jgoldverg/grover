package udpdataplane

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/jgoldverg/grover/pkg/metrics"
)

const (
	defaultReadTimeout   = 2 * time.Second
	defaultWriteTimeout  = 2 * time.Second
	maxAckRetries        = 5
	enobufsRetryInterval = 5 * time.Millisecond
)

// SendConfig captures the parameters needed to transmit data over UDP.
type SendConfig struct {
	Transport  Transport
	RemoteAddr *net.UDPAddr
	SessionID  string
	SessionKey uint32
	StreamID   uint32
	MTU        int
	Collector  *metrics.TransferCollector
}

// ReceiveConfig controls how UDP payloads are ingested.
type ReceiveConfig struct {
	Transport    Transport
	RemoteAddr   *net.UDPAddr
	SessionID    string
	SessionKey   uint32
	StreamID     uint32
	BufferSize   int
	Collector    *metrics.TransferCollector
	ExpectedSize uint64
	// OnRemoteAddr is called the first time we learn the peer address (for unconnected sockets).
	OnRemoteAddr func(*net.UDPAddr)
}

// Sender defines the behavior for transmitting data over the UDP data plane.
type Sender interface {
	Send(ctx context.Context, cfg SendConfig, src io.Reader) (uint64, error)
}

// DefaultSender implements a SACK-aware congestion-controlled sender.
var DefaultSender Sender = NewBBRSender()

// Send reads from src and sends UDP data packets using the configured sender.
func Send(ctx context.Context, cfg SendConfig, src io.Reader) (uint64, error) {
	if DefaultSender == nil {
		return 0, errors.New("no UDP sender configured")
	}
	return DefaultSender.Send(ctx, cfg, src)
}

func recordSendMetric(col *metrics.TransferCollector, bytes int, retrans bool) {
	if col == nil || bytes <= 0 {
		return
	}
	col.ObserveSend(bytes, retrans)
}

func recordReceiveMetric(col *metrics.TransferCollector, bytes int) {
	if col == nil || bytes <= 0 {
		return
	}
	col.ObserveReceive(bytes)
}

func recordPacketSend(col *metrics.TransferCollector) {
	if col == nil {
		return
	}
	col.ObservePacketSend()
}

func recordPacketReceive(col *metrics.TransferCollector) {
	if col == nil {
		return
	}
	col.ObservePacketReceive()
}

func recordAckMetric(col *metrics.TransferCollector, d time.Duration) {
	if col == nil || d <= 0 {
		return
	}
	col.ObserveAck(d)
}
