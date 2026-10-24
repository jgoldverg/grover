package udpdataplane

import (
	"context"
	"net"
	"time"
)

// AckSample is the feedback unit consumed by congestion controllers.
type AckSample struct {
	RTT   time.Duration
	Bytes int
}

// SendState is the sender loop's current transmit accounting.
type SendState struct {
	InflightBytes   int64
	InflightPackets int
	BatchBytes      int64
	BatchPackets    int
	PayloadSize     int
}

// ReliabilityController owns ACK/SACK interpretation and retransmission policy.
type ReliabilityController interface {
	TrackSent(SentPacket)
	RemoteAddr() *net.UDPAddr
	PendingPackets() int
	PendingBytes() int64
	DrainFeedback(ctx context.Context, timeout time.Duration, mode FeedbackMode) (int, error)
}

// SenderFactories are per-Send construction hooks. Nil hooks use the built-in
// implementation. Factories must return fresh mutable controllers for each
// stream; the sender never shares their state across Send calls.
type SenderFactories struct {
	Reliability func(SendConfig, func(AckSample)) ReliabilityController
	Congestion  func(SendConfig) CongestionController
	Flow        func(SendConfig) FlowController
	Pacer       func(SendConfig) Pacer
}

// FlowController is a hard sender-side safety gate.
type FlowController interface {
	CanSend(SendState) bool
}

// CongestionController chooses the current network in-flight target.
type CongestionController interface {
	TargetInflightBytes(SendState) int64
	AckTimeout() time.Duration
	OnAck(AckSample)
}

// Pacer controls packet send timing. NoopPacer preserves the current bursty path.
type Pacer interface {
	Wait(ctx context.Context, bytes int) error
	OnPacketSent(bytes int)
}

type FeedbackMode int

const (
	FeedbackNonBlocking FeedbackMode = iota
	FeedbackUntilProgress
	FeedbackUntilDrained
)
