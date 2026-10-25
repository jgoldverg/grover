package udpdataplane

import (
	"context"
	"net"
	"time"

	"github.com/jgoldverg/grover/internal/udpreliability"
	"github.com/jgoldverg/grover/pkg/metrics"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

// recoveryPolicy is defined by its consumer, the feedback coordinator. Policies
// only select indices; the coordinator owns sending and queue mutation.
type recoveryPolicy interface {
	OnFeedback([]SentPacket, *udpwire.StatusPacket) []int
	OnTimeout([]SentPacket) []int
}

// SACKReliabilityController coordinates feedback, accounting, and recovery for
// one stream. It is owned by the sender goroutine, not shared between streams.
type SACKReliabilityController struct {
	feedback    statusReader
	outstanding udpreliability.Tracker
	recovery    recoveryPolicy
	collector   *metrics.TransferCollector
	onAck       func(AckSample)
}

func newSACKReliabilityController(cfg SendConfig, onAck func(AckSample)) *SACKReliabilityController {
	return &SACKReliabilityController{
		feedback:  newStatusReader(cfg.Transport, cfg.RemoteAddr, cfg.StreamID),
		recovery:  udpreliability.SACKRecovery{Limit: fastRetransmitPacketCount(cfg.FastRetransmitPackets)},
		collector: cfg.Collector, onAck: onAck,
	}
}

func (r *SACKReliabilityController) TrackSent(pkt SentPacket) { r.outstanding.Track(pkt) }
func (r *SACKReliabilityController) PendingPackets() int      { return r.outstanding.Len() }
func (r *SACKReliabilityController) PendingBytes() int64      { return r.outstanding.Bytes() }
func (r *SACKReliabilityController) RemoteAddr() *net.UDPAddr { return r.feedback.remote }

func (r *SACKReliabilityController) DrainFeedback(ctx context.Context, timeout time.Duration, mode FeedbackMode) (int, error) {
	return r.drainFeedback(ctx, timeout, mode)
}

func (r *SACKReliabilityController) observeAck(sample AckSample) {
	recordAckMetric(r.collector, sample.RTT)
	if r.onAck != nil {
		r.onAck(sample)
	}
}

func (r *SACKReliabilityController) retransmit(ctx context.Context, indices []int) error {
	cfg := packetSendConfig{Transport: r.feedback.transport, Remote: r.feedback.remote, Collector: r.collector}
	for _, i := range indices {
		if err := retransmitOne(ctx, cfg, &r.outstanding.Pending()[i]); err != nil {
			return err
		}
	}
	return nil
}
