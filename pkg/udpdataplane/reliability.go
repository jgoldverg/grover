package udpdataplane

import (
	"context"
	"net"
	"time"

	"github.com/jgoldverg/grover/pkg/metrics"
)

// SACKReliabilityController coordinates feedback, accounting, and recovery for
// one stream. It is owned by the sender goroutine, not shared between streams.
type SACKReliabilityController struct {
	feedback    statusReader
	outstanding outstandingPackets
	recovery    recoveryPolicy
	collector   *metrics.TransferCollector
	onAck       func(AckSample)
}

func newSACKReliabilityController(cfg SendConfig, onAck func(AckSample)) *SACKReliabilityController {
	return &SACKReliabilityController{
		feedback:  newStatusReader(cfg.Transport, cfg.RemoteAddr, cfg.StreamID),
		recovery:  sackRecovery{limit: fastRetransmitPacketCount(cfg.FastRetransmitPackets)},
		collector: cfg.Collector, onAck: onAck,
	}
}

func (r *SACKReliabilityController) TrackSent(pkt SentPacket) { r.outstanding.track(pkt) }
func (r *SACKReliabilityController) PendingPackets() int      { return len(r.outstanding.packets) }
func (r *SACKReliabilityController) PendingBytes() int64      { return r.outstanding.bytes() }
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
	for _, i := range indices {
		if err := retransmitOne(ctx, r.feedback.transport, r.feedback.remote, &r.outstanding.packets[i], r.collector); err != nil {
			return err
		}
	}
	return nil
}
