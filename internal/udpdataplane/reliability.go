package udpdataplane

import (
	"context"
	"net"
	"time"

	"github.com/jgoldverg/grover/internal/metrics"
	"github.com/jgoldverg/grover/internal/udpwire"
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
	outstanding Tracker
	recovery    recoveryPolicy
	collector   *metrics.Counters
	onAck       func(AckSample)

	// lastFeedback is when the receiver was last heard from (or the first
	// packet was sent). The stream fails after feedbackTimeout of silence.
	lastFeedback    time.Time
	srtt            time.Duration
	feedbackTimeout time.Duration
}

func newSACKReliabilityController(cfg SendConfig, onAck func(AckSample)) *SACKReliabilityController {
	return &SACKReliabilityController{
		feedback:  newStatusReader(cfg.Transport, cfg.RemoteAddr, cfg.StreamID),
		recovery:  SACKRecovery{Limit: fastRetransmitPacketCount(cfg.FastRetransmitPackets)},
		collector: cfg.Collector, onAck: onAck,
		feedbackTimeout: feedbackTimeout(cfg.FeedbackTimeout),
	}
}

func (r *SACKReliabilityController) TrackSent(pkt SentPacket) {
	if r.lastFeedback.IsZero() {
		r.lastFeedback = time.Now()
	}
	r.outstanding.Track(pkt)
}
func (r *SACKReliabilityController) PendingPackets() int      { return r.outstanding.Len() }
func (r *SACKReliabilityController) PendingBytes() int64      { return r.outstanding.Bytes() }
func (r *SACKReliabilityController) RemoteAddr() *net.UDPAddr { return r.feedback.remote }

func (r *SACKReliabilityController) DrainFeedback(ctx context.Context, timeout time.Duration, mode FeedbackMode) (int, error) {
	return r.drainFeedback(ctx, timeout, mode)
}

func (r *SACKReliabilityController) observeAck(sample AckSample) {
	if sample.RTT > 0 {
		recordAckMetric(r.collector, sample.RTT)
		if r.srtt == 0 {
			r.srtt = sample.RTT
		} else {
			r.srtt = (7*r.srtt + sample.RTT) / 8
		}
	}
	if r.onAck != nil {
		r.onAck(sample)
	}
}

func (r *SACKReliabilityController) retransmit(ctx context.Context, indices []int) error {
	cfg := packetSendConfig{Transport: r.feedback.transport, Remote: r.feedback.remote, Collector: r.collector}
	// Do not resend a packet that was (re)sent less than ~one RTT ago: every
	// status packet repeats the same SACK holes, and resending on each one
	// turns a single loss into a burst of duplicates.
	minGap := min(max(2*r.srtt, 2*time.Millisecond), 100*time.Millisecond)
	now := time.Now()
	pending := r.outstanding.Pending()
	for _, i := range indices {
		if sent := pending[i].SentAt; !sent.IsZero() && now.Sub(sent) < minGap {
			continue
		}
		if err := retransmitOne(ctx, cfg, &pending[i]); err != nil {
			return err
		}
	}
	return nil
}
