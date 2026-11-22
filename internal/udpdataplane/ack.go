package udpdataplane

import (
	"context"
	"fmt"
	"net"
	"time"
)

// drainFeedback is the reliability state machine. Packet decoding, ACK
// accounting, recovery selection, and retransmission each have one owner.
//
// A read timeout triggers a bounded timeout retransmission. The stream only
// fails once the receiver has been silent for feedbackTimeout, so a receiver
// that is briefly busy (fsync, GC, a full socket buffer) does not kill it.
func (r *SACKReliabilityController) drainFeedback(ctx context.Context, timeout time.Duration, mode FeedbackMode) (int, error) {
	totalAcked := 0
	for r.PendingPackets() > 0 {
		if err := ctx.Err(); err != nil {
			return totalAcked, err
		}
		status, sampledAt, err := r.feedback.read(ctx, timeout, mode == FeedbackNonBlocking)
		if err != nil {
			ne, ok := err.(net.Error)
			if !ok || !ne.Timeout() {
				return totalAcked, err
			}
			if mode == FeedbackNonBlocking {
				return totalAcked, nil
			}
			if silent := time.Since(r.lastFeedback); silent >= r.feedbackTimeout {
				pending := r.outstanding.Pending()
				return totalAcked, fmt.Errorf("udp stream %d: no feedback from receiver for %s (oldest unacked seq %d, %d packets in flight)",
					r.feedback.streamID, silent.Round(time.Millisecond), pending[0].Sequence, len(pending))
			}
			if err := r.retransmit(ctx, r.recovery.OnTimeout(r.outstanding.Pending())); err != nil {
				return totalAcked, err
			}
			continue
		}
		if status == nil {
			continue
		}
		r.lastFeedback = time.Now()
		acked := r.outstanding.Acknowledge(status, sampledAt, r.observeAck)
		totalAcked += acked
		if err := r.retransmit(ctx, r.recovery.OnFeedback(r.outstanding.Pending(), status)); err != nil {
			return totalAcked, err
		}
		if mode == FeedbackUntilProgress && acked > 0 {
			return totalAcked, nil
		}
	}
	return totalAcked, nil
}

func feedbackTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultFeedbackTimeout
	}
	return d
}
