package udpdataplane

import (
	"context"
	"fmt"
	"net"
	"time"
)

// drainFeedback is the reliability state machine. Packet decoding, ACK
// accounting, recovery selection, and retransmission each have one owner.
func (r *SACKReliabilityController) drainFeedback(ctx context.Context, timeout time.Duration, mode FeedbackMode) (int, error) {
	attempts, totalAcked := 0, 0
	for r.PendingPackets() > 0 {
		if err := ctx.Err(); err != nil {
			return totalAcked, err
		}
		status, sampledAt, err := r.feedback.read(ctx, timeout, mode == FeedbackNonBlocking)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if mode == FeedbackNonBlocking {
					return totalAcked, nil
				}
				attempts++
				if attempts >= maxAckRetries {
					pending := r.outstanding.Pending()
					return totalAcked, fmt.Errorf("failed to receive ack for seq %d after %d attempts", pending[len(pending)-1].Sequence, maxAckRetries)
				}
				if err := r.retransmit(ctx, r.recovery.OnTimeout(r.outstanding.Pending())); err != nil {
					return totalAcked, err
				}
				continue
			}
			return totalAcked, err
		}
		if status == nil {
			continue
		}
		attempts = 0
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
