package udpdataplane

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/jgoldverg/grover/pkg/metrics"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

type pendingPacket struct {
	seq        uint32
	payloadLen int
	data       []byte
	sentAt     time.Time
}

func drainStatusPackets(
	ctx context.Context,
	transport Transport,
	remote **net.UDPAddr,
	streamID uint32,
	ackBuf []byte,
	ackPkt *udpwire.StatusPacket,
	pending *[]pendingPacket,
	collector *metrics.TransferCollector,
	timeout time.Duration,
	nonBlocking bool,
	onAck func(time.Duration, int),
) (int, error) {
	if len(*pending) == 0 {
		return 0, nil
	}
	attempts := 0
	totalAcked := 0
	for len(*pending) > 0 {
		if nonBlocking {
			if timeout > 0 {
				if err := transport.SetReadDeadline(time.Now().Add(timeout)); err != nil {
					return totalAcked, err
				}
			} else {
				if err := transport.SetReadDeadline(time.Now()); err != nil {
					return totalAcked, err
				}
			}
		} else {
			if timeout > 0 {
				if err := transport.SetReadDeadline(time.Now().Add(timeout)); err != nil {
					return totalAcked, err
				}
			} else if err := setReadDeadline(ctx, transport); err != nil {
				return totalAcked, err
			}
		}
		readStart := time.Now()
		n, addr, err := transport.ReadPacket(ackBuf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if nonBlocking {
					return totalAcked, nil
				}
				attempts++
				if attempts >= maxAckRetries {
					lastSeq := (*pending)[len(*pending)-1].seq
					return totalAcked, fmt.Errorf("failed to receive ack for seq %d after %d attempts", lastSeq, maxAckRetries)
				}
				if err := retransmitPending(ctx, transport, *remote, pending, collector); err != nil {
					return totalAcked, err
				}
				continue
			}
			return totalAcked, err
		}
		if *remote == nil {
			*remote = addr
		} else if addr != nil && !udpAddrEqual(addr, *remote) {
			continue
		}
		if n == 0 || !udpwire.IsStatusPacket(ackBuf[:n]) {
			continue
		}
		if _, err := ackPkt.Decode(ackBuf[:n]); err != nil {
			continue
		}
		if ackPkt.StreamID != streamID {
			continue
		}
		attempts = 0
		acked := advancePendingWithAck(ackPkt, pending, collector, readStart, onAck)
		totalAcked += acked
		if nonBlocking && len(*pending) == 0 {
			return totalAcked, nil
		}
	}
	return totalAcked, nil
}

func advancePendingWithAck(
	pkt *udpwire.StatusPacket,
	pending *[]pendingPacket,
	collector *metrics.TransferCollector,
	ackReceived time.Time,
	onAck func(time.Duration, int),
) int {
	if len(*pending) == 0 {
		return 0
	}
	p := *pending
	idx := 0
	ackedBytes := 0
	for idx < len(p) && p[idx].seq <= pkt.AckSeq {
		ackedBytes += p[idx].payloadLen
		if onAck != nil && !p[idx].sentAt.IsZero() {
			if sample := ackReceived.Sub(p[idx].sentAt); sample > 0 {
				recordAckMetric(collector, sample)
				onAck(sample, p[idx].payloadLen)
			}
		}
		idx++
	}
	p = p[idx:]
	if len(pkt.Sacks) > 0 && len(p) > 0 {
		keep := p[:0]
		for _, cur := range p {
			acked := false
			for _, sack := range pkt.Sacks {
				if cur.seq >= sack.Start && cur.seq <= sack.End {
					acked = true
					break
				}
			}
			if !acked {
				keep = append(keep, cur)
			} else if !cur.sentAt.IsZero() {
				ackedBytes += cur.payloadLen
				if sample := ackReceived.Sub(cur.sentAt); sample > 0 {
					recordAckMetric(collector, sample)
					if onAck != nil {
						onAck(sample, cur.payloadLen)
					}
				}
			} else {
				ackedBytes += cur.payloadLen
			}
		}
		p = keep
	}
	*pending = p
	return ackedBytes
}

func retransmitPending(ctx context.Context, transport Transport, remote *net.UDPAddr, pending *[]pendingPacket, collector *metrics.TransferCollector) error {
	for i := range *pending {
		pkt := &(*pending)[i]
		if err := writePacketWithRetry(ctx, transport, remote, pkt.data); err != nil {
			return err
		}
		recordSendMetric(collector, pkt.payloadLen, true)
		recordPacketSend(collector)
		pkt.sentAt = time.Now()
	}
	return nil
}

func waitForAck(
	ctx context.Context,
	transport Transport,
	remote *net.UDPAddr,
	streamID uint32,
	seq uint32,
	buf []byte,
	pkt *udpwire.StatusPacket,
) error {
	if transport == nil || len(buf) == 0 || pkt == nil {
		return fmt.Errorf("invalid ack buffer")
	}
	if err := setReadDeadline(ctx, transport); err != nil {
		return err
	}
	n, addr, err := transport.ReadPacket(buf)
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return ne
		}
		return err
	}
	if remote != nil && addr != nil {
		if !addr.IP.Equal(remote.IP) || addr.Port != remote.Port {
			return fmt.Errorf("ack from unexpected peer")
		}
	}
	if n == 0 {
		return fmt.Errorf("empty ack packet")
	}
	if !udpwire.IsStatusPacket(buf[:n]) {
		return fmt.Errorf("unexpected packet while waiting for ack")
	}
	if _, err := pkt.Decode(buf[:n]); err != nil {
		return err
	}
	if pkt.StreamID != streamID {
		return fmt.Errorf("ack for wrong stream %d (want %d)", pkt.StreamID, streamID)
	}
	if pkt.AckSeq < seq {
		return fmt.Errorf("stale ack seq %d (want >= %d)", pkt.AckSeq, seq)
	}
	return nil
}

func retrySendPacket(
	ctx context.Context,
	transport Transport,
	remote *net.UDPAddr,
	packet []byte,
	streamID uint32,
	seq uint32,
	buf []byte,
	pkt *udpwire.StatusPacket,
	payloadBytes int,
	collector *metrics.TransferCollector,
) error {
	for attempt := 0; attempt < maxAckRetries; attempt++ {
		if err := writePacketWithRetry(ctx, transport, remote, packet); err != nil {
			return err
		}
		recordPacketSend(collector)
		recordSendMetric(collector, payloadBytes, true)
		ackStart := time.Now()
		if err := waitForAck(ctx, transport, remote, streamID, seq, buf, pkt); err == nil {
			recordAckMetric(collector, time.Since(ackStart))
			return nil
		}
		if err := waitForRetry(ctx, 5*time.Millisecond*(1<<attempt)); err != nil {
			return err
		}
	}
	return fmt.Errorf("failed to receive ack for seq %d after %d attempts", seq, maxAckRetries)
}
