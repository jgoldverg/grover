package udpdataplane

import (
	"context"
	"net"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/metrics"
)

func retransmitOne(ctx context.Context, transport Transport, remote *net.UDPAddr, pkt *SentPacket, collector *metrics.TransferCollector) error {
	if pkt == nil {
		return nil
	}
	if err := writePacketWithRetry(ctx, transport, remote, pkt.Data); err != nil {
		return err
	}
	recordSendMetric(collector, pkt.PayloadBytes, true)
	recordPacketSend(collector)
	pkt.SentAt = time.Now()
	return nil
}

func writePendingBatch(
	ctx context.Context,
	transport Transport,
	remote *net.UDPAddr,
	batch []SentPacket,
	collector *metrics.TransferCollector,
	pacer Pacer,
	sessionID string,
	streamID uint32,
) error {
	if len(batch) == 0 {
		return nil
	}
	if _, ok := pacer.(NoopPacer); ok && len(batch) > 1 {
		buffers := make([]PacketBuffer, len(batch))
		for i := range batch {
			buffers[i] = PacketBuffer{Bytes: batch[i].Data}
		}
		if err := writeBatchWithRetry(ctx, transport, remote, buffers); err != nil {
			return err
		}
		for i := range batch {
			recordPacketSent(batch[i], collector, pacer, sessionID, streamID)
		}
		return nil
	}
	for i := range batch {
		if err := pacer.Wait(ctx, batch[i].PayloadBytes); err != nil {
			return err
		}
		if err := writePacketWithRetry(ctx, transport, remote, batch[i].Data); err != nil {
			return err
		}
		recordPacketSent(batch[i], collector, pacer, sessionID, streamID)
	}
	return nil
}

func recordPacketSent(pkt SentPacket, collector *metrics.TransferCollector, pacer Pacer, sessionID string, streamID uint32) {
	recordPacketSend(collector)
	recordSendMetric(collector, pkt.PayloadBytes, false)
	pacer.OnPacketSent(pkt.PayloadBytes)
	internal.Debug("udp data tx", internal.Fields{
		"session": sessionID,
		"stream":  streamID,
		"seq":     pkt.Sequence,
		"bytes":   pkt.PayloadBytes,
	})
}
