package udpdataplane

import (
	"context"
	"net"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/metrics"
)

// packetSendConfig binds transport and observability for one send operation.
// It contains no outstanding queue or congestion state.
type packetSendConfig struct {
	Transport Transport
	Remote    *net.UDPAddr
	Collector *metrics.TransferCollector
	Pacer     Pacer
	SessionID string
	StreamID  uint32
}

func retransmitOne(ctx context.Context, cfg packetSendConfig, pkt *SentPacket) error {
	if pkt == nil {
		return nil
	}
	if err := writePacketWithRetry(ctx, cfg.Transport, cfg.Remote, pkt.Data); err != nil {
		return err
	}
	recordSendMetric(cfg.Collector, pkt.PayloadBytes, true)
	recordPacketSend(cfg.Collector)
	pkt.SentAt = time.Now()
	return nil
}

func writePendingBatch(ctx context.Context, cfg packetSendConfig, batch []SentPacket) error {
	if len(batch) == 0 {
		return nil
	}
	if _, ok := cfg.Pacer.(NoopPacer); ok && len(batch) > 1 {
		buffers := make([]PacketBuffer, len(batch))
		for i := range batch {
			buffers[i] = PacketBuffer{Bytes: batch[i].Data}
		}
		if err := writeBatchWithRetry(ctx, cfg.Transport, cfg.Remote, buffers); err != nil {
			return err
		}
		for i := range batch {
			recordPacketSent(cfg, batch[i])
		}
		return nil
	}
	for i := range batch {
		if err := cfg.Pacer.Wait(ctx, batch[i].PayloadBytes); err != nil {
			return err
		}
		if err := writePacketWithRetry(ctx, cfg.Transport, cfg.Remote, batch[i].Data); err != nil {
			return err
		}
		recordPacketSent(cfg, batch[i])
	}
	return nil
}

func recordPacketSent(cfg packetSendConfig, pkt SentPacket) {
	recordPacketSend(cfg.Collector)
	recordSendMetric(cfg.Collector, pkt.PayloadBytes, false)
	cfg.Pacer.OnPacketSent(pkt.PayloadBytes)
	internal.Debug("udp data tx", internal.Fields{
		"session": cfg.SessionID,
		"stream":  cfg.StreamID,
		"seq":     pkt.Sequence,
		"bytes":   pkt.PayloadBytes,
	})
}
