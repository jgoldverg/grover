package dataplane

import (
	"context"
	"fmt"
	"net"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

type Handler interface {
	OnStart(ctx context.Context, pc net.PacketConn) error
	OnStop(ctx context.Context, pc net.PacketConn) error
	HandlePacket(ctx context.Context, pc net.PacketConn, src net.Addr, buf []byte, n int)
}

type MtuOnlyHandler struct{}

func (h *MtuOnlyHandler) OnStart(ctx context.Context, pc net.PacketConn) error { return nil }
func (h *MtuOnlyHandler) OnStop(ctx context.Context, pc net.PacketConn) error  { return nil }

func (h *MtuOnlyHandler) HandlePacket(ctx context.Context, pc net.PacketConn, src net.Addr, buf []byte, n int) {
	d := buf[:n]

	// Log every packet to confirm we’re actually receiving
	internal.Debug("mtu handler received packet", internal.Fields{
		internal.FieldKey("bytes"):  n,
		internal.FieldKey("from"):   src.String(),
		internal.FieldKey("first8"): fmt.Sprintf("% x", d[:min(8, n)]),
	})

	if udpwire.IsMtuProbe(d) {
		token, probeID, _, _, _, err := udpwire.ParseMtuProbe(d)
		if err != nil {
			internal.Warn("mtu handler parse probe error", internal.Fields{
				internal.FieldError: err.Error(),
			})
			return
		}
		ack, err := udpwire.BuildMtuProbeAck(token, probeID, len(d))
		if err != nil {
			internal.Warn("mtu handler build ack error", internal.Fields{
				internal.FieldError: err.Error(),
			})
			return
		}
		if _, err := pc.WriteTo(ack, src); err != nil {
			internal.Warn("mtu handler send ack error", internal.Fields{
				internal.FieldError: err.Error(),
			})
		} else {
			internal.Debug("mtu handler ack sent", internal.Fields{
				internal.FieldKey("to"): src.String(),
			})
		}
		return
	}

	if udpwire.IsMtuProbeAck(d) {
		internal.Debug("mtu handler received ack (unexpected)", nil)
		return
	}

	internal.Debug("mtu handler received non-mtu packet", nil)
}
