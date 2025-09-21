// server/control/udp_manager.go
package control

import (
	"context"
	"fmt"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/groverserver/dataplane"
	"golang.org/x/sys/unix"
)

type Options struct {
	ReadBufferSize          int
	WriteBufferSize         int
	PacketProcessingWorkers int
	ReadTimeout             time.Duration
	QueueDepth              int
}

type ListenerManager struct {
	listeners sync.Map
	pumps     sync.Map
	Opts      Options
}

func NewListenerManager() *ListenerManager {
	return &ListenerManager{
		Opts: Options{
			ReadBufferSize:          64 * 1024,
			WriteBufferSize:         64 * 1024,
			PacketProcessingWorkers: 10,
			ReadTimeout:             10 * time.Second,
			QueueDepth:              0, // auto
		},
	}
}

func (lm *ListenerManager) GetListener(portNum uint32) (uint32, net.PacketConn) {
	if pc, ok := lm.listeners.Load(portNum); ok {
		return portNum, pc.(net.PacketConn)
	}
	return 0, nil
}

func (lm *ListenerManager) DeleteListener(portNum uint32) {
	if v, ok := lm.listeners.Load(portNum); ok {
		pc := v.(net.PacketConn)
		_ = pc.Close()
		lm.listeners.Delete(portNum)
		internal.Info("udp listener closed", internal.Fields{
			internal.FieldPort: portNum,
		})
	}

	// 2) Stop the pump and remove it.
	if pv, ok := lm.pumps.Load(portNum); ok {
		if p, ok2 := pv.(*PktPump); ok2 {
			p.Stop()
		}
		lm.pumps.Delete(portNum)
	}
}

func (lm *ListenerManager) GetListeners(ctx context.Context) ([]uint32, error) {
	res := make([]uint32, 0)
	lm.listeners.Range(func(port, _ any) bool {
		res = append(res, port.(uint32))
		return true
	})
	return res, nil
}

// LaunchNewListener binds a UDP socket (v6 preferred, then v4), registers it, starts a PktPump on it.
func (lm *ListenerManager) LaunchNewListener(ctx context.Context, port int, h dataplane.Handler) (net.PacketConn, int, error) {
	internal.Info("udp listener launch requested", internal.Fields{
		internal.FieldPort: port,
	})
	// inside LaunchNewListener
	lc := net.ListenConfig{
		Control: func(network, _ string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
				_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
				if network == "udp6" {
					// Try to make v6 socket dual-stack (Linux honors this; macOS ignores)
					_ = unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_V6ONLY, 0)
				}
			})
		},
	}

	addr6 := fmt.Sprintf("[::]:%d", port)
	addr4 := fmt.Sprintf("0.0.0.0:%d", port)
	// Prefer IPv6 dual-stack; if that fails, fall back to IPv4
	pc, err := lc.ListenPacket(ctx, "udp6", addr6)
	if err != nil {
		internal.Warn("error creating udp ipv6 listener", internal.Fields{
			internal.FieldPort:  port,
			internal.FieldError: err.Error(),
		})
		pc, err = lc.ListenPacket(ctx, "udp4", addr4)
		if err != nil {
			internal.Error("error creating udp ipv4 listener", internal.Fields{
				internal.FieldPort:  port,
				internal.FieldError: err.Error(),
			})
			return nil, 0, err
		}
	}
	// Apply buffer hints if UDPConn
	if uc, ok := pc.(*net.UDPConn); ok {
		_ = uc.SetReadBuffer(lm.Opts.ReadBufferSize)
		if lm.Opts.WriteBufferSize > 0 {
			_ = uc.SetWriteBuffer(lm.Opts.WriteBufferSize)
		}
	}

	portOut := 0
	if ua, ok := pc.LocalAddr().(*net.UDPAddr); ok {
		portOut = ua.Port
	}
	internal.Info("udp listener bound", internal.Fields{
		internal.FieldPort:               port,
		internal.FieldKey("actual_port"): portOut,
		internal.FieldKey("network"):     pc.LocalAddr().Network(),
	})

	if h != nil {
		_ = h.OnStart(ctx, pc)
	}

	key := uint32(portOut)
	lm.listeners.Store(key, pc)

	pump := NewPktPump(pc, h, lm.Opts)
	lm.pumps.Store(key, pump)
	pump.Start(ctx)
	internal.Debug("udp listener pump started", internal.Fields{
		internal.FieldPort: key,
	})

	return pc, portOut, nil
}

func (lm *ListenerManager) CloseAll(ctx context.Context, h dataplane.Handler) error {
	// 1) Close ALL sockets first to unblock readers.
	lm.listeners.Range(func(k, v any) bool {
		pc := v.(net.PacketConn)
		if h != nil {
			_ = h.OnStop(ctx, pc)
		}
		_ = pc.Close()
		return true
	})

	// 2) Stop ALL pumps (their readers are now unblocked).
	lm.pumps.Range(func(_ any, v any) bool {
		if p, ok := v.(*PktPump); ok {
			p.Stop()
		}
		return true
	})

	// 3) Clear maps.
	lm.listeners.Clear()
	lm.pumps.Clear()
	return nil
}

type pkt struct {
	n   int
	src net.Addr
	buf []byte
	t   time.Time
}

type PktPump struct {
	pc     net.PacketConn
	h      dataplane.Handler
	opts   Options
	queue  chan pkt
	wg     sync.WaitGroup
	closed chan struct{}
}

func NewPktPump(pc net.PacketConn, h dataplane.Handler, opts Options) *PktPump {
	qd := opts.QueueDepth
	if qd <= 0 {
		qd = opts.PacketProcessingWorkers * 4
		if qd < 4 {
			qd = 4
		}
	}
	return &PktPump{
		pc:     pc,
		h:      h,
		opts:   opts,
		queue:  make(chan pkt, qd),
		closed: make(chan struct{}),
	}
}

func (p *PktPump) Start(ctx context.Context) {
	// Workers
	workers := p.opts.PacketProcessingWorkers
	if workers <= 0 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for pk := range p.queue {
				if p.h != nil {
					p.h.HandlePacket(ctx, p.pc, pk.src, pk.buf, pk.n)
				}
			}
		}()
	}

	// Reader
	p.wg.Add(1)
	go func() {
		defer func() {
			close(p.queue) // signal workers to drain & exit
			close(p.closed)
			p.wg.Done()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if p.opts.ReadTimeout > 0 {
				_ = p.pc.SetReadDeadline(time.Now().Add(p.opts.ReadTimeout))
			}

			buf := make([]byte, p.opts.ReadBufferSize)
			n, src, err := p.pc.ReadFrom(buf)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				// socket closed or fatal error
				return
			}

			trim := buf[:n]
			select {
			case p.queue <- pkt{n: n, src: src, buf: trim, t: time.Now()}:
			case <-ctx.Done():
				return
			default:
				// queue full: drop (consider metrics)
			}
		}
	}()
}

// Stop waits for the reader to exit (triggered by ctx cancel or pc.Close())
// and for workers to drain.
func (p *PktPump) Stop() {
	// Ensure the read loop has exited.
	<-p.closed
	// Wait for workers to finish.
	p.wg.Wait()
}
