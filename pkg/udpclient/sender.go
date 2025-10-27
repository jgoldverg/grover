package udpclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

type pendingChunk struct {
	seq      uint32
	streamID uint32
	payload  []byte
	lastSend time.Time
	tries    int
}

type Sender struct {
	pc      net.PacketConn
	session SessionParams

	statusCh chan udpwire.StatusPacket
	errCh    chan error

	mu      sync.Mutex
	pending map[uint32]*pendingChunk
	nextSeq uint32

	stopped chan struct{}
}

func NewSender(pc net.PacketConn, session SessionParams) *Sender {
	return &Sender{
		pc:       pc,
		session:  session,
		statusCh: make(chan udpwire.StatusPacket, 16),
		errCh:    make(chan error, 1),
		pending:  make(map[uint32]*pendingChunk),
		stopped:  make(chan struct{}),
	}
}

func (s *Sender) Run(ctx context.Context, stream StreamPlan, r io.Reader) error {
	if s.session.RemoteAddr == nil {
		return errors.New("remote addr required")
	}

	defer close(s.stopped)

	go s.statusLoop(ctx)
	go s.tickLoop(ctx)
	go s.recvLoop(ctx)

	if err := s.sendData(ctx, stream, r); err != nil {
		return err
	}

	if err := s.waitForDrain(ctx); err != nil {
		return err
	}

	select {
	case err := <-s.errCh:
		return err
	default:
		return nil
	}
}

func (s *Sender) UploadBytes(ctx context.Context, stream StreamPlan, data []byte) error {
	if stream.SizeBytes == 0 {
		stream.SizeBytes = int64(len(data))
	}
	reader := bytes.NewReader(data)
	return s.Run(ctx, stream, reader)
}

func (s *Sender) sendData(ctx context.Context, stream StreamPlan, r io.Reader) error {
	chunkSize := s.session.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 1400
	}

	buf := make([]byte, chunkSize)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			payload := append([]byte(nil), buf[:n]...)
			if err := s.queueChunk(ctx, stream.StreamID, payload); err != nil {
				return err
			}
		}

		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	return nil
}

func (s *Sender) queueChunk(ctx context.Context, streamID uint32, payload []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	window := s.session.WindowSize
	if window <= 0 {
		window = 8
	}

	s.mu.Lock()
	for window > 0 && len(s.pending) >= window {
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
		s.mu.Lock()
	}
	defer s.mu.Unlock()

	seq := s.nextSeq
	s.nextSeq++

	p := &pendingChunk{
		seq:      seq,
		streamID: streamID,
		payload:  payload,
	}

	s.pending[seq] = p
	out := make([]byte, len(payload)+32)
	packet := udpwire.DataPacket{
		SessionID: s.session.SessionID,
		StreamID:  streamID,
		Seq:       seq,
		Payload:   payload,
	}
	n, err := packet.Encode(out)
	if err != nil {
		delete(s.pending, seq)
		return err
	}

	_, err = s.pc.WriteTo(out[:n], s.session.RemoteAddr)
	if err != nil {
		delete(s.pending, seq)
		return err
	}
	p.lastSend = time.Now()

	internal.Debug("udp chunk sent", internal.Fields{
		internal.FieldKey("session"): s.session.SessionID,
		internal.FieldKey("stream"):  streamID,
		internal.FieldKey("seq"):     seq,
		internal.FieldKey("bytes"):   len(payload),
	})

	return nil
}

func (s *Sender) statusLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case pkt := <-s.statusCh:
			s.applyStatus(pkt)
		case <-s.stopped:
			return
		}
	}
}

func (s *Sender) applyStatus(pkt udpwire.StatusPacket) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cumulative := pkt.AckSeq
	for seq := range s.pending {
		if seq < cumulative {
			delete(s.pending, seq)
		}
	}

	for _, rng := range pkt.Sacks {
		for seq := rng.Start; ; seq++ {
			if p, ok := s.pending[seq]; ok {
				p.lastSend = time.Time{}
			}
			if seq == rng.End {
				break
			}
		}
	}
}

func (s *Sender) tickLoop(ctx context.Context) {
	interval := s.session.RetryAfter
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}

	ticker := time.NewTicker(interval / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.resendExpired(interval)
		case <-s.stopped:
			return
		}
	}
}

func (s *Sender) resendExpired(maxAge time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, p := range s.pending {
		if p.lastSend.IsZero() || time.Since(p.lastSend) > maxAge {
			out := make([]byte, len(p.payload)+32)
			packet := udpwire.DataPacket{
				SessionID: s.session.SessionID,
				StreamID:  p.streamID,
				Seq:       p.seq,
				Payload:   p.payload,
			}
			n, err := packet.Encode(out)
			if err != nil {
				continue
			}
			if _, err := s.pc.WriteTo(out[:n], s.session.RemoteAddr); err != nil {
				internal.Warn("udp resend failed", internal.Fields{
					internal.FieldError: err.Error(),
				})
				continue
			}
			p.lastSend = time.Now()
			p.tries++
		}
	}
}

func (s *Sender) recvLoop(ctx context.Context) {
	buf := make([]byte, 64*1024)
	for {
		deadline := s.session.AckInterval
		if deadline <= 0 {
			deadline = 200 * time.Millisecond
		}
		_ = s.pc.SetReadDeadline(time.Now().Add(deadline))
		n, addr, err := s.pc.ReadFrom(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				select {
				case <-ctx.Done():
					return
				case <-s.stopped:
					return
				default:
					continue
				}
			}
			s.pushErr(err)
			return
		}

		if !matchAddr(addr, s.session.RemoteAddr) {
			continue
		}

		packetBytes := buf[:n]
		if udpwire.IsStatusPacket(packetBytes) {
			var pkt udpwire.StatusPacket
			if _, err := pkt.Decode(packetBytes); err == nil {
				select {
				case s.statusCh <- pkt:
				default:
				}
			}
		}
	}
}

func (s *Sender) waitForDrain(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		s.mu.Lock()
		empty := len(s.pending) == 0
		s.mu.Unlock()

		if empty {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Sender) pushErr(err error) {
	select {
	case s.errCh <- err:
	default:
	}
}

func matchAddr(addr net.Addr, target *net.UDPAddr) bool {
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok || target == nil {
		return false
	}
	if !udpAddr.IP.Equal(target.IP) {
		return false
	}
	return udpAddr.Port == target.Port
}
