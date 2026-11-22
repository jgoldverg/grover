package udpdataplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"time"

	"github.com/jgoldverg/grover/internal/metrics"
	"github.com/jgoldverg/grover/internal/udpwire"
)

const (
	defaultIdleTimeout = 30 * time.Second
	defaultLinger      = time.Second
	receivePollEvery   = 250 * time.Millisecond
)

// ReceiveConfig controls one receive session: a single file arriving on one
// or more streams that all share SessionKey.
type ReceiveConfig struct {
	Transport    Transport
	SessionKey   uint32
	StreamIDs    []uint32
	BufferSize   int // max datagram size; default 64KiB
	Collector    *metrics.Counters
	ExpectedSize uint64

	AckEveryPackets int
	AckEvery        time.Duration
	BatchPackets    int

	// WriteFlushBytes enables receive-side coalescing before WriteAt.
	// Zero writes each packet directly.
	WriteFlushBytes uint64
	// MaxBufferedWriteBytes bounds coalescing memory; zero derives from WriteFlushBytes.
	MaxBufferedWriteBytes uint64

	// IdleTimeout fails the receive when no data arrives for this long.
	IdleTimeout time.Duration
	// OnComplete runs once every expected byte is written, before the final
	// linger. The transfer layer uses it to fsync and send DONE.
	OnComplete func() error
	// Linger keeps answering retransmissions after completion so the sender
	// sees its final ACK. Negative disables lingering.
	Linger time.Duration
}

type receiveStream struct {
	tracker *udpwire.SackTracker
	acks    *ackCoalescer
	remote  *net.UDPAddr
	sacks   []udpwire.SackRange
}

type receiver struct {
	cfg       ReceiveConfig
	streams   map[uint32]*receiveStream
	writer    PayloadWriter
	statusBuf []byte
	packet    udpwire.DataPacket
	lastData  time.Time
}

// Receive writes the session's payload into dst and emits ACK/SACK status
// packets per stream. It returns the number of durable bytes written.
func Receive(ctx context.Context, cfg ReceiveConfig, dst io.WriterAt) (uint64, error) {
	if cfg.Transport == nil {
		return 0, errors.New("transport is required")
	}
	if dst == nil {
		return 0, errors.New("nil destination writer")
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 64 * 1024
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultIdleTimeout
	}
	if cfg.Linger == 0 {
		cfg.Linger = defaultLinger
	}
	r := &receiver{
		cfg:       cfg,
		streams:   make(map[uint32]*receiveStream, len(cfg.StreamIDs)),
		statusBuf: make([]byte, udpwire.StatusHeaderLen+udpwire.MaxSackRanges*udpwire.SackBlockLen),
		lastData:  time.Now(),
	}
	for _, id := range cfg.StreamIDs {
		if id != 0 {
			r.streams[id] = &receiveStream{tracker: udpwire.NewSackTracker(), acks: newAckCoalescer(cfg.AckEveryPackets, cfg.AckEvery)}
		}
	}
	if len(r.streams) == 0 {
		return 0, errors.New("at least one non-zero stream id is required")
	}
	writer, err := newPayloadWriter(dst, cfg.ExpectedSize, cfg.WriteFlushBytes, cfg.MaxBufferedWriteBytes)
	if err != nil {
		return 0, err
	}
	r.writer = writer
	defer cfg.Transport.SetReadDeadline(time.Time{}) //nolint:errcheck

	reader := newPacketReader(cfg.Transport, cfg.BufferSize, cfg.BatchPackets)
	for {
		if err := ctx.Err(); err != nil {
			return writer.DurableBytes(), err
		}
		if err := cfg.Transport.SetReadDeadline(r.nextDeadline(ctx)); err != nil {
			return writer.DurableBytes(), err
		}
		finished := false
		err := reader.read(func(p []byte, addr *net.UDPAddr) error {
			if finished {
				return nil
			}
			done, err := r.handle(p, addr)
			finished = done
			return err
		})
		if finished {
			return r.complete(ctx, reader)
		}
		if err == nil {
			continue
		}
		if isTimeout(err) {
			r.flushDueAcks()
			if idle := time.Since(r.lastData); idle > cfg.IdleTimeout {
				return writer.DurableBytes(), fmt.Errorf("udp receive idle for %s: received %d of %d bytes", idle.Round(time.Second), writer.DurableBytes(), cfg.ExpectedSize)
			}
			continue
		}
		if isClosedNetworkError(err) || errors.Is(err, io.EOF) {
			return writer.DurableBytes(), ensureReceiveComplete(writer, cfg.ExpectedSize)
		}
		return writer.DurableBytes(), err
	}
}

// handle processes one datagram and reports whether the file is complete.
func (r *receiver) handle(p []byte, addr *net.UDPAddr) (bool, error) {
	if !udpwire.IsDataPacket(p) {
		return false, nil
	}
	if _, err := r.packet.Decode(p); err != nil || r.packet.SessionID != r.cfg.SessionKey {
		return false, nil
	}
	st := r.streams[r.packet.StreamID]
	if st == nil {
		return false, nil
	}
	if st.remote == nil {
		st.remote = addr
	} else if addr != nil && !udpAddrEqual(addr, st.remote) {
		return false, nil
	}
	r.lastData = time.Now()
	finished := false
	if payload := r.packet.Payload; len(payload) > 0 {
		recordNetworkReceiveMetric(r.cfg.Collector, len(payload))
		if exp := r.cfg.ExpectedSize; exp > 0 {
			switch {
			case r.packet.Offset >= exp:
				payload = nil
			case r.packet.Offset+uint64(len(payload)) > exp:
				payload = payload[:exp-r.packet.Offset]
			}
		}
		added, done, err := r.writer.WritePayload(r.packet.Offset, payload)
		if err != nil {
			return false, err
		}
		if added > 0 {
			recordPacketReceive(r.cfg.Collector)
			recordReceiveMetric(r.cfg.Collector, int(added))
		}
		finished = done
	}
	before, _ := st.tracker.Snapshot(0, nil)
	changed := st.tracker.OnPacket(r.packet.Seq)
	gap := r.packet.Seq > before+1
	if changed && st.acks.OnPacket(gap) {
		r.emitStatus(r.packet.StreamID, st)
		st.acks.MarkSent()
	}
	return finished, nil
}

// complete flushes, runs OnComplete, ACKs every stream, then lingers.
func (r *receiver) complete(ctx context.Context, reader *packetReader) (uint64, error) {
	if err := r.writer.Flush(); err != nil {
		return r.writer.DurableBytes(), err
	}
	r.emitAll()
	if r.cfg.OnComplete != nil {
		if err := r.cfg.OnComplete(); err != nil {
			return r.writer.DurableBytes(), err
		}
	}
	if r.cfg.Linger > 0 {
		r.linger(ctx, reader)
	}
	return r.writer.DurableBytes(), nil
}

// linger re-ACKs retransmitted packets so a sender whose final ACK was lost
// can still finish.
func (r *receiver) linger(ctx context.Context, reader *packetReader) {
	deadline := time.Now().Add(r.cfg.Linger)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		_ = r.cfg.Transport.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		err := reader.read(func(p []byte, addr *net.UDPAddr) error {
			if !udpwire.IsDataPacket(p) {
				return nil
			}
			if _, err := r.packet.Decode(p); err != nil || r.packet.SessionID != r.cfg.SessionKey {
				return nil
			}
			if st := r.streams[r.packet.StreamID]; st != nil && st.remote != nil {
				st.tracker.OnPacket(r.packet.Seq)
				r.emitStatus(r.packet.StreamID, st)
			}
			return nil
		})
		if err != nil && !isTimeout(err) {
			return
		}
	}
}

func (r *receiver) nextDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(receivePollEvery)
	for _, st := range r.streams {
		if next := st.acks.NextDeadline(); !next.IsZero() && next.Before(deadline) {
			deadline = next
		}
	}
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return deadline
}

func (r *receiver) flushDueAcks() {
	for id, st := range r.streams {
		if st.remote != nil && st.acks.ShouldFlush() {
			r.emitStatus(id, st)
			st.acks.MarkSent()
		}
	}
}

func (r *receiver) emitAll() {
	for id, st := range r.streams {
		if st.remote != nil {
			r.emitStatus(id, st)
		}
	}
}

func (r *receiver) emitStatus(streamID uint32, st *receiveStream) {
	ackSeq, sacks := st.tracker.Snapshot(udpwire.MaxSackRanges, st.sacks)
	sp := udpwire.StatusPacket{SessionID: r.cfg.SessionKey, StreamID: streamID, AckSeq: ackSeq, Sacks: sacks}
	if n, err := sp.Encode(r.statusBuf); err == nil {
		_, _ = r.cfg.Transport.WritePacket(r.statusBuf[:n], st.remote)
	}
	st.sacks = sacks[:0]
}

func ensureReceiveComplete(writer PayloadWriter, expected uint64) error {
	if expected == 0 || writer == nil {
		return nil
	}
	if got := writer.DurableBytes(); got < expected {
		return fmt.Errorf("udp receive incomplete: durable=%d expected=%d", got, expected)
	}
	return nil
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// packetReader reads one datagram, or a batch via recvmmsg on Linux.
type packetReader struct {
	transport Transport
	buf       []byte
	batchT    BatchTransport
	batch     []PacketBuffer
}

func newPacketReader(t Transport, bufSize, batchPackets int) *packetReader {
	pr := &packetReader{transport: t, buf: make([]byte, bufSize)}
	if bt, ok := t.(BatchTransport); ok && runtime.GOOS == "linux" {
		pr.batchT = bt
		pr.batch = make([]PacketBuffer, batchPacketCount(batchPackets))
		for i := range pr.batch {
			pr.batch[i].Bytes = make([]byte, bufSize)
		}
	}
	return pr
}

// read delivers every available datagram to fn. Read errors are returned after
// any datagrams that arrived with them have been delivered.
func (pr *packetReader) read(fn func([]byte, *net.UDPAddr) error) error {
	if pr.batchT == nil {
		n, addr, err := pr.transport.ReadPacket(pr.buf)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		return fn(pr.buf[:n], addr)
	}
	for i := range pr.batch {
		pr.batch[i].N, pr.batch[i].Addr = 0, nil
	}
	n, readErr := pr.batchT.ReadBatch(pr.batch)
	for i := 0; i < n; i++ {
		if pr.batch[i].N <= 0 {
			continue
		}
		if err := fn(pr.batch[i].Bytes[:pr.batch[i].N], pr.batch[i].Addr); err != nil {
			return err
		}
	}
	if n > 0 && isTimeout(readErr) {
		return nil
	}
	return readErr
}
