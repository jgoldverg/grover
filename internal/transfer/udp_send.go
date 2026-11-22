package transfer

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jgoldverg/grover/internal/udpdataplane"
	"github.com/jgoldverg/grover/internal/wire"
)

const (
	udpStartTimeout = 10 * time.Second
	udpDoneTimeout  = 2 * time.Minute // covers the receiver's final fsync
	udpResendEvery  = 250 * time.Millisecond
)

// udpExecutor sends one file as a UDP session:
//
//  1. START (resent until READY) on a control socket,
//  2. each stream sends its byte range from its own socket via udpdataplane,
//  3. START is resent until the receiver answers DONE (file synced).
//
// The receiver answers a repeated START for a finished session with DONE
// again, so a lost DONE costs one resend interval, not a failure.
type udpExecutor struct{}

func (udpExecutor) TransferFile(ctx context.Context, t *FileTask) error {
	addr, err := net.ResolveUDPAddr("udp", endpointAddr(t.Target.data))
	if err != nil {
		return err
	}
	ctrl, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	defer ctrl.Close()

	ranges := splitRanges(int64(t.Plan.Size), int(t.Tuning.Parallelism))
	ids := make([]uint32, len(ranges))
	for i := range ids {
		ids[i] = uint32(i + 1)
	}
	key := randomSessionKey()
	start, err := wire.UDPStartPacket{
		SessionKey: key, Token: t.Target.token, FileSize: t.Plan.Size, StreamIDs: ids,
		AckEveryPackets: t.Tuning.UDPAckEveryPackets, AckEveryMs: t.Tuning.UDPAckEveryMs, WriteFlushBytes: t.Tuning.UDPWriteFlushBytes,
		JobID: t.JobID, RouteID: t.RouteID, Destination: t.Destination, RelativePath: t.Plan.RelativePath,
	}.Encode()
	if err != nil {
		return err
	}
	if _, err := udpControl(ctx, ctrl, start, key, wire.UDPReady, udpStartTimeout); err != nil {
		return fmt.Errorf("udp start %s: %w", t.Plan.RelativePath, err)
	}

	var wg sync.WaitGroup
	errs := make([]error, len(ranges))
	for i, r := range ranges {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = sendUDPStream(ctx, t, addr, key, ids[i], r)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if _, err := udpControl(ctx, ctrl, start, key, wire.UDPDone, udpDoneTimeout); err != nil {
		return fmt.Errorf("udp finish %s: %w", t.Plan.RelativePath, err)
	}
	return nil
}

func sendUDPStream(ctx context.Context, t *FileTask, addr *net.UDPAddr, key, id uint32, r byteRange) (err error) {
	stream := t.Progress.StartStream(id, uint64(r.offset), uint64(r.length))
	defer func() { stream.Done(err) }()
	if r.length == 0 {
		return nil
	}
	src, err := openRange(t.Plan, r.offset, r.length)
	if err != nil {
		return err
	}
	defer src.Close()
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	setUDPBuffers(conn, t.Tuning.UDPSocketRead, t.Tuning.UDPSocketWrite)
	readsDisk := !IsSynthetic(t.Plan.SourcePath)
	reader := &countingReader{r: src, onRead: func(n int) {
		if readsDisk {
			t.Counters.ObserveDiskRead(n)
		}
		stream.Add(int64(n))
	}}
	_, err = udpdataplane.Send(ctx, udpdataplane.SendConfig{
		Transport:       udpdataplane.NewUDPConnTransport(conn),
		SessionKey:      key,
		StreamID:        id,
		BaseOffset:      uint64(r.offset),
		MTU:             int(t.Tuning.UDPMTU),
		Collector:       t.Counters,
		FlowControl:     t.Tuning.UDPFlowControl,
		WindowPackets:   int(t.Tuning.UDPWindowPackets),
		BatchPackets:    int(t.Tuning.UDPBatchPackets),
		RequireFinalAck: true,
	}, reader)
	return err
}

// udpControl resends packet until a control reply of kind want (or DONE, which
// implies every earlier state) arrives for key. ERROR replies fail immediately.
func udpControl(ctx context.Context, conn *net.UDPConn, packet []byte, key uint32, want byte, timeout time.Duration) (byte, error) {
	defer conn.SetReadDeadline(time.Time{}) //nolint:errcheck
	deadline := time.Now().Add(timeout)
	buf := make([]byte, 64<<10)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if _, err := conn.Write(packet); err != nil {
			return 0, err
		}
		resend := time.Now().Add(udpResendEvery)
		for time.Now().Before(resend) {
			_ = conn.SetReadDeadline(resend)
			n, err := conn.Read(buf)
			if err != nil {
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					break
				}
				return 0, err // e.g. ICMP port unreachable surfaces as ECONNREFUSED
			}
			kind, k, ok := wire.PeekUDPControl(buf[:n])
			if !ok || k != key {
				continue // stale status/data replies
			}
			switch kind {
			case wire.UDPError:
				return kind, fmt.Errorf("%w: %s", wire.ErrRemote, wire.UDPErrorMessage(buf[:n]))
			case want, wire.UDPDone:
				return kind, nil
			}
		}
	}
	return 0, fmt.Errorf("no reply from %s within %s", conn.RemoteAddr(), timeout)
}

func setUDPBuffers(conn *net.UDPConn, read, write uint64) {
	if read > 0 {
		_ = conn.SetReadBuffer(int(read))
	}
	if write > 0 {
		_ = conn.SetWriteBuffer(int(write))
	}
}

func randomSessionKey() uint32 {
	var b [4]byte
	for {
		_, _ = rand.Read(b[:])
		if k := binary.BigEndian.Uint32(b[:]); k != 0 {
			return k
		}
	}
}
