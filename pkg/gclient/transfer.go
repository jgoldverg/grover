package gclient

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"github.com/jgoldverg/grover/pkg/metrics"
	"github.com/jgoldverg/grover/pkg/udpdataplane"
	"github.com/jgoldverg/grover/pkg/udpwire"
)

const (
	defaultWriteTimeout  = 2 * time.Second
	enobufsRetryInterval = 5 * time.Millisecond
)

type Mode int

const (
	UPLOAD Mode = iota
	DOWNLOAD
)

type RemoteFile struct {
	FullPath     string
	RelativePath string
	Size         uint64
}

type GroverTransferClient struct {
	cfg          *internal.UdpClientConfig
	controlPb    pb.TransferControlClient
	fallbackHost string
	collector    *metrics.TransferCollector
	transport    string
}

type TransferOptions struct {
	ParallelStreams    int
	UDPFlowControl     string
	UDPWindowPackets   int
	UDPWindowBytes     int
	UDPAckEveryPackets int
	UDPAckEveryMs      int
	MTU                string
	AutoMTU            bool
}

func NewTransferAPI(cfg *internal.UdpClientConfig, cc pb.TransferControlClient, fallbackHost, protocol string) *GroverTransferClient {
	transport := strings.ToLower(strings.TrimSpace(protocol))
	if env := strings.ToLower(strings.TrimSpace(os.Getenv("GROVER_TRANSFER_PROTOCOL"))); env != "" {
		transport = env
	}
	if transport != "tcp" {
		transport = "udp"
	}
	return &GroverTransferClient{
		cfg:          cfg,
		controlPb:    cc,
		fallbackHost: strings.TrimSpace(fallbackHost),
		transport:    transport,
	}
}

func (t *GroverTransferClient) ApplyTransferOptions(opts TransferOptions) {
	if t.cfg == nil {
		t.cfg = &internal.UdpClientConfig{}
	}
	if opts.ParallelStreams > 0 {
		t.cfg.ParallelStreams = uint(opts.ParallelStreams)
		t.cfg.ParallelSenders = uint(opts.ParallelStreams)
	}
	if strings.TrimSpace(opts.UDPFlowControl) != "" {
		t.cfg.FlowControl = strings.ToLower(strings.TrimSpace(opts.UDPFlowControl))
	}
	if opts.UDPWindowPackets > 0 {
		t.cfg.WindowPackets = opts.UDPWindowPackets
		t.cfg.MaxInFlightPackets = opts.UDPWindowPackets
	}
	if opts.UDPWindowBytes > 0 {
		t.cfg.WindowBytes = opts.UDPWindowBytes
	}
	if opts.UDPAckEveryPackets > 0 {
		t.cfg.AckEveryPackets = opts.UDPAckEveryPackets
	}
	if opts.UDPAckEveryMs > 0 {
		t.cfg.AckEveryMs = opts.UDPAckEveryMs
	}
	if opts.AutoMTU || strings.EqualFold(strings.TrimSpace(opts.MTU), "auto") {
		t.cfg.MtuSize = 0
	} else if strings.TrimSpace(opts.MTU) != "" {
		var mtu int
		if _, err := fmt.Sscanf(strings.TrimSpace(opts.MTU), "%d", &mtu); err == nil && mtu > 0 {
			t.cfg.MtuSize = mtu
		}
	}
}

// SetMetricsCollector installs a collector that will be updated as transfers run.
func (t *GroverTransferClient) SetMetricsCollector(col *metrics.TransferCollector) {
	t.collector = col
}

// MetricsCollector exposes the current collector (if any).
func (t *GroverTransferClient) MetricsCollector() *metrics.TransferCollector {
	return t.collector
}

func (t *GroverTransferClient) Get(ctx context.Context, path string, w io.Writer) error {
	info, err := t.openSession(ctx, path, -1, DOWNLOAD)
	if err != nil {
		return err
	}

	lease, err := t.leaseStream(ctx, info, path, -1, DOWNLOAD, backend.UNSPECIFIED)
	if err != nil {
		return err
	}

	var (
		bytesRead uint64
		readErr   error
	)
	if t.transport == "tcp" {
		bytesRead, readErr = t.getTCP(ctx, info, w)
	} else {
		conn, err := t.dialSession(ctx, info)
		if err != nil {
			return err
		}
		defer conn.Close()
		transport := udpdataplane.NewUDPConnTransport(conn)
		dst := udpdataplane.NewSequentialWriter(w)
		if wa, ok := w.(io.WriterAt); ok {
			dst = wa
		}
		recvCfg := udpdataplane.ReceiveConfig{
			Transport:       transport,
			SessionID:       info.id,
			SessionKey:      info.sessionKey,
			StreamID:        lease.streamID,
			StreamIDs:       append([]uint32(nil), info.streamIDs...),
			BufferSize:      t.recvBufferSize(),
			Collector:       t.collector,
			ExpectedSize:    info.totalSize,
			AckEveryPackets: t.ackEveryPackets(),
			AckEvery:        t.ackEvery(),
		}
		if len(info.streamIDs) > 1 && info.totalSize > 0 {
			bytesRead, readErr = udpdataplane.ReceiveMany(ctx, recvCfg, dst)
		} else {
			bytesRead, readErr = udpdataplane.Receive(ctx, recvCfg, dst)
		}
	}
	releaseErr := t.releaseStream(ctx, info, lease, readErr == nil, bytesRead)
	if readErr != nil {
		return readErr
	}
	return releaseErr
}

func (t *GroverTransferClient) getTCP(ctx context.Context, info *sessionInfo, w io.Writer) (uint64, error) {
	if info.totalSize == 0 || len(info.streamIDs) <= 1 {
		conn, err := t.dialTCPSession(ctx, info)
		if err != nil {
			return 0, err
		}
		defer conn.Close()
		if info.totalSize > 0 {
			n, err := io.CopyN(w, newTCPMetricReader(conn, t.collector), int64(info.totalSize))
			if n < 0 {
				n = 0
			}
			return uint64(n), err
		}
		n, err := io.Copy(w, newTCPMetricReader(conn, t.collector))
		if n < 0 {
			n = 0
		}
		return uint64(n), err
	}

	if wa, ok := w.(io.WriterAt); ok {
		return t.getTCPToWriterAt(ctx, info, wa)
	}

	tmp, err := os.CreateTemp("", "grover-tcp-download-*")
	if err != nil {
		return 0, err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Truncate(int64(info.totalSize)); err != nil {
		return 0, err
	}

	ranges, err := planByteRanges(int64(info.totalSize), len(info.streamIDs))
	if err != nil {
		return 0, err
	}

	var total atomic.Uint64
	errCh := make(chan error, len(ranges))
	var wg sync.WaitGroup
	for _, br := range ranges {
		wg.Add(1)
		go func(off, ln int64) {
			defer wg.Done()
			if err := t.fetchTCPRange(ctx, info, tmp, off, ln, &total); err != nil {
				errCh <- err
			}
		}(br.offset, br.length)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return total.Load(), err
		}
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return total.Load(), err
	}
	n, err := io.Copy(w, tmp)
	if n < 0 {
		n = 0
	}
	return uint64(n), err
}

func (t *GroverTransferClient) getTCPToWriterAt(ctx context.Context, info *sessionInfo, dst io.WriterAt) (uint64, error) {
	ranges, err := planByteRanges(int64(info.totalSize), len(info.streamIDs))
	if err != nil {
		return 0, err
	}

	var total atomic.Uint64
	errCh := make(chan error, len(ranges))
	var wg sync.WaitGroup
	for _, br := range ranges {
		wg.Add(1)
		go func(off, ln int64) {
			defer wg.Done()
			if err := t.fetchTCPRange(ctx, info, dst, off, ln, &total); err != nil {
				errCh <- err
			}
		}(br.offset, br.length)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return total.Load(), err
		}
	}
	return total.Load(), nil
}

func (t *GroverTransferClient) fetchTCPRange(ctx context.Context, info *sessionInfo, dst io.WriterAt, offset, length int64, total *atomic.Uint64) error {
	conn, err := t.dialTCPSession(ctx, info)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := writeChunkHeader(conn, uint64(offset), uint64(length)); err != nil {
		return err
	}
	buf := make([]byte, 32*1024)
	var written int64
	for written < length {
		toRead := len(buf)
		remaining := length - written
		if int64(toRead) > remaining {
			toRead = int(remaining)
		}
		n, err := io.ReadFull(conn, buf[:toRead])
		if n > 0 {
			if t.collector != nil {
				t.collector.ObserveReceive(n)
				t.collector.ObservePacketReceive()
			}
			if _, werr := dst.WriteAt(buf[:n], offset+written); werr != nil {
				return werr
			}
			written += int64(n)
			total.Add(uint64(n))
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("short range read at offset=%d want=%d got=%d", offset, length, written)
			}
			return err
		}
	}
	return nil
}

func (t *GroverTransferClient) Put(ctx context.Context, path string, r io.Reader, size int64, overwrite backend.OverwritePolicy) error {
	internal.Info("starting upload", internal.Fields{
		"path":              path,
		"transport":         t.transport,
		"size_bytes":        size,
		"overwrite_policy":  describeOverwrite(overwrite),
		"parallel_streams":  1,
		"checksum_verified": true,
	})

	info, err := t.openSession(ctx, path, size, UPLOAD)
	if err != nil {
		return err
	}

	lease, err := t.leaseStream(ctx, info, path, size, UPLOAD, overwrite)
	if err != nil {
		return err
	}
	var (
		bytesWritten uint64
		writeErr     error
	)
	if t.transport == "tcp" {
		bytesWritten, writeErr = t.putTCP(ctx, info, r, size)
	} else {
		if err := t.discoverMTUIfNeeded(ctx, info); err != nil {
			return err
		}
		bytesWritten, writeErr = t.putUDP(ctx, info, lease, r, size)
	}
	releaseErr := t.releaseStream(ctx, info, lease, writeErr == nil, bytesWritten)
	if writeErr != nil {
		return writeErr
	}
	if releaseErr != nil {
		return releaseErr
	}
	internal.Info("upload finished", internal.Fields{
		"path":       path,
		"transport":  t.transport,
		"size_bytes": size,
	})
	return nil
}

func (t *GroverTransferClient) putUDP(ctx context.Context, info *sessionInfo, lease *leasedStream, r io.Reader, size int64) (uint64, error) {
	ra, ok := r.(io.ReaderAt)
	if ok && size > 0 && len(info.streamIDs) > 1 {
		return t.putUDPParallel(ctx, info, ra, size)
	}
	streamID := info.streamID
	if lease != nil && lease.streamID != 0 {
		streamID = lease.streamID
	}
	conn, err := t.dialSession(ctx, info)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	transport := udpdataplane.NewUDPConnTransport(conn)
	return udpdataplane.Send(ctx, udpdataplane.SendConfig{
		Transport:       transport,
		SessionID:       info.id,
		SessionKey:      info.sessionKey,
		StreamID:        streamID,
		MTU:             t.mtu(info),
		Collector:       t.collector,
		FlowControl:     t.flowControl(),
		WindowPackets:   t.windowPackets(),
		WindowBytes:     t.windowBytes(),
		RequireFinalAck: true,
	}, r)
}

func (t *GroverTransferClient) putUDPParallel(ctx context.Context, info *sessionInfo, ra io.ReaderAt, size int64) (uint64, error) {
	ranges, err := planByteRanges(size, len(info.streamIDs))
	if err != nil {
		return 0, err
	}
	if len(ranges) == 0 {
		return 0, nil
	}
	var total atomic.Uint64
	errCh := make(chan error, len(ranges))
	var wg sync.WaitGroup
	for i, br := range ranges {
		streamID := info.streamIDs[i]
		wg.Add(1)
		go func(streamID uint32, br byteRange) {
			defer wg.Done()
			conn, err := t.dialSession(ctx, info)
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Close()
			sr := io.NewSectionReader(ra, br.offset, br.length)
			n, err := udpdataplane.Send(ctx, udpdataplane.SendConfig{
				Transport:       udpdataplane.NewUDPConnTransport(conn),
				SessionID:       info.id,
				SessionKey:      info.sessionKey,
				StreamID:        streamID,
				BaseOffset:      uint64(br.offset),
				MTU:             t.mtu(info),
				Collector:       t.collector,
				FlowControl:     t.flowControl(),
				WindowPackets:   t.windowPackets(),
				WindowBytes:     t.windowBytes(),
				RequireFinalAck: true,
			}, sr)
			if n > 0 {
				total.Add(n)
			}
			if err != nil {
				errCh <- err
			}
		}(streamID, br)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return total.Load(), err
		}
	}
	return total.Load(), nil
}

func (t *GroverTransferClient) Enumerate(ctx context.Context, path string, recursive bool) ([]RemoteFile, error) {
	if t.controlPb == nil {
		return nil, fmt.Errorf("transfer control client unavailable")
	}
	req := &pb.EnumeratePathRequest{
		Path:      path,
		Recursive: recursive,
	}
	resp, err := t.controlPb.EnumeratePath(ctx, req)
	if err != nil {
		return nil, err
	}
	files := resp.GetFiles()
	out := make([]RemoteFile, 0, len(files))
	for _, f := range files {
		out = append(out, RemoteFile{
			FullPath:     f.GetFullPath(),
			RelativePath: f.GetRelativePath(),
			Size:         f.GetSize(),
		})
	}
	return out, nil
}

type sessionInfo struct {
	id         string
	idRaw      []byte
	sessionKey uint32
	token      []byte
	host       string
	port       uint32
	mtu        int
	streamID   uint32
	totalSize  uint64
	streamIDs  []uint32
}

type leasedStream struct {
	streamID uint32
	leaseID  []byte
}

func (t *GroverTransferClient) openSession(ctx context.Context, path string, size int64, mode Mode) (*sessionInfo, error) {
	if t.controlPb == nil {
		return nil, fmt.Errorf("transfer control client unavailable")
	}

	var m pb.OpenSessionRequest_Mode
	switch mode {
	case UPLOAD:
		m = pb.OpenSessionRequest_WRITE
	case DOWNLOAD:
		m = pb.OpenSessionRequest_READ
	default:
		m = pb.OpenSessionRequest_MODE_UNSPECIFIED
	}

	parallelStreams := uint32(1)
	if t.cfg != nil {
		if t.cfg.ParallelStreams > 1 {
			parallelStreams = uint32(t.cfg.ParallelStreams)
		} else if t.cfg.ParallelSenders > 1 {
			parallelStreams = uint32(t.cfg.ParallelSenders)
		}
	}
	req := pb.OpenSessionRequest{
		Mode:            m,
		Path:            path,
		Size:            size,
		VerifyChecksum:  true,
		ParallelStreams: parallelStreams,
	}

	internal.Debug("requesting udp session", internal.Fields{
		"mode":             req.GetMode().String(),
		"path":             req.GetPath(),
		"size_bytes":       req.GetSize(),
		"verify_checksum":  req.GetVerifyChecksum(),
		"parallel_streams": req.GetParallelStreams(),
	})

	resp, err := t.controlPb.OpenSession(ctx, &req)
	if err != nil {
		return nil, err
	}

	sessionIDRaw := append([]byte(nil), resp.GetSessionId()...)
	sessionUUID, err := uuid.FromBytes(sessionIDRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid session id: %w", err)
	}

	host := resp.GetServerHost()
	if t.fallbackHost != "" && isLoopbackHost(host) {
		internal.Debug("overriding transfer host from control plane response", internal.Fields{
			"server_host": host,
			"fallback":    t.fallbackHost,
		})
		host = t.fallbackHost
	}

	streamIDs := resp.GetStreamIds()
	if len(streamIDs) == 0 {
		return nil, fmt.Errorf("server returned no stream_ids")
	}

	internal.Info("transfer session allocated", internal.Fields{
		"session_id":      sessionUUID.String(),
		"transport":       t.transport,
		"server_host":     host,
		"server_port":     resp.GetServerPort(),
		"stream_ids":      streamIDs,
		"mtu_hint":        resp.GetMtuHint(),
		"total_size":      resp.GetTotalSize(),
		"ttl_seconds":     resp.GetTtlSeconds(),
		"parallel_stream": len(streamIDs),
	})

	info := &sessionInfo{
		id:         sessionUUID.String(),
		idRaw:      sessionIDRaw,
		sessionKey: binary.BigEndian.Uint32(sessionIDRaw[:4]),
		token:      append([]byte(nil), resp.GetToken()...),
		host:       host,
		port:       resp.GetServerPort(),
		mtu:        int(resp.GetMtuHint()),
		totalSize:  resp.GetTotalSize(),
		streamIDs:  append([]uint32(nil), streamIDs...),
	}
	if len(streamIDs) > 0 {
		info.streamID = streamIDs[0]
	}
	return info, nil
}

func (t *GroverTransferClient) dialTCPSession(ctx context.Context, info *sessionInfo) (*net.TCPConn, error) {
	addr := net.JoinHostPort(info.host, fmt.Sprint(info.port))
	d := net.Dialer{}
	connAny, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	conn, ok := connAny.(*net.TCPConn)
	if !ok {
		_ = connAny.Close()
		return nil, fmt.Errorf("unexpected tcp connection type %T", connAny)
	}
	if err := sendHelloTCP(conn, info.idRaw, info.token); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (t *GroverTransferClient) putTCP(ctx context.Context, info *sessionInfo, r io.Reader, size int64) (uint64, error) {
	ra, ok := r.(io.ReaderAt)
	if !ok || size <= 0 || len(info.streamIDs) <= 1 {
		conn, err := t.dialTCPSession(ctx, info)
		if err != nil {
			return 0, err
		}
		defer conn.Close()
		n, err := io.Copy(newTCPMetricWriter(conn, t.collector), r)
		if closeErr := conn.CloseWrite(); closeErr != nil && err == nil {
			err = closeErr
		}
		if n < 0 {
			n = 0
		}
		return uint64(n), err
	}

	ranges, err := planByteRanges(size, len(info.streamIDs))
	if err != nil {
		return 0, err
	}

	var total atomic.Uint64
	errCh := make(chan error, len(ranges))
	var wg sync.WaitGroup
	for _, br := range ranges {
		wg.Add(1)
		go func(off, ln int64) {
			defer wg.Done()
			if err := t.sendTCPChunk(ctx, info, ra, off, ln, &total); err != nil {
				errCh <- err
			}
		}(br.offset, br.length)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return total.Load(), err
		}
	}
	return total.Load(), nil
}

func (t *GroverTransferClient) sendTCPChunk(ctx context.Context, info *sessionInfo, ra io.ReaderAt, offset, length int64, total *atomic.Uint64) error {
	conn, err := t.dialTCPSession(ctx, info)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := writeChunkHeader(conn, uint64(offset), uint64(length)); err != nil {
		return err
	}
	sr := io.NewSectionReader(ra, offset, length)
	n, err := io.Copy(newTCPMetricWriter(conn, t.collector), sr)
	if closeErr := conn.CloseWrite(); closeErr != nil && err == nil {
		err = closeErr
	}
	if n > 0 {
		total.Add(uint64(n))
	}
	return err
}

func (t *GroverTransferClient) leaseStream(
	ctx context.Context,
	info *sessionInfo,
	path string,
	size int64,
	mode Mode,
	overwrite backend.OverwritePolicy,
) (*leasedStream, error) {
	if t.controlPb == nil {
		return nil, fmt.Errorf("transfer control client unavailable")
	}

	var m pb.OpenSessionRequest_Mode
	switch mode {
	case UPLOAD:
		m = pb.OpenSessionRequest_WRITE
	case DOWNLOAD:
		m = pb.OpenSessionRequest_READ
	default:
		m = pb.OpenSessionRequest_MODE_UNSPECIFIED
	}

	req := &pb.LeaseStreamRequest{
		SessionId:         append([]byte(nil), info.idRaw...),
		Mode:              m,
		Path:              path,
		Size:              size,
		VerifyChecksum:    true,
		Overwrite:         toProtoOverwrite(overwrite),
		PreferredStreamId: 0,
	}
	internal.Info("sending lease request for UDP id's", internal.Fields{
		"lease_request": req,
	})
	resp, err := t.controlPb.LeaseStream(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("lease stream: %w", err)
	}
	internal.Info("lease response", internal.Fields{
		"resp": req,
	})

	streamID := resp.GetStreamId()
	if streamID == 0 {
		streamID = info.streamID
	}
	if streamID == 0 {
		return nil, fmt.Errorf("no stream available for session %s", info.id)
	}

	return &leasedStream{
		streamID: streamID,
		leaseID:  append([]byte(nil), resp.GetLeaseId()...),
	}, nil
}

func (t *GroverTransferClient) releaseStream(ctx context.Context, info *sessionInfo, lease *leasedStream, commit bool, bytes uint64) error {
	if lease == nil || t.controlPb == nil {
		return nil
	}
	releaseCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req := &pb.ReleaseStreamRequest{
		SessionId:        append([]byte(nil), info.idRaw...),
		StreamId:         lease.streamID,
		LeaseId:          append([]byte(nil), lease.leaseID...),
		Commit:           commit,
		BytesTransferred: bytes,
	}
	_, err := t.controlPb.ReleaseStream(releaseCtx, req)
	return err
}

func (t *GroverTransferClient) dialSession(ctx context.Context, info *sessionInfo) (*net.UDPConn, error) {
	addrStr := net.JoinHostPort(info.host, fmt.Sprint(info.port))
	udpAddr, err := net.ResolveUDPAddr("udp", addrStr)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}
	if t.cfg != nil && t.cfg.SocketBufferSize > 0 {
		_ = conn.SetWriteBuffer(t.cfg.SocketBufferSize)
		_ = conn.SetReadBuffer(t.cfg.SocketBufferSize)
	}

	if err := sendHello(ctx, conn, info.idRaw, info.token); err != nil {
		conn.Close()
		return nil, fmt.Errorf("send hello: %w", err)
	}
	return conn, nil
}

func (t *GroverTransferClient) recvBufferSize() int {
	if t.cfg != nil && t.cfg.SocketBufferSize > 0 {
		return t.cfg.SocketBufferSize
	}
	return 64 * 1024
}

func (t *GroverTransferClient) mtu(info *sessionInfo) int {
	if t.cfg != nil && t.cfg.MtuSize > 0 {
		return t.cfg.MtuSize
	}
	if info != nil && info.mtu > 0 {
		return info.mtu
	}
	return 1500
}

func (t *GroverTransferClient) discoverMTUIfNeeded(ctx context.Context, info *sessionInfo) error {
	if t.cfg == nil || t.cfg.MtuSize > 0 || info == nil || info.port == 0 {
		return nil
	}
	prober := NewPMTUService()
	mtu, err := prober.DiscoverPMTU(ctx, info.host, int(info.port), 1200, 9000, 300*time.Millisecond)
	if err != nil {
		return err
	}
	t.cfg.MtuSize = mtu
	internal.Info("udp pmtu discovered", internal.Fields{
		"host": info.host,
		"port": info.port,
		"mtu":  mtu,
	})
	return nil
}

func (t *GroverTransferClient) flowControl() string {
	if t.cfg == nil || strings.TrimSpace(t.cfg.FlowControl) == "" {
		return "fixed"
	}
	return strings.ToLower(strings.TrimSpace(t.cfg.FlowControl))
}

func (t *GroverTransferClient) windowPackets() int {
	if t.cfg != nil {
		if t.cfg.WindowPackets > 0 {
			return t.cfg.WindowPackets
		}
		if t.cfg.MaxInFlightPackets > 0 {
			return t.cfg.MaxInFlightPackets
		}
	}
	return 4096
}

func (t *GroverTransferClient) windowBytes() int {
	if t.cfg != nil && t.cfg.WindowBytes > 0 {
		return t.cfg.WindowBytes
	}
	return 0
}

func (t *GroverTransferClient) ackEveryPackets() int {
	if t.cfg != nil && t.cfg.AckEveryPackets > 0 {
		return t.cfg.AckEveryPackets
	}
	return 32
}

func (t *GroverTransferClient) ackEvery() time.Duration {
	if t.cfg != nil && t.cfg.AckEveryMs > 0 {
		return time.Duration(t.cfg.AckEveryMs) * time.Millisecond
	}
	return 5 * time.Millisecond
}

func sendHello(ctx context.Context, conn *net.UDPConn, sessionID []byte, token []byte) error {
	totalLen := len(udpwire.HelloMagic) + 1 + 1 + len(sessionID) + 2 + len(token)
	tmp := make([]byte, totalLen)
	hp := udpwire.HelloPacket{
		SessionID: sessionID,
		Token:     token,
	}
	n, err := hp.Encode(tmp)
	if err != nil {
		return err
	}

	fields := internal.Fields{
		"session_id":  fmt.Sprintf("%x", sessionID),
		"token_len":   len(token),
		"hello_bytes": n,
	}
	if conn != nil && conn.RemoteAddr() != nil {
		fields["remote_addr"] = conn.RemoteAddr().String()
	}

	internal.Info("sending udp hello", fields)
	if err := writePacketWithRetry(ctx, conn, tmp[:n]); err != nil {
		internal.Error("failed to send udp hello", internal.Fields{
			internal.FieldError: err.Error(),
			"session_id":        fields["session_id"],
			"token_len":         len(token),
		})
		return err
	}
	internal.Info("udp hello sent", fields)
	return nil
}

func sendHelloTCP(conn *net.TCPConn, sessionID []byte, token []byte) error {
	totalLen := len(udpwire.HelloMagic) + 1 + 1 + len(sessionID) + 2 + len(token)
	tmp := make([]byte, totalLen)
	hp := udpwire.HelloPacket{
		SessionID: sessionID,
		Token:     token,
	}
	n, err := hp.Encode(tmp)
	if err != nil {
		return err
	}
	if n > 0xffff {
		return fmt.Errorf("hello packet too large: %d", n)
	}
	hdr := []byte{byte(n >> 8), byte(n)}
	if _, err := conn.Write(hdr); err != nil {
		return err
	}
	_, err = conn.Write(tmp[:n])
	return err
}

func writeChunkHeader(conn *net.TCPConn, offset, length uint64) error {
	var hdr [16]byte
	binary.BigEndian.PutUint64(hdr[0:8], offset)
	binary.BigEndian.PutUint64(hdr[8:16], length)
	_, err := conn.Write(hdr[:])
	return err
}

type tcpMetricReader struct {
	r         io.Reader
	collector *metrics.TransferCollector
}

func newTCPMetricReader(r io.Reader, collector *metrics.TransferCollector) io.Reader {
	if collector == nil || r == nil {
		return r
	}
	return &tcpMetricReader{r: r, collector: collector}
}

func (r *tcpMetricReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.collector.ObserveReceive(n)
		r.collector.ObservePacketReceive()
	}
	return n, err
}

type tcpMetricWriter struct {
	w         io.Writer
	collector *metrics.TransferCollector
}

func newTCPMetricWriter(w io.Writer, collector *metrics.TransferCollector) io.Writer {
	if collector == nil || w == nil {
		return w
	}
	return &tcpMetricWriter{w: w, collector: collector}
}

func (w *tcpMetricWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if n > 0 {
		w.collector.ObserveSend(n, false)
		w.collector.ObservePacketSend()
	}
	return n, err
}

func nonNegative(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}

func setWriteDeadline(ctx context.Context, conn *net.UDPConn) error {
	if conn == nil {
		return nil
	}
	if deadline, ok := ctx.Deadline(); ok {
		return conn.SetWriteDeadline(deadline)
	}
	return conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout))
}

func writePacketWithRetry(ctx context.Context, conn *net.UDPConn, packet []byte) error {
	for {
		if err := setWriteDeadline(ctx, conn); err != nil {
			return err
		}
		if _, err := conn.Write(packet); err != nil {
			if isNoBufferSpaceErr(err) {
				internal.Debug("udp write hit ENOBUFS, backing off", internal.Fields{
					internal.FieldError: err.Error(),
				})
				if err := waitForRetry(ctx, enobufsRetryInterval); err != nil {
					return err
				}
				continue
			}
			return err
		}
		return nil
	}
}

func waitForRetry(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		d = time.Millisecond
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isNoBufferSpaceErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ENOBUFS) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if errors.Is(opErr.Err, syscall.ENOBUFS) {
			return true
		}
		var sysErr *os.SyscallError
		if errors.As(opErr.Err, &sysErr) {
			return errors.Is(sysErr.Err, syscall.ENOBUFS)
		}
	}
	return strings.Contains(strings.ToLower(err.Error()), "no buffer space")
}

func isLoopbackHost(host string) bool {
	if strings.TrimSpace(host) == "" {
		return true
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

func describeOverwrite(p backend.OverwritePolicy) string {
	switch p {
	case backend.ALWAYS:
		return "always"
	case backend.IF_NEWER:
		return "if_newer"
	case backend.NEVER:
		return "never"
	case backend.IF_DIFFERENT:
		return "if_different"
	case backend.UNSPECIFIED:
		return "unspecified"
	default:
		return fmt.Sprintf("unknown(%d)", int(p))
	}
}

func toProtoOverwrite(p backend.OverwritePolicy) pb.OverwritePolicy {
	switch p {
	case backend.ALWAYS:
		return pb.OverwritePolicy_OVERWRITE_ALWAYS
	case backend.IF_NEWER:
		return pb.OverwritePolicy_OVERWRITE_IF_NEWER
	case backend.NEVER:
		return pb.OverwritePolicy_OVERWRITE_NEVER
	case backend.IF_DIFFERENT:
		return pb.OverwritePolicy_OVERWRITE_IF_DIFFERENT
	default:
		return pb.OverwritePolicy_OVERWRITE_UNSPECIFIED
	}
}
