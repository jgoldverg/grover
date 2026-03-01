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
}

func NewTransferAPI(cfg *internal.UdpClientConfig, cc pb.TransferControlClient, fallbackHost string) *GroverTransferClient {
	return &GroverTransferClient{
		cfg:          cfg,
		controlPb:    cc,
		fallbackHost: strings.TrimSpace(fallbackHost),
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

	conn, err := t.dialSession(ctx, info)
	if err != nil {
		return err
	}
	defer conn.Close()

	lease, err := t.leaseStream(ctx, info, path, -1, DOWNLOAD, backend.UNSPECIFIED)
	if err != nil {
		return err
	}

	transport := udpdataplane.NewUDPConnTransport(conn)
	bytesRead, readErr := udpdataplane.Receive(ctx, udpdataplane.ReceiveConfig{
		Transport:  transport,
		SessionID:  info.id,
		SessionKey: info.sessionKey,
		StreamID:   lease.streamID,
		BufferSize: t.recvBufferSize(),
		Collector:  t.collector,
	}, udpdataplane.NewSequentialWriter(w))
	releaseErr := t.releaseStream(ctx, info, lease, readErr == nil, bytesRead)
	if readErr != nil {
		return readErr
	}
	return releaseErr
}

func (t *GroverTransferClient) Put(ctx context.Context, path string, r io.Reader, size int64, overwrite backend.OverwritePolicy) error {
	internal.Info("starting udp upload", internal.Fields{
		"path":              path,
		"size_bytes":        size,
		"overwrite_policy":  describeOverwrite(overwrite),
		"parallel_streams":  1,
		"checksum_verified": true,
	})

	info, err := t.openSession(ctx, path, size, UPLOAD)
	if err != nil {
		return err
	}

	conn, err := t.dialSession(ctx, info)
	if err != nil {
		return err
	}
	defer conn.Close()

	lease, err := t.leaseStream(ctx, info, path, size, UPLOAD, overwrite)
	if err != nil {
		return err
	}

	transport := udpdataplane.NewUDPConnTransport(conn)
	bytesWritten, writeErr := udpdataplane.Send(ctx, udpdataplane.SendConfig{
		Transport:  transport,
		SessionID:  info.id,
		SessionKey: info.sessionKey,
		StreamID:   lease.streamID,
		MTU:        info.mtu,
		Collector:  t.collector,
	}, r)
	releaseErr := t.releaseStream(ctx, info, lease, writeErr == nil, bytesWritten)
	if writeErr != nil {
		return writeErr
	}
	if releaseErr != nil {
		return releaseErr
	}
	internal.Info("udp upload finished", internal.Fields{
		"path":       path,
		"size_bytes": size,
	})
	return nil
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

	req := pb.OpenSessionRequest{
		Mode:            m,
		Path:            path,
		Size:            size,
		VerifyChecksum:  true,
		ParallelStreams: 1,
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

	udpHost := resp.GetServerHost()
	if t.fallbackHost != "" && isLoopbackHost(udpHost) {
		internal.Debug("overriding UDP host from control plane response", internal.Fields{
			"server_host": udpHost,
			"fallback":    t.fallbackHost,
		})
		udpHost = t.fallbackHost
	}

	streamIDs := resp.GetStreamIds()
	if len(streamIDs) == 0 {
		return nil, fmt.Errorf("server returned no stream_ids")
	}

	internal.Info("udp session allocated", internal.Fields{
		"session_id":      sessionUUID.String(),
		"server_host":     udpHost,
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
		host:       udpHost,
		port:       resp.GetServerPort(),
		mtu:        int(resp.GetMtuHint()),
	}
	if len(streamIDs) > 0 {
		info.streamID = streamIDs[0]
	}
	return info, nil
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
