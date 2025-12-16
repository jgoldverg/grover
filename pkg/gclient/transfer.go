package gclient

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
)

type Mode int

const (
	UPLOAD Mode = iota
	DOWNLOAD
)

type GroverTransferClient struct {
	cfg            *internal.UdpClientConfig
	ClientSessions *ClientSessions
	controlPb      pb.TransferControlClient
	fallbackHost   string
}

func NewTransferAPI(cfg *internal.UdpClientConfig, cc pb.TransferControlClient, fallbackHost string) *GroverTransferClient {
	return &GroverTransferClient{
		cfg:            cfg,
		ClientSessions: newClientSessions(time.Duration(cfg.SessionTTL), time.Duration(cfg.SessionScan)),
		controlPb:      cc,
		fallbackHost:   strings.TrimSpace(fallbackHost),
	}
}

// Get we assume a number of things at this point. 1. The file path exists, 2. the remoteURI is working correctly ip:port is valid and server is ready
func (t *GroverTransferClient) Get(ctx context.Context, path string, w io.Writer) error {
	sess, err := t.OpenSession(ctx, path, -1, DOWNLOAD)
	if err != nil {
		return err
	}
	defer sess.Close()

	rc, err := sess.Read(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	_, err = io.Copy(w, rc)
	return err
}

func (t *GroverTransferClient) Put(ctx context.Context, path string, r io.Reader, size int64, overwrite backend.OverwritePolicy) error {
	internal.Info("starting udp upload", internal.Fields{
		"path":              path,
		"size_bytes":        size,
		"overwrite_policy":  describeOverwrite(overwrite),
		"parallel_streams":  1,
		"checksum_verified": true,
	})
	sess, err := t.OpenSession(ctx, path, size, UPLOAD)
	if err != nil {
		return err
	}
	defer sess.Close()

	if err := sess.Write(ctx, r, size, overwrite); err != nil {
		return err
	}
	internal.Info("udp upload finished", internal.Fields{
		"path":       path,
		"size_bytes": size,
	})
	return nil
}

func (t *GroverTransferClient) ReadAt(ctx context.Context, path string, offset int64, p []byte) (int, error) {
	sess, err := t.OpenSession(ctx, path, -1, DOWNLOAD)
	if err != nil {
		return -1, err
	}
	defer sess.Close()

	ra, err := sess.ReadAt(ctx, offset, p)
	if err != nil {
		return -1, err
	}
	return ra, nil
}

func (t *GroverTransferClient) WriteAt(ctx context.Context, path string, offset int64, p []byte) (int, error) {
	sess, err := t.OpenSession(ctx, path, -1, UPLOAD)
	if err != nil {
		return -1, err
	}
	defer sess.Close()

	wa, err := sess.WriteAt(ctx, offset, p)
	if err != nil {
		return -1, err
	}
	return wa, nil
}

func (t *GroverTransferClient) OpenSession(
	ctx context.Context,
	path string,
	size int64,
	mode Mode,
) (Session, error) {
	// 1) Map local Mode → gRPC enum.
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
		ParallelStreams: 1, // bump later
	}

	internal.Info("requesting udp session", internal.Fields{
		"mode":             req.GetMode().String(),
		"path":             req.GetPath(),
		"size_bytes":       req.GetSize(),
		"verify_checksum":  req.GetVerifyChecksum(),
		"parallel_streams": req.GetParallelStreams(),
	})

	// 2) Call control plane.
	resp, err := t.controlPb.OpenSession(ctx, &req)
	if err != nil {
		return nil, err
	}
	sessionIDBytes := append([]byte(nil), resp.GetSessionId()...)
	sessionIDStr := fmt.Sprintf("%x", sessionIDBytes)
	if parsed, err := uuid.FromBytes(sessionIDBytes); err == nil {
		sessionIDStr = parsed.String()
	}

	udpHost := resp.GetServerHost()
	if t.fallbackHost != "" && isLoopbackHost(udpHost) {
		internal.Info("overriding UDP host from control plane response", internal.Fields{
			"server_host": udpHost,
			"fallback":    t.fallbackHost,
		})
		udpHost = t.fallbackHost
	}

	// 3) Extract parameters for data plane.
	streamIDs := resp.GetStreamIds()
	if len(streamIDs) == 0 {
		return nil, fmt.Errorf("server returned no stream_ids")
	}

	internal.Info("udp session allocated", internal.Fields{
		"session_id":      sessionIDStr,
		"server_host":     udpHost,
		"server_port":     resp.GetServerPort(),
		"stream_ids":      streamIDs,
		"mtu_hint":        resp.GetMtuHint(),
		"total_size":      resp.GetTotalSize(),
		"ttl_seconds":     resp.GetTtlSeconds(),
		"parallel_stream": len(streamIDs),
	})

	params := SessionParams{
		SessionID:    sessionIDStr,
		SessionIDRaw: sessionIDBytes,
		Token:        string(resp.GetToken()),
		UDPHost:      udpHost,
		UDPPort:      resp.GetServerPort(),
		MTU:          int(resp.GetMtuHint()),
		BufferSize:   uint64(t.cfg.SocketBufferSize),
		StreamIDs:    streamIDs,
	}

	// 4) Let ClientSessions handle UDP and session lifecycle.
	conn, err := t.ClientSessions.Open(ctx, params)
	if err != nil {
		return nil, err
	}

	// For now, we bind one logical Session to the first streamID.
	// Multi-stream (striping, etc.) can be added later.
	sess := &udpSession{
		conn:     conn,
		streamID: streamIDs[0],
	}

	internal.Info("udp session ready", internal.Fields{
		"session_id": sessionIDStr,
		"stream_id":  streamIDs[0],
		"udp_host":   udpHost,
		"udp_port":   params.UDPPort,
		"mtu":        params.MTU,
	})

	return sess, nil
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
