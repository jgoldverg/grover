package gclient

import (
	"context"
	"fmt"
	"io"
	"time"

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
	sessionManager *sessionManager
	controlPb      pb.TransferControlClient
}

func NewTransferAPI(cfg *internal.UdpClientConfig, cc pb.TransferControlClient) *GroverTransferClient {
	return &GroverTransferClient{
		cfg:            cfg,
		sessionManager: newSessionManager(time.Duration(cfg.SessionTTL), time.Duration(cfg.SessionScan)),
		controlPb:      cc,
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
	sess, err := t.OpenSession(ctx, path, size, UPLOAD)
	if err != nil {
		return err
	}
	defer sess.Close()

	if err := sess.Write(ctx, r, size, overwrite); err != nil {
		return err
	}
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

	// 2) Call control plane.
	resp, err := t.controlPb.OpenSession(ctx, &req, nil)
	if err != nil {
		return nil, err
	}

	// 3) Extract parameters for data plane.
	streamIDs := resp.GetStreamIds()
	if len(streamIDs) == 0 {
		return nil, fmt.Errorf("server returned no stream_ids")
	}

	sessionIDBytes := resp.GetSessionId()
	sessionID := fmt.Sprintf("%x", sessionIDBytes) // or uuid.FromBytes

	params := SessionParams{
		SessionID:    sessionID,
		SessionIDRaw: sessionIDBytes,
		Token:        string(resp.GetToken()),
		UDPHost:      resp.GetServerHost(),
		UDPPort:      resp.GetServerPort(),
		MTU:          int(resp.GetMtuHint()),
		BufferSize:   uint64(t.cfg.SocketBufferSize),
		StreamIDs:    streamIDs,
	}

	// 4) Let sessionManager handle UDP and session lifecycle.
	conn, err := t.sessionManager.Open(ctx, params)
	if err != nil {
		return nil, err
	}

	// For now, we bind one logical Session to the first streamID.
	// Multi-stream (striping, etc.) can be added later.
	s := &udpSession{
		conn:     conn,
		streamID: streamIDs[0],
	}

	return s, nil
}
