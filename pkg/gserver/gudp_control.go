package gserver

import (
	"context"
	"errors"
	"os"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GudpControl implements the TransferControl gRPC service and delegates the
// heavy lifting to the SessionManager.
type GudpControl struct {
	pb.UnimplementedTransferControlServer

	sm *SessionManager
}

func NewGUdpControl(cfg *internal.ServerConfig) *GudpControl {
	return &GudpControl{
		sm: NewSessionManager(cfg),
	}
}

func (gc *GudpControl) OpenSession(ctx context.Context, req *pb.OpenSessionRequest) (*pb.OpenSessionResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "empty request")
	}
	if req.GetMode() == pb.OpenSessionRequest_MODE_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "mode is required")
	}
	if req.GetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "path is required")
	}

	meta, err := gc.sm.CreateSession(req)
	if err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			return nil, status.Errorf(codes.NotFound, "path %q not found", req.GetPath())
		case errors.Is(err, os.ErrPermission):
			return nil, status.Errorf(codes.PermissionDenied, "access denied to %q: %v", req.GetPath(), err)
		case errors.Is(err, errNotRegularFile):
			return nil, status.Errorf(codes.InvalidArgument, "path %q is not a file", req.GetPath())
		default:
			return nil, status.Errorf(codes.Internal, "create session: %v", err)
		}
	}

	sessionID := make([]byte, len(meta.ID))
	copy(sessionID, meta.ID[:])
	token := make([]byte, len(meta.Token))
	copy(token, meta.Token)

	resp := &pb.OpenSessionResponse{
		SessionId:  sessionID,
		Token:      token,
		ServerHost: gc.sm.UDPHost(meta),
		ServerPort: uint32(meta.LocalAddr.Port),
		StreamIds:  append([]uint32(nil), meta.StreamIDs...),
		MtuHint:    meta.MTU,
		TotalSize:  meta.TotalSize,
		TtlSeconds: meta.TTLSeconds,
	}
	return resp, nil
}

func (gc *GudpControl) CloseSession(ctx context.Context, req *pb.CloseSessionRequest) (*pb.CloseSessionResponse, error) {
	if req == nil || len(req.GetSessionId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	sessionUUID, err := uuid.FromBytes(req.GetSessionId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid session id: %v", err)
	}

	gc.sm.CloseSession(sessionUUID.String())
	return &pb.CloseSessionResponse{Ok: true}, nil
}

func (gc *GudpControl) PrepareForTransfer(mode pb.OpenSessionRequest_Mode, path string) {
	// Placeholder for the actual filesystem/open-file logic that will decide
	// what backend reader/writer we need. Right now we rely on the client to
	// handle path validation so that we can flesh out the UDP data plane.
}
