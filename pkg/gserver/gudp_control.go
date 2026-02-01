package gserver

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GudpControl implements the TransferControl gRPC service and delegates the
// heavy lifting to the ServerSessions.
type GudpControl struct {
	pb.UnimplementedTransferControlServer

	sm *ServerSessions
}

func NewGUdpControl(cfg *internal.ServerConfig) *GudpControl {
	return &GudpControl{
		sm: NewServerSessions(cfg),
	}
}

func (gc *GudpControl) OpenSession(ctx context.Context, req *pb.OpenSessionRequest) (*pb.OpenSessionResponse, error) {
	internal.Info("Opening Session ServerConfig", nil)
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
		ServerPort: func() uint32 {
			if addr := meta.LocalAddr(); addr != nil {
				return uint32(addr.Port)
			}
			return 0
		}(),
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

func (gc *GudpControl) LeaseStream(ctx context.Context, req *pb.LeaseStreamRequest) (*pb.LeaseStreamResponse, error) {
	if req == nil || len(req.GetSessionId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	sessionUUID, err := uuid.FromBytes(req.GetSessionId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid session id: %v", err)
	}

	_, binding, err := gc.sm.LeaseStream(sessionUUID.String(), req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "lease stream: %v", err)
	}

	leaseBytes := append([]byte(nil), binding.leaseID[:]...)
	return &pb.LeaseStreamResponse{
		StreamId: binding.streamID,
		LeaseId:  leaseBytes,
	}, nil
}

func (gc *GudpControl) ReleaseStream(ctx context.Context, req *pb.ReleaseStreamRequest) (*pb.ReleaseStreamResponse, error) {
	if req == nil || len(req.GetSessionId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	if len(req.GetLeaseId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "lease_id is required")
	}
	sessionUUID, err := uuid.FromBytes(req.GetSessionId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid session id: %v", err)
	}
	leaseUUID, err := uuid.FromBytes(req.GetLeaseId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid lease id: %v", err)
	}

	if _, err := gc.sm.ReleaseStream(sessionUUID.String(), req.GetStreamId(), leaseUUID, req.GetCommit()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "release stream: %v", err)
	}

	return &pb.ReleaseStreamResponse{Ok: true}, nil
}

func (gc *GudpControl) EnumeratePath(ctx context.Context, req *pb.EnumeratePathRequest) (*pb.EnumeratePathResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	path := strings.TrimSpace(req.GetPath())
	if path == "" {
		return nil, status.Error(codes.InvalidArgument, "path is required")
	}

	files, err := enumeratePath(path, req.GetRecursive())
	if err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			return nil, status.Errorf(codes.NotFound, "path %q not found", path)
		case errors.Is(err, os.ErrPermission):
			return nil, status.Errorf(codes.PermissionDenied, "access denied to %q", path)
		default:
			return nil, status.Errorf(codes.Internal, "enumerate path: %v", err)
		}
	}

	return &pb.EnumeratePathResponse{Files: files}, nil
}

func enumeratePath(root string, recursive bool) ([]*pb.FileSpec, error) {
	info, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	cleanRoot := filepath.Clean(root)
	if !info.IsDir() {
		return []*pb.FileSpec{{
			FullPath:     cleanRoot,
			RelativePath: filepath.Base(cleanRoot),
			Size:         uint64(info.Size()),
		}}, nil
	}

	if !recursive {
		entries, err := os.ReadDir(cleanRoot)
		if err != nil {
			return nil, err
		}
		out := make([]*pb.FileSpec, 0, len(entries))
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			info, err := e.Info()
			if err != nil {
				return nil, err
			}
			fp := filepath.Join(cleanRoot, e.Name())
			out = append(out, &pb.FileSpec{
				FullPath:     fp,
				RelativePath: filepath.ToSlash(e.Name()),
				Size:         uint64(info.Size()),
			})
		}
		return out, nil
	}

	files := make([]*pb.FileSpec, 0, 128)
	err = filepath.WalkDir(cleanRoot, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(cleanRoot, p)
		if err != nil {
			return err
		}
		files = append(files, &pb.FileSpec{
			FullPath:     p,
			RelativePath: filepath.ToSlash(rel),
			Size:         uint64(info.Size()),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
