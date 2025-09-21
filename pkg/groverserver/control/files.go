package control

import (
	"context"
	"fmt"

	"github.com/jgoldverg/grover/backend"
	fs "github.com/jgoldverg/grover/backend/filesystem"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FileService struct {
	pb.UnimplementedFileServiceServer
	storage backend.CredentialStorage
}

// NewFileService constructs the service and returns an error if storage can't be opened.
func NewFileService(cfg *internal.ServerConfig) (*FileService, error) {
	storage, err := backend.NewTomlCredentialStorage(cfg.CredentialsFile)
	if err != nil {
		return nil, err
	}
	return &FileService{storage: storage}, nil
}

func (s *FileService) List(ctx context.Context, in *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	bt := backend.PbTypeToBackendType(in.GetType())
	internal.Debug("list request received", internal.Fields{
		internal.FieldMsg: fmt.Sprintf("%+v", in),
	})
	cred, err := internal.ResolveCredential(s.storage, in.GetCredentialRef())
	if err != nil {
		return nil, err
	}

	ops, err := backend.OpsFactory(bt, cred)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "init backend ops: %v", err)
	}

	files, err := ops.List(ctx, in.GetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}

	return &pb.ListFilesResponse{Files: toPBFiles(files)}, nil
}

func (s *FileService) Remove(ctx context.Context, in *pb.RemoveFileRequest) (*pb.RemoveFileResponse, error) {
	bt := backend.PbTypeToBackendType(in.GetType())

	cred, err := internal.ResolveCredential(s.storage, in.GetCredentialRef())
	if err != nil {
		return nil, err
	}

	ops, err := backend.OpsFactory(bt, cred)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "init backend ops: %v", err)
	}

	if err := ops.Remove(ctx, in.GetPath()); err != nil {
		return &pb.RemoveFileResponse{Success: false}, status.Errorf(codes.Internal, "remove: %v", err)
	}
	return &pb.RemoveFileResponse{Success: true}, nil
}

/* -------------------------- helpers -------------------------- */

func toPBFiles(in []fs.FileInfo) []*pb.FileInfo {
	out := make([]*pb.FileInfo, 0, len(in))
	for _, f := range in {
		out = append(out, &pb.FileInfo{
			Id:   f.ID,
			Path: f.AbsPath, // use f.Path if you renamed the field
			Size: f.Size,
		})
	}
	return out
}
