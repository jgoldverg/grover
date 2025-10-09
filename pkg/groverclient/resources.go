package groverclient

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/filesystem"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/protoutil"
	"google.golang.org/grpc"
)

type FileResourceService struct {
	fileServiceClient pb.FileServiceClient
	storage           backend.CredentialStorage
	policy            RoutePolicy
	hasRemote         bool
}

var ErrRemoteUnavailable = errors.New("remote route requested but server_url not configured")

func NewFileResourceService(cfg *internal.AppConfig, conn grpc.ClientConnInterface, policy RoutePolicy) (*FileResourceService, error) {
	store, err := backend.NewTomlCredentialStorage(cfg.CredentialsFile)
	if err != nil {
		return nil, err
	}
	var (
		api       pb.FileServiceClient
		hasRemote bool
	)
	if conn != nil {
		api = pb.NewFileServiceClient(conn)
		hasRemote = true
	}
	return &FileResourceService{
		fileServiceClient: api,
		storage:           store,
		policy:            policy,
		hasRemote:         hasRemote,
	}, nil
}

func (s *FileResourceService) Rm(ctx context.Context, bt backend.BackendType, path string, credName string, credUUID uuid.UUID) (bool, error) {
	if ShouldUseRemote(s.policy, s.hasRemote) {
		if s.fileServiceClient == nil {
			return false, ErrRemoteUnavailable
		}
		req := buildRmRequest(bt, path, credName, credUUID)
		resp, err := s.fileServiceClient.Remove(ctx, req)
		if err != nil {
			return false, err
		}
		return resp.GetSuccess(), nil
	}

	cred, err := s.resolveLocalCredential(credName, credUUID)
	if err != nil {
		return false, err
	}
	ops, err := backend.OpsFactory(bt, cred)
	if err != nil {
		return false, err
	}
	if err := ops.Remove(ctx, path); err != nil {
		return false, err
	}
	return true, nil
}

func (s *FileResourceService) List(ctx context.Context, bt backend.BackendType, path string, credName string, credUUID uuid.UUID) ([]filesystem.FileInfo, error) {
	if ShouldUseRemote(s.policy, s.hasRemote) {
		if s.fileServiceClient == nil {
			return nil, ErrRemoteUnavailable
		}
		req := buildListRequest(bt, path, credName, credUUID)
		resp, err := s.fileServiceClient.List(ctx, req)
		if err != nil {
			return nil, err
		}
		return protoutil.PbFilesToFsFiles(resp.GetFiles()), nil
	}

	cred, err := s.resolveLocalCredential(credName, credUUID)
	if err != nil {
		return nil, err
	}
	ops, err := backend.OpsFactory(bt, cred)
	if err != nil {
		return nil, err
	}
	return ops.List(ctx, path)
}

func (s *FileResourceService) resolveLocalCredential(name string, id uuid.UUID) (backend.Credential, error) {
	name = strings.TrimSpace(name)
	switch {
	case id != uuid.Nil:
		return s.storage.GetCredentialByUUID(id)
	case name != "":
		return s.storage.GetCredentialByName(name)
	default:
		return nil, nil
	}
}

func buildRmRequest(bt backend.BackendType, path, name string, id uuid.UUID) *pb.RemoveFileRequest {
	req := &pb.RemoveFileRequest{
		Type: protoutil.BackendTypeToPbType(bt),
		Path: path,
	}
	req.CredentialRef = protoutil.BuildCredentialRef(name, id)
	return req
}

func buildListRequest(bt backend.BackendType, path, name string, id uuid.UUID) *pb.ListFilesRequest {
	req := &pb.ListFilesRequest{
		Type: protoutil.BackendTypeToPbType(bt),
		Path: path,
	}
	req.CredentialRef = protoutil.BuildCredentialRef(name, id)
	return req
}
