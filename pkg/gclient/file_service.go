package gclient

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/filesystem"
	pbudp "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	groverpb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/util"
)

var ErrFileServiceNotImplemented = errors.New("file service functionality not implemented yet")

type FileService struct {
	transferSvc pbudp.TransferServiceClient
	fileSvc     groverpb.FileServiceClient
	credStore   backend.CredentialStorage
}

func NewFileService(transferSvc pbudp.TransferServiceClient, fileSvc groverpb.FileServiceClient, credStore backend.CredentialStorage) *FileService {
	return &FileService{
		transferSvc: transferSvc,
		fileSvc:     fileSvc,
		credStore:   credStore,
	}
}

func (c *FileService) List(ctx context.Context, endpoint backend.Endpoint) ([]filesystem.FileInfo, error) {
	path := firstEndpointPath(endpoint)
	bt := backendTypeFromEndpoint(endpoint)
	credName, credUUID := credentialHints(endpoint)

	if c.fileSvc != nil {
		req := buildListRequest(bt, path, credName, credUUID)
		resp, err := c.fileSvc.List(ctx, req)
		if err != nil {
			return nil, err
		}
		return util.PbFilesToFsFiles(resp.GetFiles()), nil
	}

	cred, err := c.resolveCredential(endpoint)
	if err != nil {
		return nil, err
	}
	ops, err := backend.OpsFactory(bt, cred)
	if err != nil {
		return nil, err
	}
	return ops.List(ctx, path, false)
}

func (c *FileService) Mkdir(ctx context.Context, endpoint backend.Endpoint, path string) error {
	return ErrFileServiceNotImplemented
}

func (c *FileService) Rename(ctx context.Context, endpoint backend.Endpoint, oldPath, newPath string) error {
	return ErrFileServiceNotImplemented
}

func (c *FileService) Remove(ctx context.Context, endpoint backend.Endpoint, path string) error {
	bt := backendTypeFromEndpoint(endpoint)
	credName, credUUID := credentialHints(endpoint)

	if c.fileSvc != nil {
		req := buildRmRequest(bt, path, credName, credUUID)
		resp, err := c.fileSvc.Remove(ctx, req)
		if err != nil {
			return err
		}
		if !resp.GetSuccess() {
			return errors.New("remote remove operation failed")
		}
		return nil
	}

	cred, err := c.resolveCredential(endpoint)
	if err != nil {
		return err
	}
	ops, err := backend.OpsFactory(bt, cred)
	if err != nil {
		return err
	}
	return ops.Remove(ctx, path)
}

func (c *FileService) resolveCredential(endpoint backend.Endpoint) (backend.Credential, error) {
	if c.credStore == nil {
		return nil, nil
	}
	if id := strings.TrimSpace(endpoint.CredentialID); id != "" {
		return backend.ResolveCredential(c.credStore, id)
	}
	if hint := strings.TrimSpace(endpoint.CredentialHint); hint != "" {
		return backend.ResolveCredential(c.credStore, hint)
	}
	return nil, nil
}

func firstEndpointPath(endpoint backend.Endpoint) string {
	for _, p := range endpoint.Paths {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			return trimmed
		}
	}
	if trimmed := strings.TrimSpace(endpoint.Raw); trimmed != "" {
		return trimmed
	}
	return "/"
}

func backendTypeFromEndpoint(endpoint backend.Endpoint) backend.BackendType {
	bt := backend.BackendType(strings.ToLower(strings.TrimSpace(endpoint.Scheme)))
	if !backend.IsBackendTypeValid(bt) {
		bt = backend.LOCALFSBackend
	}
	return bt
}

func credentialHints(endpoint backend.Endpoint) (string, uuid.UUID) {
	credName := strings.TrimSpace(endpoint.CredentialHint)
	var credUUID uuid.UUID
	if id := strings.TrimSpace(endpoint.CredentialID); id != "" {
		if parsed, err := uuid.Parse(id); err == nil {
			credUUID = parsed
		}
	}
	return credName, credUUID
}

func buildListRequest(bt backend.BackendType, path, name string, id uuid.UUID) *groverpb.ListFilesRequest {
	req := &groverpb.ListFilesRequest{
		Type: util.BackendTypeToPbType(bt),
		Path: path,
	}
	req.CredentialRef = util.BuildCredentialRef(name, id)
	return req
}

func buildRmRequest(bt backend.BackendType, path, name string, id uuid.UUID) *groverpb.RemoveFileRequest {
	req := &groverpb.RemoveFileRequest{
		Type: util.BackendTypeToPbType(bt),
		Path: path,
	}
	req.CredentialRef = util.BuildCredentialRef(name, id)
	return req
}
