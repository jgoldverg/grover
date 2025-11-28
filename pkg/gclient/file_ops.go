package gclient

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/filesystem"
	groverpb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/util"
)

var ErrFileServiceNotImplemented = errors.New("file service functionality not implemented yet")

type FileService struct {
	fileSvc   groverpb.FileServiceClient
	credStore backend.CredentialStorage
	client    *Client
}

func NewFileService(client *Client, fileSvc groverpb.FileServiceClient, credStore backend.CredentialStorage) *FileService {
	return &FileService{
		fileSvc:   fileSvc,
		credStore: credStore,
		client:    client,
	}
}

func (c *FileService) List(ctx context.Context, endpoint backend.Endpoint) ([]filesystem.FileInfo, error) {
	path := firstEndpointPath(endpoint)
	bt := backendTypeFromEndpoint(endpoint)
	credName, credUUID := credentialHints(endpoint)

	if bt == backend.GROVERBackend {
		return c.listGrover(ctx, endpoint, path)
	}
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
	if path == "" {
		path = firstEndpointPath(endpoint)
	}
	bt := backendTypeFromEndpoint(endpoint)
	credName, credUUID := credentialHints(endpoint)

	if bt == backend.GROVERBackend {
		return c.mkdirGrover(ctx, endpoint, path)
	}
	if c.fileSvc != nil {
		req := &groverpb.MkdirRequest{
			Type:          util.BackendTypeToPbType(bt),
			Path:          path,
			CredentialRef: util.BuildCredentialRef(credName, credUUID),
		}
		_, err := c.fileSvc.Mkdir(ctx, req)
		return err
	}

	cred, err := c.resolveCredential(endpoint)
	if err != nil {
		return err
	}
	ops, err := backend.OpsFactory(bt, cred)
	if err != nil {
		return err
	}
	return ops.Mkdir(ctx, path)
}

func (c *FileService) Rename(ctx context.Context, endpoint backend.Endpoint, oldPath, newPath string) error {
	bt := backendTypeFromEndpoint(endpoint)
	credName, credUUID := credentialHints(endpoint)

	if bt == backend.GROVERBackend {
		return c.renameGrover(ctx, endpoint, oldPath, newPath)
	}
	if c.fileSvc != nil {
		req := &groverpb.RenameRequest{
			Type:          util.BackendTypeToPbType(bt),
			OldPath:       oldPath,
			NewPath:       newPath,
			CredentialRef: util.BuildCredentialRef(credName, credUUID),
		}
		_, err := c.fileSvc.Rename(ctx, req)
		return err
	}

	cred, err := c.resolveCredential(endpoint)
	if err != nil {
		return err
	}
	ops, err := backend.OpsFactory(bt, cred)
	if err != nil {
		return err
	}
	return ops.Rename(ctx, oldPath, newPath)
}

func (c *FileService) Remove(ctx context.Context, endpoint backend.Endpoint, path string) error {
	bt := backendTypeFromEndpoint(endpoint)
	credName, credUUID := credentialHints(endpoint)

	if bt == backend.GROVERBackend {
		return c.removeGrover(ctx, endpoint, path)
	}
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
	credName, credUUID := credentialHints(endpoint)
	switch {
	case credUUID != uuid.Nil:
		return c.credStore.GetCredentialByUUID(credUUID)
	case credName != "":
		return c.credStore.GetCredentialByName(credName)
	default:
		return nil, nil
	}
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

func (c *FileService) listGrover(ctx context.Context, endpoint backend.Endpoint, path string) ([]filesystem.FileInfo, error) {
	client, err := c.newGroverClient(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	remoteEndpoint := backend.Endpoint{
		Scheme: string(backend.LOCALFSBackend),
		Paths:  []string{path},
	}
	return client.Files().List(ctx, remoteEndpoint)
}

func (c *FileService) mkdirGrover(ctx context.Context, endpoint backend.Endpoint, path string) error {
	client, err := c.newGroverClient(ctx, endpoint)
	if err != nil {
		return err
	}
	defer client.Close()

	remoteEndpoint := backend.Endpoint{
		Scheme: string(backend.LOCALFSBackend),
		Paths:  []string{path},
	}
	return client.Files().Mkdir(ctx, remoteEndpoint, path)
}

func (c *FileService) renameGrover(ctx context.Context, endpoint backend.Endpoint, oldPath, newPath string) error {
	client, err := c.newGroverClient(ctx, endpoint)
	if err != nil {
		return err
	}
	defer client.Close()

	remoteEndpoint := backend.Endpoint{
		Scheme: string(backend.LOCALFSBackend),
	}
	return client.Files().Rename(ctx, remoteEndpoint, oldPath, newPath)
}

func (c *FileService) removeGrover(ctx context.Context, endpoint backend.Endpoint, path string) error {
	client, err := c.newGroverClient(ctx, endpoint)
	if err != nil {
		return err
	}
	defer client.Close()

	remoteEndpoint := backend.Endpoint{
		Scheme: string(backend.LOCALFSBackend),
		Paths:  []string{path},
	}
	return client.Files().Remove(ctx, remoteEndpoint, path)
}

func (c *FileService) newGroverClient(ctx context.Context, endpoint backend.Endpoint) (*Client, error) {
	if c.client == nil {
		return nil, errors.New("grover backend operations require client context")
	}
	cred, err := c.resolveCredential(endpoint)
	if err != nil {
		return nil, err
	}
	if cred == nil {
		return nil, fmt.Errorf("credential is required for grover backend")
	}
	basic, ok := cred.(*backend.BasicAuthCredential)
	if !ok {
		return nil, fmt.Errorf("credential %q must be basic to access grover backend", cred.GetName())
	}

	remoteCfg := c.client.cfg
	remoteCfg.ServerURL = basic.GetUrl()

	client := NewClient(remoteCfg)
	if err := client.Initialize(ctx, util.RouteForceRemote); err != nil {
		return nil, fmt.Errorf("connect to grover server %q: %w", remoteCfg.ServerURL, err)
	}
	return client, nil
}
