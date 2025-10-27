package gclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"github.com/jgoldverg/grover/pkg/util"
	"google.golang.org/grpc"
)

type TransferService struct {
	cfg       *internal.AppConfig
	remote    pb.TransferServiceClient
	policy    util.RoutePolicy
	hasRemote bool
	store     backend.CredentialStorage
	registry  *backend.TransferRegistry
}

var ErrRemoteUnavailable = errors.New("remote route requested but server_url not configured")

func NewClientTransferService(cfg *internal.AppConfig, conn grpc.ClientConnInterface, policy util.RoutePolicy) (*TransferService, error) {
	store, err := backend.NewTomlCredentialStorage(cfg.CredentialsFile)
	if err != nil {
		return nil, err
	}
	svc := &TransferService{
		cfg:    cfg,
		policy: policy,
		store:  store,
		registry: backend.NewTransferRegistry(
			backend.JobPersistenceFactory(backend.NoopStore),
		),
	}
	if conn != nil {
		svc.remote = pb.NewTransferServiceClient(conn)
		svc.hasRemote = true
	}

	return svc, nil
}

func (s *TransferService) LaunchTransfer(ctx context.Context, req *backend.TransferRequest) (*pb.FileTransferResponse, error) {
	return nil, ErrFileTransferNotImplemented
}

func (s *TransferService) resolveUDPHost(req *backend.TransferRequest) (string, error) {
	if req != nil {
		for _, dst := range req.Destinations {
			if strings.EqualFold(strings.TrimSpace(dst.Scheme), string(backend.GROVERBackend)) {
				if host := parseHost(dst.Raw); host != "" {
					return host, nil
				}
			}
		}
	}
	if s.cfg != nil {
		if host := parseHost(s.cfg.ServerURL); host != "" {
			return host, nil
		}
	}
	return "", fmt.Errorf("unable to determine udp host for transfer")
}

func parseHost(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.Contains(raw, "://") {
		if u, err := url.Parse(raw); err == nil {
			if host := u.Hostname(); host != "" {
				return host
			}
			return strings.TrimSpace(u.Host)
		}
	}
	if host, _, err := net.SplitHostPort(raw); err == nil {
		return host
	}
	return raw
}

func firstNonEmptyPath(paths []string) string {
	for _, p := range paths {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
