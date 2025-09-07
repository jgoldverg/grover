package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"strings"
	"time"

	"github.com/jgoldverg/grover/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type GroverClient struct {
	CredService     *CredentialService
	ResourceService *FileResourceService

	conn *grpc.ClientConn
	cfg  config.AppConfig
}

func NewGroverClient(cfg config.AppConfig) *GroverClient { return &GroverClient{cfg: cfg} }

func (c *GroverClient) Initialize(ctx context.Context, policy RoutePolicy) error {
	var (
		cc         *grpc.ClientConn         // the real conn pointer (may stay nil)
		ci         grpc.ClientConnInterface // interface we pass to services
		err        error
		wantRemote = policy == RouteForceRemote ||
			(policy == RouteAuto && strings.TrimSpace(c.cfg.ServerURL) != "")
	)

	if wantRemote {
		cc, err = c.dialTLS(ctx, c.cfg.ServerURL, c.cfg.CACertFile)
		if err != nil {
			return err
		}
		ci = cc // only set the interface when we actually have a conn
	}

	c.conn = cc

	var e error
	c.ResourceService, e = NewFileResourceService(&c.cfg, ci, policy)
	if e != nil {
		return e
	}

	c.CredService, e = NewCredentialService(&c.cfg, ci, policy)
	if e != nil {
		return e
	}

	return nil
}

func (c *GroverClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *GroverClient) dialTLS(ctx context.Context, target, caPath string) (*grpc.ClientConn, error) {
	// Build root pool: system roots by default; add custom CA if provided.
	roots, _ := x509.SystemCertPool()
	if caPath != "" {
		pem, err := os.ReadFile(os.ExpandEnv(caPath))
		if err != nil {
			return nil, err
		}
		if roots == nil {
			roots = x509.NewCertPool()
		}
		roots.AppendCertsFromPEM(pem)
	}
	creds := credentials.NewTLS(&tls.Config{RootCAs: roots})

	// Give dialing a sane default timeout if the caller didn’t.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}

	return grpc.NewClient(
		target,
		grpc.WithTransportCredentials(creds),
	)
}
