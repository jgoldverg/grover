package gclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/filesystem"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	groverpb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type FilesAPI interface {
	List(ctx context.Context, endpoint backend.Endpoint) ([]filesystem.FileInfo, error)
	Remove(ctx context.Context, endpoint backend.Endpoint, path string) error
	Mkdir(ctx context.Context, endpoint backend.Endpoint, path string) error
	Rename(ctx context.Context, endpoint backend.Endpoint, oldPath, newPath string) error
}

type CredentialsAPI interface {
	AddCredential(ctx context.Context, cred backend.Credential) error
	ListCredentials(ctx context.Context, credType string) ([]backend.Credential, error)
	DeleteCredential(ctx context.Context, credUUID uuid.UUID, credName string) error
}

type TransferAPI interface {
	LaunchTransfer(ctx context.Context, req *backend.TransferRequest) (*pb.FileTransferResponse, error)
}

type MTUAPI interface {
	DiscoverPMTU(ctx context.Context, server string, port int, minSize, maxSize int, perTry time.Duration) (int, error)
}

type ServerAPI interface {
	CreatePorts(ctx context.Context, portCount uint32) ([]uint32, error)
	DeletePorts(ctx context.Context, ports []uint32) (bool, error)
	ListPorts(ctx context.Context) ([]uint32, error)
	StartServer(ctx context.Context) (uint32, error)
	StopServer(ctx context.Context) (string, error)
}

type Client struct {
	cfg        internal.AppConfig
	conn       *grpc.ClientConn
	udpManager *pkg.ListenerManager

	mtu MTUAPI

	files       FilesAPI
	credentials CredentialsAPI
	transfer    TransferAPI
	server      ServerAPI
	heartbeat   *HeartBeatService
}

func NewClient(cfg internal.AppConfig) *Client {
	return &Client{
		cfg:        cfg,
		udpManager: pkg.NewListenerManager(),
		mtu:        NewMTUService(),
	}
}

func (c *Client) Files() FilesAPI { return c.files }

func (c *Client) Credentials() CredentialsAPI { return c.credentials }

func (c *Client) Transfer() TransferAPI { return c.transfer }

func (c *Client) Server() ServerAPI { return c.server }

func (c *Client) Heartbeat() *HeartBeatService { return c.heartbeat }

func (c *Client) MTU() MTUAPI { return c.mtu }

func (c *Client) Initialize(ctx context.Context, policy util.RoutePolicy) error {
	var (
		cc         *grpc.ClientConn         // the real conn pointer (may stay nil)
		ci         grpc.ClientConnInterface // interface we pass to services
		err        error
		wantRemote = policy == util.RouteForceRemote ||
			(policy == util.RouteAuto && strings.TrimSpace(c.cfg.ServerURL) != "")
	)

	if wantRemote {
		cc, err = c.dialTLS(ctx, c.cfg.ServerURL, c.cfg.CACertFile)
		if err != nil {
			return err
		}
		ci = cc
	}

	c.conn = cc

	var e error
	c.credentials, e = NewCredentialService(&c.cfg, ci, policy)
	if e != nil {
		return e
	}
	c.transfer, e = NewClientTransferService(&c.cfg, ci, policy)
	if e != nil {
		return e
	}
	if c.conn != nil {
		c.server = NewServerService(&c.cfg, c.conn)
		c.heartbeat = NewHeartBeatService(&c.cfg, c.conn)
	} else {
		c.server = nil
		c.heartbeat = nil
	}

	fileStore, err := backend.NewTomlCredentialStorage(c.cfg.CredentialsFile)
	if err != nil {
		return err
	}
	var transferClient pb.TransferServiceClient
	var fileServiceClient groverpb.FileServiceClient
	if ci != nil {
		transferClient = pb.NewTransferServiceClient(ci)
		fileServiceClient = groverpb.NewFileServiceClient(ci)
	}
	c.files = NewFileService(transferClient, fileServiceClient, fileStore)
	return nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) dialTLS(ctx context.Context, target, caPath string) (*grpc.ClientConn, error) {
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
