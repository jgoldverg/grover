// Package gclient provides the public API to using the grover protocol client
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
	groverpb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"github.com/jgoldverg/grover/pkg/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
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

type RoutedTransferAPI interface {
	PrepareTransferEndpoint(ctx context.Context, req *groverpb.PrepareTransferEndpointRequest) (*groverpb.TransferEndpoint, error)
	StartTransferJob(ctx context.Context, req *groverpb.StartTransferJobRequest) (*groverpb.TransferJob, error)
	GetTransferJob(ctx context.Context, jobID string) (*groverpb.TransferJob, error)
	ListTransferJobs(ctx context.Context, routeID string) ([]*groverpb.TransferJob, error)
	AbortTransferJob(ctx context.Context, jobID string) (*groverpb.TransferJob, error)
	UpdateTransferConcurrency(ctx context.Context, jobID string, filesInFlight, streamsPerFile uint32) (*groverpb.TransferJob, error)
	UpdateTransferTuning(ctx context.Context, req *groverpb.UpdateTransferTuningRequest) (*groverpb.TransferJob, error)
	StreamTransferStats(ctx context.Context, jobID, routeID string) (groverpb.TransferJobControl_StreamTransferStatsClient, error)
}

type RelayControlAPI interface {
	CreateForward(ctx context.Context, req *groverpb.CreateForwardRequest) (*groverpb.ForwardSession, error)
	GetForward(ctx context.Context, forwardID string) (*groverpb.ForwardSession, error)
	ListForwards(ctx context.Context, routeID, jobID string) ([]*groverpb.ForwardSession, error)
	DeleteForward(ctx context.Context, forwardID string) (bool, error)
	StreamForwardStats(ctx context.Context, forwardID, routeID, jobID string) (groverpb.RelayControl_StreamForwardStatsClient, error)
}

type RouteSessionAPI interface {
	CreateRouteSession(ctx context.Context, req *groverpb.CreateRouteSessionRequest) (*groverpb.RouteSession, error)
	GetRouteSession(ctx context.Context, sessionID string) (*groverpb.RouteSession, error)
	ListRouteSessions(ctx context.Context, routeID, jobID string) ([]*groverpb.RouteSession, error)
	DeleteRouteSession(ctx context.Context, sessionID string) (bool, error)
	AbortRouteSession(ctx context.Context, sessionID string) (*groverpb.RouteSession, error)
	UpdateRouteSessionState(ctx context.Context, sessionID string, state groverpb.RuntimeState, errText string) (*groverpb.RouteSession, error)
	StreamRouteSessionStats(ctx context.Context, sessionID, routeID, jobID string) (groverpb.RouteSessionControl_StreamRouteSessionStatsClient, error)
}

type RouteConfigAPI interface {
	PutRoute(ctx context.Context, route *groverpb.RouteConfig) (*groverpb.RouteConfig, error)
	GetRoute(ctx context.Context, name string) (*groverpb.RouteConfig, error)
	ListRoutes(ctx context.Context) ([]*groverpb.RouteConfig, error)
	DeleteRoute(ctx context.Context, name string) (bool, error)
}

type JobHistoryAPI interface {
	ListJobHistory(ctx context.Context, req *groverpb.ListJobHistoryRequest) ([]*groverpb.JobHistoryEntry, error)
	GetJobManifest(ctx context.Context, jobID string) (*groverpb.JobHistoryManifest, error)
	GetJobFinal(ctx context.Context, jobID string) (*groverpb.TransferJob, error)
	ListJobSnapshots(ctx context.Context, req *groverpb.ListJobSnapshotsRequest) ([]*groverpb.TransferJob, error)
	ListJobEnergy(ctx context.Context, req *groverpb.ListJobEnergyRequest) (*groverpb.ListJobEnergyResponse, error)
}

type Client struct {
	cfg  internal.AppConfig
	conn *grpc.ClientConn

	files       FilesAPI
	credentials CredentialsAPI
	routed      RoutedTransferAPI
	relay       RelayControlAPI
	route       RouteSessionAPI
	routeConfig RouteConfigAPI
	history     JobHistoryAPI
}

func NewClient(cfg internal.AppConfig) *Client {
	return &Client{cfg: cfg}
}

func (c *Client) Files() FilesAPI { return c.files }

func (c *Client) Credentials() CredentialsAPI { return c.credentials }

func (c *Client) RoutedTransfer() RoutedTransferAPI { return c.routed }

func (c *Client) RelayControl() RelayControlAPI { return c.relay }

func (c *Client) RouteSessionControl() RouteSessionAPI { return c.route }

func (c *Client) RouteConfigControl() RouteConfigAPI { return c.routeConfig }

func (c *Client) JobHistoryControl() JobHistoryAPI { return c.history }

func (c *Client) Initialize(ctx context.Context, policy util.RoutePolicy) error {
	var (
		cc         *grpc.ClientConn         // the real conn pointer (may stay nil)
		ci         grpc.ClientConnInterface // interface we pass to services
		err        error
		wantRemote = policy == util.RouteForceRemote ||
			(policy == util.RouteAuto && strings.TrimSpace(c.cfg.ServerURL) != "")
	)

	if wantRemote {
		internal.Debug("control-plane dial started", internal.Fields{
			"server_url":   c.cfg.ServerURL,
			"ca_cert_file": c.cfg.CACertFile,
			"insecure":     c.cfg.InsecureControl,
		})
		started := time.Now()
		cc, err = c.dialControl(ctx, c.cfg.ServerURL, c.cfg.CACertFile, c.cfg.InsecureControl)
		if err != nil {
			return err
		}
		if err := waitForReady(ctx, cc, 10*time.Second); err != nil {
			_ = cc.Close()
			return err
		}
		internal.Debug("control-plane connection ready", internal.Fields{
			"server_url": c.cfg.ServerURL,
			"state":      cc.GetState().String(),
			"elapsed_ms": time.Since(started).Milliseconds(),
		})

		ci = cc
	}

	c.conn = cc

	var e error
	c.credentials, e = NewCredentialService(&c.cfg, ci, policy)
	if e != nil {
		return e
	}
	if c.conn != nil {
		c.routed = NewRoutedTransferService(c.conn)
		c.relay = NewRelayControlService(c.conn)
		c.route = NewRouteSessionService(c.conn)
		c.routeConfig = NewRouteConfigService(c.conn)
		c.history = NewJobHistoryService(c.conn)
	} else {
		c.routed = nil
		c.relay = nil
		c.route = nil
		c.routeConfig = nil
		c.history = nil
	}

	fileStore, err := backend.NewTomlCredentialStorage(c.cfg.CredentialsFile)
	if err != nil {
		return err
	}
	var fileServiceClient groverpb.FileServiceClient
	if ci != nil {
		fileServiceClient = groverpb.NewFileServiceClient(ci)
	}
	c.files = NewFileService(c, fileServiceClient, fileStore)
	return nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) dialControl(ctx context.Context, target, caPath string, insecureControl bool) (*grpc.ClientConn, error) {
	if insecureControl {
		return grpc.NewClient(
			target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}
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
		_, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}

	return grpc.NewClient(
		target,
		grpc.WithTransportCredentials(creds),
	)
}

func waitForReady(ctx context.Context, cc *grpc.ClientConn, timeout time.Duration) error {
	if _, ok := ctx.Deadline(); !ok && timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	cc.Connect()
	for {
		state := cc.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !cc.WaitForStateChange(ctx, state) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return context.DeadlineExceeded
		}
	}
}
