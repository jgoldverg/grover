package control

import (
	"context"
	"fmt"
	"strings"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TransferService struct {
	pb.UnimplementedTransferServiceServer
	registry  *backend.TransferRegistry
	engine    backend.TransportEngine
	store     backend.CredentialStorage
	cfg       *internal.ServerConfig
	credStore backend.CredentialStorage
}

func NewTransferService(config *internal.ServerConfig, credStore backend.CredentialStorage, registry *backend.TransferRegistry, engine backend.TransportEngine) *TransferService {
	return &TransferService{
		registry: registry,
		engine:   engine,
		store:    credStore,
		cfg:      config,
	}
}

func (ts *TransferService) LaunchFileTransfer(ctx context.Context, req *pb.FileTransferRequest) (*pb.FileTransferResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "transfer request cannot be nil")
	}
	backendReq, err := NormalizeTransferRequest(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	exec := backend.NewTransferExecutor(backendReq, ts.store, backend.NoopStore)
	jobCtx, cancel := context.WithCancel(ctx)

	jobID, err := ts.registry.Create(ctx, backendReq, exec, cancel, 0)
	if err != nil {
		cancel()
		return nil, status.Errorf(codes.Internal, "register transfer job: %v", err)
	}

	registration, err := ts.engine.RegisterJob(jobCtx, jobID, exec)
	if err != nil {
		ts.registry.Cancel(jobID)
		_ = ts.registry.Complete(ctx, jobID, backend.StatusFailed, fmt.Sprintf("transport registration failed: %v", err))
		cancel()
		return nil, status.Errorf(codes.Internal, "transport registration failed: %v", err)
	}

	ts.registry.SetUDPPort(jobID, registration.Port)

	internal.Info("transfer job registered for udp session", internal.Fields{
		internal.FieldKey("job_id"): jobID.String(),
		internal.FieldPort:          registration.Port,
	})

	return &pb.FileTransferResponse{
		TransferId:   jobID.String(),
		Accepted:     true,
		UdpPort:      int32(registration.Port),
		SessionToken: registration.SessionToken,
	}, nil
}

func NormalizeTransferRequest(req *pb.FileTransferRequest) (*backend.TransferRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("transfer request cannot be nil")
	}
	sources := req.GetSources()
	destinations := req.GetDestinations()
	edges := req.GetEdges()

	if len(sources) == 0 {
		return nil, fmt.Errorf("at least one source endpoint is required")
	}
	if len(destinations) == 0 {
		return nil, fmt.Errorf("at least one destination endpoint is required")
	}
	if len(edges) == 0 {
		return nil, fmt.Errorf("at least one transfer edge is required")
	}

	backendSources := make([]backend.Endpoint, len(sources))
	for i, ep := range sources {
		backendSources[i] = backend.Endpoint{
			Raw:            strings.TrimSpace(ep.GetRaw()),
			Scheme:         strings.TrimSpace(ep.GetScheme()),
			Paths:          trimmedStrings(ep.GetPaths()),
			CredentialHint: strings.TrimSpace(ep.GetCredentialHint()),
			CredentialID:   strings.TrimSpace(ep.GetCredentialId()),
		}
	}

	backendDestinations := make([]backend.Endpoint, len(destinations))
	for i, ep := range destinations {
		backendDestinations[i] = backend.Endpoint{
			Raw:            strings.TrimSpace(ep.GetRaw()),
			Scheme:         strings.TrimSpace(ep.GetScheme()),
			Paths:          trimmedStrings(ep.GetPaths()),
			CredentialHint: strings.TrimSpace(ep.GetCredentialHint()),
			CredentialID:   strings.TrimSpace(ep.GetCredentialId()),
		}
	}

	backendEdges := make([]backend.TransferEdge, len(edges))
	for i, edge := range edges {
		srcIdx := int(edge.GetSourceIndex())
		dstIdx := int(edge.GetDestIndex())
		if srcIdx < 0 || srcIdx >= len(backendSources) {
			return nil, fmt.Errorf("edge %d references invalid source index %d", i, srcIdx)
		}
		if dstIdx < 0 || dstIdx >= len(backendDestinations) {
			return nil, fmt.Errorf("edge %d references invalid destination index %d", i, dstIdx)
		}
		opts := map[string]string{}
		for _, opt := range edge.GetOptions() {
			key := strings.TrimSpace(opt.GetKey())
			if key == "" {
				continue
			}
			opts[strings.ToLower(key)] = strings.TrimSpace(opt.GetValue())
		}
		backendEdges[i] = backend.TransferEdge{
			SourceIndex: srcIdx,
			DestIndex:   dstIdx,
			SourcePath:  strings.TrimSpace(edge.GetSourcePath()),
			DestPath:    strings.TrimSpace(edge.GetDestPath()),
			Options:     opts,
		}
	}

	params := req.GetParams()
	backendParams := backend.TransferParams{}
	if params != nil {
		backendParams.Concurrency = params.GetConcurrency()
		backendParams.Parallelism = params.GetParallelism()
		backendParams.Pipelining = params.GetPipelining()
		backendParams.ChunkSize = params.GetChunkSize()
		backendParams.RateLimitMbps = params.GetRateLimitMbps()
		backendParams.Overwrite = convertOverwritePolicy(params.GetOverwrite())
		backendParams.Checksum = convertChecksumType(params.GetChecksumType())
		backendParams.VerifyChecksum = params.GetVerifyChecksum()
		backendParams.MaxRetries = params.GetMaxRetries()
		backendParams.RetryBackoffMs = params.GetRetryBackoffMs()
	}

	return &backend.TransferRequest{
		Sources:        backendSources,
		Destinations:   backendDestinations,
		Edges:          backendEdges,
		Params:         backendParams,
		IdempotencyKey: strings.TrimSpace(req.GetIdempotencyKey()),
		DeleteSource:   req.GetDeleteSource(),
	}, nil
}

func convertOverwritePolicy(in pb.OverwritePolicy) backend.OverwritePolicy {
	switch in {
	case pb.OverwritePolicy_OVERWRITE_ALWAYS:
		return backend.ALWAYS
	case pb.OverwritePolicy_OVERWRITE_IF_NEWER:
		return backend.IF_NEWER
	case pb.OverwritePolicy_OVERWRITE_NEVER:
		return backend.NEVER
	case pb.OverwritePolicy_OVERWRITE_IF_DIFFERENT:
		return backend.IF_DIFFERENT
	default:
		return backend.UNSPECIFIED
	}
}

func convertChecksumType(in pb.ChecksumType) backend.CheckSumType {
	switch in {
	case pb.ChecksumType_CHECKSUM_MD5:
		return backend.MD5
	case pb.ChecksumType_CHECKSUM_SHA256:
		return backend.SHA256SUM
	case pb.ChecksumType_CHECKSUM_XXH3:
		return backend.XXH3
	default:
		return backend.NONE
	}
}

func trimmedStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			out = append(out, trimmed)
		}
	}
	return out
}
