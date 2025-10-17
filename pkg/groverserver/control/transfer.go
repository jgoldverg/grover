package control

import (
	"context"
	"fmt"
	"strings"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
)

type TransferService struct {
	pb.UnimplementedTransferServiceServer
}

func NewTransferService(config *internal.ServerConfig) *TransferService {
	return &TransferService{}
}

func (ts *TransferService) LaunchFileTransfer(ctx context.Context, req *pb.FileTransferRequest) (*pb.FileTransferResponse, error) {
	return nil, nil
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
