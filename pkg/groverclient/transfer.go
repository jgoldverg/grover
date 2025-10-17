package groverclient

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"google.golang.org/grpc"
)

type TransferService struct {
	cfg            *internal.AppConfig
	remote         pb.TransferServiceClient
	policy         RoutePolicy
	hasRemote      bool
	store          backend.CredentialStorage
	jobPersistence backend.JobPersistence
}

func NewClientTransferService(cfg *internal.AppConfig, conn grpc.ClientConnInterface, policy RoutePolicy) (*TransferService, error) {
	store, err := backend.NewTomlCredentialStorage(cfg.CredentialsFile)
	if err != nil {
		return nil, err
	}
	svc := &TransferService{
		cfg:    cfg,
		policy: policy,
		store:  store,
	}
	if conn != nil {
		svc.remote = pb.NewTransferServiceClient(conn)
		svc.hasRemote = true
	}

	return svc, nil
}

func (s *TransferService) LaunchTransfer(ctx context.Context, req *backend.TransferRequest) (*pb.FileTransferResponse, error) {
	if req == nil {
		return nil, errors.New("transfer request cannot be nil")
	}
	if ShouldUseRemote(s.policy, s.hasRemote) {
		if s.remote == nil {
			return nil, ErrRemoteUnavailable
		}
		pbReq, err := backendTransferRequestToProto(req)
		if err != nil {
			return nil, err
		}
		return s.remote.LaunchFileTransfer(ctx, pbReq)
	}

	// TODO make the job persistence type a part of the config here instead of hard coding Noop
	executor := backend.NewTransferExecutor(req, s.store, backend.NoopStore)
	executor.PrepareTransfer()
	executor.StartTransfer(ctx)
	return nil, ErrFileTransferNotImplemented
}

func backendTransferRequestToProto(req *backend.TransferRequest) (*pb.FileTransferRequest, error) {
	sources := make([]*pb.Endpoint, len(req.Sources))
	for i, src := range req.Sources {
		sources[i] = &pb.Endpoint{
			Raw:            strings.TrimSpace(src.Raw),
			Scheme:         strings.TrimSpace(src.Scheme),
			Paths:          trimmedPaths(src.Paths),
			CredentialHint: strings.TrimSpace(src.CredentialHint),
			CredentialId:   strings.TrimSpace(src.CredentialID),
		}
	}
	dests := make([]*pb.Endpoint, len(req.Destinations))
	for i, dst := range req.Destinations {
		dests[i] = &pb.Endpoint{
			Raw:            strings.TrimSpace(dst.Raw),
			Scheme:         strings.TrimSpace(dst.Scheme),
			Paths:          trimmedPaths(dst.Paths),
			CredentialHint: strings.TrimSpace(dst.CredentialHint),
			CredentialId:   strings.TrimSpace(dst.CredentialID),
		}
	}
	edges := make([]*pb.TransferEdge, len(req.Edges))
	for i, edge := range req.Edges {
		pbEdge := &pb.TransferEdge{
			SourceIndex: uint32(edge.SourceIndex),
			DestIndex:   uint32(edge.DestIndex),
			SourcePath:  strings.TrimSpace(edge.SourcePath),
			DestPath:    strings.TrimSpace(edge.DestPath),
		}
		if len(edge.Options) > 0 {
			keys := make([]string, 0, len(edge.Options))
			for k := range edge.Options {
				keys = append(keys, strings.ToLower(strings.TrimSpace(k)))
			}
			sort.Strings(keys)
			opts := make([]*pb.TransferEdgeOption, 0, len(keys))
			for _, k := range keys {
				opts = append(opts, &pb.TransferEdgeOption{
					Key:   k,
					Value: strings.TrimSpace(edge.Options[k]),
				})
			}
			pbEdge.Options = opts
		}
		edges[i] = pbEdge
	}

	params, err := convertTransferParamsToProto(req.Params)
	if err != nil {
		return nil, err
	}

	return &pb.FileTransferRequest{
		Sources:        sources,
		Destinations:   dests,
		Edges:          edges,
		Params:         params,
		IdempotencyKey: strings.TrimSpace(req.IdempotencyKey),
		DeleteSource:   req.DeleteSource,
	}, nil
}

func trimmedPaths(paths []string) []string {
	out := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			out = append(out, trimmed)
		}
	}
	return out
}

func convertTransferParamsToProto(in backend.TransferParams) (*pb.FileTransferParams, error) {
	overwrite, err := convertOverwritePolicyToProto(in.Overwrite)
	if err != nil {
		return nil, err
	}
	checksum, err := convertChecksumTypeToProto(in.Checksum)
	if err != nil {
		return nil, err
	}
	return &pb.FileTransferParams{
		Concurrency:    in.Concurrency,
		Parallelism:    in.Parallelism,
		Pipelining:     in.Pipelining,
		ChunkSize:      in.ChunkSize,
		RateLimitMbps:  in.RateLimitMbps,
		Overwrite:      overwrite,
		ChecksumType:   checksum,
		VerifyChecksum: in.VerifyChecksum,
		MaxRetries:     in.MaxRetries,
		RetryBackoffMs: in.RetryBackoffMs,
	}, nil
}

func convertOverwritePolicyToProto(in backend.OverwritePolicy) (pb.OverwritePolicy, error) {
	switch in {
	case backend.UNSPECIFIED:
		return pb.OverwritePolicy_OVERWRITE_UNSPECIFIED, nil
	case backend.ALWAYS:
		return pb.OverwritePolicy_OVERWRITE_ALWAYS, nil
	case backend.IF_NEWER:
		return pb.OverwritePolicy_OVERWRITE_IF_NEWER, nil
	case backend.NEVER:
		return pb.OverwritePolicy_OVERWRITE_NEVER, nil
	case backend.IF_DIFFERENT:
		return pb.OverwritePolicy_OVERWRITE_IF_DIFFERENT, nil
	default:
		return 0, fmt.Errorf("unsupported overwrite policy %d", in)
	}
}

func convertChecksumTypeToProto(in backend.CheckSumType) (pb.ChecksumType, error) {
	switch in {
	case backend.NONE:
		return pb.ChecksumType_CHECKSUM_NONE, nil
	case backend.MD5:
		return pb.ChecksumType_CHECKSUM_MD5, nil
	case backend.SHA256SUM:
		return pb.ChecksumType_CHECKSUM_SHA256, nil
	case backend.XXH3:
		return pb.ChecksumType_CHECKSUM_XXH3, nil
	default:
		return 0, fmt.Errorf("unsupported checksum type %d", in)
	}
}
