package gserver

import (
	"context"
	"time"

	"github.com/jgoldverg/grover/internal"
	groverPb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RelayControlService struct {
	groverPb.UnimplementedRelayControlServer

	manager *ForwardSessionManager
}

func NewRelayControlService(cfg *internal.ServerConfig) (*RelayControlService, error) {
	manager, err := NewForwardSessionManager(cfg)
	if err != nil {
		return nil, err
	}
	return &RelayControlService{manager: manager}, nil
}

func (s *RelayControlService) CreateForward(ctx context.Context, req *groverPb.CreateForwardRequest) (*groverPb.CreateForwardResponse, error) {
	internal.Info("rpc RelayControl.CreateForward received", internal.Fields{
		"route_id":    req.GetRouteId(),
		"job_id":      req.GetJobId(),
		"hop_index":   req.GetHopIndex(),
		"protocol":    req.GetProtocol().String(),
		"egress":      endpointLabel(req.GetEgress()),
		"ttl_seconds": req.GetTtlSeconds(),
	})
	forward, err := s.manager.Create(ctx, req)
	if err != nil {
		internal.Warn("rpc RelayControl.CreateForward rejected", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          req.GetRouteId(),
			"job_id":            req.GetJobId(),
			"hop_index":         req.GetHopIndex(),
		})
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	internal.Info("rpc RelayControl.CreateForward completed", internal.Fields{
		"route_id":   forward.GetRouteId(),
		"job_id":     forward.GetJobId(),
		"forward_id": forward.GetForwardId(),
		"hop_index":  forward.GetHopIndex(),
		"ingress":    endpointLabel(forward.GetIngress()),
		"egress":     endpointLabel(forward.GetEgress()),
		"state":      forward.GetState().String(),
	})
	return &groverPb.CreateForwardResponse{Forward: forward}, nil
}

func (s *RelayControlService) GetForward(ctx context.Context, req *groverPb.GetForwardRequest) (*groverPb.GetForwardResponse, error) {
	forward, err := s.manager.Get(req.GetForwardId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &groverPb.GetForwardResponse{Forward: forward}, nil
}

func (s *RelayControlService) ListForwards(ctx context.Context, req *groverPb.ListForwardsRequest) (*groverPb.ListForwardsResponse, error) {
	return &groverPb.ListForwardsResponse{
		Forwards: s.manager.List(req.GetRouteId(), req.GetJobId()),
	}, nil
}

func (s *RelayControlService) DeleteForward(ctx context.Context, req *groverPb.DeleteForwardRequest) (*groverPb.DeleteForwardResponse, error) {
	return &groverPb.DeleteForwardResponse{Ok: s.manager.Delete(req.GetForwardId())}, nil
}

func (s *RelayControlService) RenewForward(ctx context.Context, req *groverPb.RenewForwardRequest) (*groverPb.RenewForwardResponse, error) {
	forward, err := s.manager.Renew(req.GetForwardId(), req.GetTtlSeconds())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &groverPb.RenewForwardResponse{Forward: forward}, nil
}

func (s *RelayControlService) StreamForwardStats(req *groverPb.StreamForwardStatsRequest, stream groverPb.RelayControl_StreamForwardStatsServer) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			if req.GetForwardId() != "" {
				forward, err := s.manager.Get(req.GetForwardId())
				if err != nil {
					return status.Error(codes.NotFound, err.Error())
				}
				if err := stream.Send(forward); err != nil {
					return err
				}
				continue
			}
			forwards := s.manager.List(req.GetRouteId(), req.GetJobId())
			if len(forwards) == 0 {
				if req.GetRouteId() == "" && req.GetJobId() == "" {
					continue
				}
				continue
			}
			for _, forward := range forwards {
				if err := stream.Send(forward); err != nil {
					return err
				}
			}
		}
	}
}

type TransferJobControlService struct {
	groverPb.UnimplementedTransferJobControlServer

	manager *TransferJobManager
}

func NewTransferJobControlService(cfg *internal.ServerConfig) *TransferJobControlService {
	return &TransferJobControlService{manager: NewTransferJobManager(cfg, nil)}
}

func (s *TransferJobControlService) PrepareTransferEndpoint(ctx context.Context, req *groverPb.PrepareTransferEndpointRequest) (*groverPb.PrepareTransferEndpointResponse, error) {
	internal.Info("rpc TransferJobControl.PrepareTransferEndpoint received", internal.Fields{
		"route_id":          req.GetRouteId(),
		"session_id":        req.GetSessionId(),
		"job_id":            req.GetJobId(),
		"role":              req.GetRole().String(),
		"protocol":          req.GetProtocol().String(),
		"root":              req.GetRootPath(),
		"bind":              endpointLabel(req.GetBind()),
		"connection_origin": req.GetConnectionOrigin().String(),
	})
	endpoint, err := s.manager.PrepareEndpoint(ctx, req)
	if err != nil {
		internal.Warn("rpc TransferJobControl.PrepareTransferEndpoint rejected", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          req.GetRouteId(),
			"session_id":        req.GetSessionId(),
			"job_id":            req.GetJobId(),
			"role":              req.GetRole().String(),
		})
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	internal.Info("rpc TransferJobControl.PrepareTransferEndpoint completed", internal.Fields{
		"route_id":    endpoint.GetRouteId(),
		"session_id":  endpoint.GetSessionId(),
		"job_id":      endpoint.GetJobId(),
		"endpoint_id": endpoint.GetEndpointId(),
		"role":        endpoint.GetRole().String(),
		"data":        endpointLabel(endpoint.GetDataEndpoint()),
		"root":        endpoint.GetRootPath(),
	})
	return &groverPb.PrepareTransferEndpointResponse{Endpoint: endpoint}, nil
}

func (s *TransferJobControlService) StartTransferJob(ctx context.Context, req *groverPb.StartTransferJobRequest) (*groverPb.StartTransferJobResponse, error) {
	internal.Info("rpc TransferJobControl.StartTransferJob received", internal.Fields{
		"route_id":          req.GetRouteId(),
		"session_id":        req.GetSessionId(),
		"job_id":            req.GetJobId(),
		"source_endpoint":   req.GetSource().GetEndpointId(),
		"destination":       endpointLabel(req.GetDestination().GetDataEndpoint()),
		"files_in_flight":   req.GetFilesInFlight(),
		"streams_per_file":  req.GetStreamsPerFile(),
		"paths":             len(req.GetPaths()),
		"connection_origin": req.GetConnectionOrigin().String(),
	})
	job, err := s.manager.StartJob(ctx, req)
	if err != nil {
		internal.Warn("rpc TransferJobControl.StartTransferJob rejected", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          req.GetRouteId(),
			"session_id":        req.GetSessionId(),
			"job_id":            req.GetJobId(),
		})
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	internal.Info("rpc TransferJobControl.StartTransferJob completed", internal.Fields{
		"route_id":    job.GetRouteId(),
		"session_id":  job.GetSessionId(),
		"job_id":      job.GetJobId(),
		"state":       job.GetState().String(),
		"files_total": len(job.GetFiles()),
	})
	return &groverPb.StartTransferJobResponse{Job: job}, nil
}

func (s *TransferJobControlService) GetTransferJob(ctx context.Context, req *groverPb.GetTransferJobRequest) (*groverPb.GetTransferJobResponse, error) {
	job, err := s.manager.GetJob(req.GetJobId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &groverPb.GetTransferJobResponse{Job: job}, nil
}

func (s *TransferJobControlService) ListTransferJobs(ctx context.Context, req *groverPb.ListTransferJobsRequest) (*groverPb.ListTransferJobsResponse, error) {
	return &groverPb.ListTransferJobsResponse{Jobs: s.manager.ListJobs(req.GetRouteId())}, nil
}

func (s *TransferJobControlService) AbortTransferJob(ctx context.Context, req *groverPb.AbortTransferJobRequest) (*groverPb.AbortTransferJobResponse, error) {
	job, err := s.manager.AbortJob(req.GetJobId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &groverPb.AbortTransferJobResponse{Job: job}, nil
}

func (s *TransferJobControlService) UpdateTransferConcurrency(ctx context.Context, req *groverPb.UpdateTransferConcurrencyRequest) (*groverPb.UpdateTransferConcurrencyResponse, error) {
	job, err := s.manager.UpdateConcurrency(req.GetJobId(), req.GetFilesInFlight(), req.GetStreamsPerFile())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &groverPb.UpdateTransferConcurrencyResponse{Job: job}, nil
}

func (s *TransferJobControlService) StreamTransferStats(req *groverPb.StreamTransferStatsRequest, stream groverPb.TransferJobControl_StreamTransferStatsServer) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			if req.GetJobId() != "" {
				job, err := s.manager.GetJob(req.GetJobId())
				if err != nil {
					return status.Error(codes.NotFound, err.Error())
				}
				if err := stream.Send(job); err != nil {
					return err
				}
				continue
			}
			jobs := s.manager.ListJobs(req.GetRouteId())
			for _, job := range jobs {
				if err := stream.Send(job); err != nil {
					return err
				}
			}
		}
	}
}
