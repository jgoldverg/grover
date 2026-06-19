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
	const rpcName = "RelayControl.CreateForward"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{
		"route_id":    req.GetRouteId(),
		"job_id":      req.GetJobId(),
		"hop_index":   req.GetHopIndex(),
		"protocol":    req.GetProtocol().String(),
		"egress":      endpointLabel(req.GetEgress()),
		"ttl_seconds": req.GetTtlSeconds(),
	})
	forward, err := s.manager.Create(ctx, req)
	if err != nil {
		internal.RPCRejected(rpcName, err, internal.Fields{
			"route_id":  req.GetRouteId(),
			"job_id":    req.GetJobId(),
			"hop_index": req.GetHopIndex(),
		}, time.Since(started))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{
		"route_id":   forward.GetRouteId(),
		"job_id":     forward.GetJobId(),
		"forward_id": forward.GetForwardId(),
		"hop_index":  forward.GetHopIndex(),
		"ingress":    endpointLabel(forward.GetIngress()),
		"egress":     endpointLabel(forward.GetEgress()),
		"state":      forward.GetState().String(),
	}, time.Since(started))
	return &groverPb.CreateForwardResponse{Forward: forward}, nil
}

func (s *RelayControlService) GetForward(ctx context.Context, req *groverPb.GetForwardRequest) (*groverPb.GetForwardResponse, error) {
	const rpcName = "RelayControl.GetForward"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{"forward_id": req.GetForwardId()})
	forward, err := s.manager.Get(req.GetForwardId())
	if err != nil {
		internal.RPCFailed(rpcName, err, internal.Fields{"forward_id": req.GetForwardId()}, time.Since(started))
		return nil, status.Error(codes.NotFound, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{
		"route_id":   forward.GetRouteId(),
		"job_id":     forward.GetJobId(),
		"forward_id": forward.GetForwardId(),
		"state":      forward.GetState().String(),
	}, time.Since(started))
	return &groverPb.GetForwardResponse{Forward: forward}, nil
}

func (s *RelayControlService) ListForwards(ctx context.Context, req *groverPb.ListForwardsRequest) (*groverPb.ListForwardsResponse, error) {
	const rpcName = "RelayControl.ListForwards"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{
		"route_id": req.GetRouteId(),
		"job_id":   req.GetJobId(),
	})
	forwards := s.manager.List(req.GetRouteId(), req.GetJobId())
	internal.RPCCompleted(rpcName, internal.Fields{
		"route_id": req.GetRouteId(),
		"job_id":   req.GetJobId(),
		"forwards": len(forwards),
	}, time.Since(started))
	return &groverPb.ListForwardsResponse{
		Forwards: forwards,
	}, nil
}

func (s *RelayControlService) DeleteForward(ctx context.Context, req *groverPb.DeleteForwardRequest) (*groverPb.DeleteForwardResponse, error) {
	const rpcName = "RelayControl.DeleteForward"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{"forward_id": req.GetForwardId()})
	ok := s.manager.Delete(req.GetForwardId())
	internal.RPCCompleted(rpcName, internal.Fields{"forward_id": req.GetForwardId(), "deleted": ok}, time.Since(started))
	return &groverPb.DeleteForwardResponse{Ok: ok}, nil
}

func (s *RelayControlService) RenewForward(ctx context.Context, req *groverPb.RenewForwardRequest) (*groverPb.RenewForwardResponse, error) {
	const rpcName = "RelayControl.RenewForward"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{
		"forward_id":  req.GetForwardId(),
		"ttl_seconds": req.GetTtlSeconds(),
	})
	forward, err := s.manager.Renew(req.GetForwardId(), req.GetTtlSeconds())
	if err != nil {
		internal.RPCFailed(rpcName, err, internal.Fields{"forward_id": req.GetForwardId()}, time.Since(started))
		return nil, status.Error(codes.NotFound, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{
		"forward_id": forward.GetForwardId(),
		"route_id":   forward.GetRouteId(),
		"job_id":     forward.GetJobId(),
		"state":      forward.GetState().String(),
	}, time.Since(started))
	return &groverPb.RenewForwardResponse{Forward: forward}, nil
}

func (s *RelayControlService) StreamForwardStats(req *groverPb.StreamForwardStatsRequest, stream groverPb.RelayControl_StreamForwardStatsServer) error {
	const rpcName = "RelayControl.StreamForwardStats"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{
		"forward_id": req.GetForwardId(),
		"route_id":   req.GetRouteId(),
		"job_id":     req.GetJobId(),
	})
	defer func() {
		internal.RPCCompleted(rpcName, internal.Fields{
			"forward_id": req.GetForwardId(),
			"route_id":   req.GetRouteId(),
			"job_id":     req.GetJobId(),
		}, time.Since(started))
	}()
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
	const rpcName = "TransferJobControl.PrepareTransferEndpoint"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{
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
		internal.RPCRejected(rpcName, err, internal.Fields{
			"route_id":   req.GetRouteId(),
			"session_id": req.GetSessionId(),
			"job_id":     req.GetJobId(),
			"role":       req.GetRole().String(),
		}, time.Since(started))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{
		"route_id":    endpoint.GetRouteId(),
		"session_id":  endpoint.GetSessionId(),
		"job_id":      endpoint.GetJobId(),
		"endpoint_id": endpoint.GetEndpointId(),
		"role":        endpoint.GetRole().String(),
		"data":        endpointLabel(endpoint.GetDataEndpoint()),
		"root":        endpoint.GetRootPath(),
	}, time.Since(started))
	return &groverPb.PrepareTransferEndpointResponse{Endpoint: endpoint}, nil
}

func (s *TransferJobControlService) StartTransferJob(ctx context.Context, req *groverPb.StartTransferJobRequest) (*groverPb.StartTransferJobResponse, error) {
	const rpcName = "TransferJobControl.StartTransferJob"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{
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
		internal.RPCRejected(rpcName, err, internal.Fields{
			"route_id":   req.GetRouteId(),
			"session_id": req.GetSessionId(),
			"job_id":     req.GetJobId(),
		}, time.Since(started))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{
		"route_id":    job.GetRouteId(),
		"session_id":  job.GetSessionId(),
		"job_id":      job.GetJobId(),
		"state":       job.GetState().String(),
		"files_total": len(job.GetFiles()),
	}, time.Since(started))
	return &groverPb.StartTransferJobResponse{Job: job}, nil
}

func (s *TransferJobControlService) GetTransferJob(ctx context.Context, req *groverPb.GetTransferJobRequest) (*groverPb.GetTransferJobResponse, error) {
	const rpcName = "TransferJobControl.GetTransferJob"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{"job_id": req.GetJobId()})
	job, err := s.manager.GetJob(req.GetJobId())
	if err != nil {
		internal.RPCFailed(rpcName, err, internal.Fields{"job_id": req.GetJobId()}, time.Since(started))
		return nil, status.Error(codes.NotFound, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{
		"route_id":    job.GetRouteId(),
		"session_id":  job.GetSessionId(),
		"job_id":      job.GetJobId(),
		"state":       job.GetState().String(),
		"good_bytes":  job.GetGoodBytes(),
		"files_done":  job.GetFilesDone(),
		"files_total": len(job.GetFiles()),
	}, time.Since(started))
	return &groverPb.GetTransferJobResponse{Job: job}, nil
}

func (s *TransferJobControlService) ListTransferJobs(ctx context.Context, req *groverPb.ListTransferJobsRequest) (*groverPb.ListTransferJobsResponse, error) {
	const rpcName = "TransferJobControl.ListTransferJobs"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{"route_id": req.GetRouteId()})
	jobs := s.manager.ListJobs(req.GetRouteId())
	internal.RPCCompleted(rpcName, internal.Fields{"route_id": req.GetRouteId(), "jobs": len(jobs)}, time.Since(started))
	return &groverPb.ListTransferJobsResponse{Jobs: jobs}, nil
}

func (s *TransferJobControlService) AbortTransferJob(ctx context.Context, req *groverPb.AbortTransferJobRequest) (*groverPb.AbortTransferJobResponse, error) {
	const rpcName = "TransferJobControl.AbortTransferJob"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{"job_id": req.GetJobId()})
	job, err := s.manager.AbortJob(req.GetJobId())
	if err != nil {
		internal.RPCFailed(rpcName, err, internal.Fields{"job_id": req.GetJobId()}, time.Since(started))
		return nil, status.Error(codes.NotFound, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{
		"route_id": job.GetRouteId(),
		"job_id":   job.GetJobId(),
		"state":    job.GetState().String(),
	}, time.Since(started))
	return &groverPb.AbortTransferJobResponse{Job: job}, nil
}

func (s *TransferJobControlService) UpdateTransferConcurrency(ctx context.Context, req *groverPb.UpdateTransferConcurrencyRequest) (*groverPb.UpdateTransferConcurrencyResponse, error) {
	const rpcName = "TransferJobControl.UpdateTransferConcurrency"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{
		"job_id":           req.GetJobId(),
		"files_in_flight":  req.GetFilesInFlight(),
		"streams_per_file": req.GetStreamsPerFile(),
	})
	job, err := s.manager.UpdateConcurrency(req.GetJobId(), req.GetFilesInFlight(), req.GetStreamsPerFile())
	if err != nil {
		internal.RPCFailed(rpcName, err, internal.Fields{"job_id": req.GetJobId()}, time.Since(started))
		return nil, status.Error(codes.NotFound, err.Error())
	}
	internal.RPCCompleted(rpcName, internal.Fields{
		"route_id":         job.GetRouteId(),
		"job_id":           job.GetJobId(),
		"state":            job.GetState().String(),
		"files_in_flight":  job.GetFilesInFlight(),
		"streams_per_file": job.GetStreamsPerFile(),
	}, time.Since(started))
	return &groverPb.UpdateTransferConcurrencyResponse{Job: job}, nil
}

func (s *TransferJobControlService) StreamTransferStats(req *groverPb.StreamTransferStatsRequest, stream groverPb.TransferJobControl_StreamTransferStatsServer) error {
	const rpcName = "TransferJobControl.StreamTransferStats"
	started := time.Now()
	internal.RPCReceived(rpcName, internal.Fields{
		"job_id":   req.GetJobId(),
		"route_id": req.GetRouteId(),
	})
	defer func() {
		internal.RPCCompleted(rpcName, internal.Fields{
			"job_id":   req.GetJobId(),
			"route_id": req.GetRouteId(),
		}, time.Since(started))
	}()
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
