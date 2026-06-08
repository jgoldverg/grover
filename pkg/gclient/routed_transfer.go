package gclient

import (
	"context"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc"
)

type RoutedTransferService struct {
	tc pb.TransferJobControlClient
}

func NewRoutedTransferService(conn *grpc.ClientConn) *RoutedTransferService {
	return &RoutedTransferService{tc: pb.NewTransferJobControlClient(conn)}
}

func (s *RoutedTransferService) PrepareTransferEndpoint(ctx context.Context, req *pb.PrepareTransferEndpointRequest) (*pb.TransferEndpoint, error) {
	started := time.Now()
	internal.Debug("grpc TransferJobControl.PrepareTransferEndpoint start", internal.Fields{
		"route_id":          req.GetRouteId(),
		"session_id":        req.GetSessionId(),
		"job_id":            req.GetJobId(),
		"role":              req.GetRole().String(),
		"protocol":          req.GetProtocol().String(),
		"root":              req.GetRootPath(),
		"bind":              dataEndpointLabel(req.GetBind()),
		"connection_origin": req.GetConnectionOrigin().String(),
		"ttl_seconds":       req.GetTtlSeconds(),
	})
	resp, err := s.tc.PrepareTransferEndpoint(ctx, req)
	if err != nil {
		internal.Warn("grpc TransferJobControl.PrepareTransferEndpoint failed", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          req.GetRouteId(),
			"session_id":        req.GetSessionId(),
			"job_id":            req.GetJobId(),
			"role":              req.GetRole().String(),
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	endpoint := resp.GetEndpoint()
	internal.Debug("grpc TransferJobControl.PrepareTransferEndpoint done", internal.Fields{
		"route_id":     endpoint.GetRouteId(),
		"session_id":   endpoint.GetSessionId(),
		"job_id":       endpoint.GetJobId(),
		"endpoint_id":  endpoint.GetEndpointId(),
		"role":         endpoint.GetRole().String(),
		"data":         dataEndpointLabel(endpoint.GetDataEndpoint()),
		"root":         endpoint.GetRootPath(),
		"expires_unix": endpoint.GetExpiresAtUnix(),
		"elapsed_ms":   time.Since(started).Milliseconds(),
	})
	return endpoint, nil
}

func (s *RoutedTransferService) StartTransferJob(ctx context.Context, req *pb.StartTransferJobRequest) (*pb.TransferJob, error) {
	started := time.Now()
	internal.Debug("grpc TransferJobControl.StartTransferJob start", internal.Fields{
		"route_id":          req.GetRouteId(),
		"session_id":        req.GetSessionId(),
		"job_id":            req.GetJobId(),
		"source_endpoint":   req.GetSource().GetEndpointId(),
		"destination":       dataEndpointLabel(req.GetDestination().GetDataEndpoint()),
		"files_in_flight":   req.GetFilesInFlight(),
		"streams_per_file":  req.GetStreamsPerFile(),
		"paths":             len(req.GetPaths()),
		"connection_origin": req.GetConnectionOrigin().String(),
	})
	resp, err := s.tc.StartTransferJob(ctx, req)
	if err != nil {
		internal.Warn("grpc TransferJobControl.StartTransferJob failed", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          req.GetRouteId(),
			"session_id":        req.GetSessionId(),
			"job_id":            req.GetJobId(),
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	job := resp.GetJob()
	internal.Debug("grpc TransferJobControl.StartTransferJob done", internal.Fields{
		"route_id":    job.GetRouteId(),
		"session_id":  job.GetSessionId(),
		"job_id":      job.GetJobId(),
		"state":       job.GetState().String(),
		"files_total": len(job.GetFiles()),
		"total_bytes": transferJobPlannedBytes(job),
		"elapsed_ms":  time.Since(started).Milliseconds(),
	})
	return job, nil
}

func (s *RoutedTransferService) GetTransferJob(ctx context.Context, jobID string) (*pb.TransferJob, error) {
	started := time.Now()
	internal.Debug("grpc TransferJobControl.GetTransferJob start", internal.Fields{"job_id": jobID})
	resp, err := s.tc.GetTransferJob(ctx, &pb.GetTransferJobRequest{JobId: jobID})
	if err != nil {
		internal.Warn("grpc TransferJobControl.GetTransferJob failed", internal.Fields{
			internal.FieldError: err.Error(),
			"job_id":            jobID,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	job := resp.GetJob()
	internal.Debug("grpc TransferJobControl.GetTransferJob done", internal.Fields{
		"route_id":   job.GetRouteId(),
		"session_id": job.GetSessionId(),
		"job_id":     job.GetJobId(),
		"state":      job.GetState().String(),
		"good_bytes": job.GetGoodBytes(),
		"files_done": job.GetFilesDone(),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return job, nil
}

func (s *RoutedTransferService) ListTransferJobs(ctx context.Context, routeID string) ([]*pb.TransferJob, error) {
	started := time.Now()
	internal.Debug("grpc TransferJobControl.ListTransferJobs start", internal.Fields{"route_id": routeID})
	resp, err := s.tc.ListTransferJobs(ctx, &pb.ListTransferJobsRequest{RouteId: routeID})
	if err != nil {
		internal.Warn("grpc TransferJobControl.ListTransferJobs failed", internal.Fields{
			internal.FieldError: err.Error(),
			"route_id":          routeID,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	jobs := resp.GetJobs()
	internal.Debug("grpc TransferJobControl.ListTransferJobs done", internal.Fields{
		"route_id":   routeID,
		"jobs":       len(jobs),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return jobs, nil
}

func (s *RoutedTransferService) AbortTransferJob(ctx context.Context, jobID string) (*pb.TransferJob, error) {
	started := time.Now()
	internal.Debug("grpc TransferJobControl.AbortTransferJob start", internal.Fields{"job_id": jobID})
	resp, err := s.tc.AbortTransferJob(ctx, &pb.AbortTransferJobRequest{JobId: jobID})
	if err != nil {
		internal.Warn("grpc TransferJobControl.AbortTransferJob failed", internal.Fields{
			internal.FieldError: err.Error(),
			"job_id":            jobID,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	job := resp.GetJob()
	internal.Debug("grpc TransferJobControl.AbortTransferJob done", internal.Fields{
		"route_id":   job.GetRouteId(),
		"session_id": job.GetSessionId(),
		"job_id":     job.GetJobId(),
		"state":      job.GetState().String(),
		"elapsed_ms": time.Since(started).Milliseconds(),
	})
	return job, nil
}

func (s *RoutedTransferService) UpdateTransferConcurrency(ctx context.Context, jobID string, filesInFlight, streamsPerFile uint32) (*pb.TransferJob, error) {
	started := time.Now()
	internal.Debug("grpc TransferJobControl.UpdateTransferConcurrency start", internal.Fields{
		"job_id":           jobID,
		"files_in_flight":  filesInFlight,
		"streams_per_file": streamsPerFile,
	})
	resp, err := s.tc.UpdateTransferConcurrency(ctx, &pb.UpdateTransferConcurrencyRequest{
		JobId:          jobID,
		FilesInFlight:  filesInFlight,
		StreamsPerFile: streamsPerFile,
	})
	if err != nil {
		internal.Warn("grpc TransferJobControl.UpdateTransferConcurrency failed", internal.Fields{
			internal.FieldError: err.Error(),
			"job_id":            jobID,
			"elapsed_ms":        time.Since(started).Milliseconds(),
		})
		return nil, err
	}
	job := resp.GetJob()
	internal.Debug("grpc TransferJobControl.UpdateTransferConcurrency done", internal.Fields{
		"route_id":         job.GetRouteId(),
		"session_id":       job.GetSessionId(),
		"job_id":           job.GetJobId(),
		"files_in_flight":  job.GetFilesInFlight(),
		"streams_per_file": job.GetStreamsPerFile(),
		"elapsed_ms":       time.Since(started).Milliseconds(),
	})
	return job, nil
}

func (s *RoutedTransferService) StreamTransferStats(ctx context.Context, jobID, routeID string) (pb.TransferJobControl_StreamTransferStatsClient, error) {
	return s.tc.StreamTransferStats(ctx, &pb.StreamTransferStatsRequest{JobId: jobID, RouteId: routeID})
}

func transferJobPlannedBytes(job *pb.TransferJob) uint64 {
	var total uint64
	for _, file := range job.GetFiles() {
		total += file.GetSize()
	}
	return total
}
