package gclient

import (
	"context"

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
	resp, err := s.tc.PrepareTransferEndpoint(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetEndpoint(), nil
}

func (s *RoutedTransferService) StartTransferJob(ctx context.Context, req *pb.StartTransferJobRequest) (*pb.TransferJob, error) {
	resp, err := s.tc.StartTransferJob(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetJob(), nil
}

func (s *RoutedTransferService) GetTransferJob(ctx context.Context, jobID string) (*pb.TransferJob, error) {
	resp, err := s.tc.GetTransferJob(ctx, &pb.GetTransferJobRequest{JobId: jobID})
	if err != nil {
		return nil, err
	}
	return resp.GetJob(), nil
}

func (s *RoutedTransferService) ListTransferJobs(ctx context.Context, routeID string) ([]*pb.TransferJob, error) {
	resp, err := s.tc.ListTransferJobs(ctx, &pb.ListTransferJobsRequest{RouteId: routeID})
	if err != nil {
		return nil, err
	}
	return resp.GetJobs(), nil
}

func (s *RoutedTransferService) AbortTransferJob(ctx context.Context, jobID string) (*pb.TransferJob, error) {
	resp, err := s.tc.AbortTransferJob(ctx, &pb.AbortTransferJobRequest{JobId: jobID})
	if err != nil {
		return nil, err
	}
	return resp.GetJob(), nil
}

func (s *RoutedTransferService) UpdateTransferConcurrency(ctx context.Context, jobID string, filesInFlight, streamsPerFile uint32) (*pb.TransferJob, error) {
	resp, err := s.tc.UpdateTransferConcurrency(ctx, &pb.UpdateTransferConcurrencyRequest{
		JobId:          jobID,
		FilesInFlight:  filesInFlight,
		StreamsPerFile: streamsPerFile,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetJob(), nil
}

func (s *RoutedTransferService) StreamTransferStats(ctx context.Context, jobID, routeID string) (pb.TransferJobControl_StreamTransferStatsClient, error) {
	return s.tc.StreamTransferStats(ctx, &pb.StreamTransferStatsRequest{JobId: jobID, RouteId: routeID})
}
