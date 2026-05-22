package gserver

import (
	"context"
	"testing"

	"github.com/jgoldverg/grover/internal"
	groverPb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRoutedControlServicesRegister(t *testing.T) {
	cfg := &internal.ServerConfig{
		InsecureControl: true,
		CredentialsFile: t.TempDir() + "/credentials.toml",
	}
	server := NewGroverServer(context.Background(), cfg)

	services := server.grpcServer.GetServiceInfo()
	for _, name := range []string{
		groverPb.RelayControl_ServiceDesc.ServiceName,
		groverPb.TransferJobControl_ServiceDesc.ServiceName,
	} {
		if _, ok := services[name]; !ok {
			t.Fatalf("service %s was not registered; got %v", name, serviceNames(services))
		}
	}
}

func TestRelayControlCreateForwardValidation(t *testing.T) {
	relay, err := NewRelayControlService(&internal.ServerConfig{DataBindHost: "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.manager.Close()

	if _, err := relay.CreateForward(context.Background(), &groverPb.CreateForwardRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateForward error = %v, want invalid argument", err)
	}
}

func TestTransferJobControlValidation(t *testing.T) {
	jobs := NewTransferJobControlService(nil)
	if _, err := jobs.PrepareTransferEndpoint(context.Background(), &groverPb.PrepareTransferEndpointRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("PrepareTransferEndpoint error = %v, want invalid argument", err)
	}
}

func serviceNames(services map[string]grpc.ServiceInfo) []string {
	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	return names
}
