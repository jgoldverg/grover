package gserver

import (
	"fmt"
	"net"
	"strings"

	"google.golang.org/protobuf/proto"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func cloneTransferEndpoint(ep *pb.TransferEndpoint) *pb.TransferEndpoint {
	if ep == nil {
		return nil
	}
	return proto.Clone(ep).(*pb.TransferEndpoint)
}

func endpointLabel(ep *pb.DataEndpoint) string {
	if ep == nil || strings.TrimSpace(ep.GetHost()) == "" || ep.GetPort() == 0 {
		return ""
	}
	return net.JoinHostPort(ep.GetHost(), fmt.Sprintf("%d", ep.GetPort()))
}
