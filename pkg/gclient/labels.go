package gclient

import (
	"fmt"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func dataEndpointLabel(endpoint *pb.DataEndpoint) string {
	if endpoint == nil || endpoint.GetHost() == "" || endpoint.GetPort() == 0 {
		return "(none)"
	}
	return fmt.Sprintf("%s:%d", endpoint.GetHost(), endpoint.GetPort())
}
