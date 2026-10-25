package transfer

import pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"

func cloneEndpoint(ep *pb.DataEndpoint) *pb.DataEndpoint {
	if ep == nil {
		return nil
	}
	return &pb.DataEndpoint{Host: ep.GetHost(), Port: ep.GetPort()}
}
