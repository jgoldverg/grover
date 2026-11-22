// Package orchestrate is the control-plane logic that spans several nodes:
// materializing a route into a session (endpoints + relay forwards), tearing
// it down, and running/watching jobs. The CLI and the integration tests both
// use it; nothing here touches file bytes.
package orchestrate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jgoldverg/grover/internal/client"
	"github.com/jgoldverg/grover/internal/pb"
)

// Spec describes a path to materialize. Node fields are profile names or
// host:port control addresses.
type Spec struct {
	RouteID     string
	SessionID   string
	Source      string
	Destination string
	Via         []string // ordered source -> destination
	Protocol    pb.DataProtocol
	ConnectFrom pb.ConnectionOrigin
	Connections uint32 // connect_from=destination: connections the destination keeps open
}

// SpecFromRoute builds a Spec for a stored route.
func SpecFromRoute(r *pb.Route, sessionID string) Spec {
	return Spec{
		RouteID: r.GetName(), SessionID: sessionID, Source: r.GetSource(), Destination: r.GetDestination(),
		Via: r.GetVia(), Protocol: r.GetProtocol(), ConnectFrom: r.GetConnectFrom(),
	}
}

// NewSessionID returns a readable unique session id.
func NewSessionID(routeID string) string {
	if routeID == "" {
		routeID = "direct"
	}
	return fmt.Sprintf("%s-%s", routeID, time.Now().UTC().Format("20060102-150405.000"))
}

// Prepare creates every resource for spec and records the session on the
// source node. On failure, everything created so far is torn down again.
func Prepare(ctx context.Context, pool *client.Pool, spec Spec) (_ *pb.RouteSession, err error) {
	if spec.SessionID == "" {
		spec.SessionID = NewSessionID(spec.RouteID)
	}
	if spec.Protocol == pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED {
		spec.Protocol = pb.DataProtocol_DATA_PROTOCOL_TCP
	}
	if spec.ConnectFrom == pb.ConnectionOrigin_CONNECTION_ORIGIN_UNSPECIFIED {
		spec.ConnectFrom = pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE
	}
	src, err := pool.Get(ctx, spec.Source)
	if err != nil {
		return nil, err
	}
	dst, err := pool.Get(ctx, spec.Destination)
	if err != nil {
		return nil, err
	}
	sess := &pb.RouteSession{
		SessionId: spec.SessionID, RouteId: spec.RouteID, Protocol: spec.Protocol, ConnectFrom: spec.ConnectFrom,
		SourceControl: src.Node.Addr, DestinationControl: dst.Node.Addr,
	}
	defer func() {
		if err != nil {
			if cerr := release(context.WithoutCancel(ctx), pool, sess); cerr != nil {
				slog.Warn("cleanup after failed prepare was incomplete", "session_id", sess.SessionId, "error", cerr)
			}
		}
	}()
	slog.Info("preparing session", "session_id", sess.SessionId, "route_id", spec.RouteID, "source", src.Node.Addr,
		"destination", dst.Node.Addr, "relays", len(spec.Via), "protocol", spec.Protocol.String(), "connect_from", spec.ConnectFrom.String())

	base := &pb.PrepareEndpointRequest{SessionId: sess.SessionId, RouteId: spec.RouteID, Protocol: spec.Protocol, ConnectFrom: spec.ConnectFrom}
	if spec.ConnectFrom == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION {
		// The destination dials toward the source: source listener first, then
		// forwards from the source side outward, then the destination dialer.
		req := cloneReq(base)
		req.Role = pb.EndpointRole_ENDPOINT_ROLE_SOURCE
		if sess.Source, err = src.Transfers.PrepareEndpoint(ctx, req); err != nil {
			return nil, fmt.Errorf("prepare source endpoint on %s: %w", src.Node.Name, err)
		}
		next := sess.Source.GetData()
		for i := 0; i < len(spec.Via); i++ {
			hop, err := createForward(ctx, pool, spec, sess.SessionId, i, next)
			if err != nil {
				return nil, err
			}
			sess.Hops = append(sess.Hops, hop)
			next = hop.Ingress
		}
		req = cloneReq(base)
		req.Role, req.Dial, req.Token, req.Connections = pb.EndpointRole_ENDPOINT_ROLE_DESTINATION, next, sess.Source.GetToken(), spec.Connections
		if sess.Destination, err = dst.Transfers.PrepareEndpoint(ctx, req); err != nil {
			return nil, fmt.Errorf("prepare destination endpoint on %s: %w", dst.Node.Name, err)
		}
	} else {
		// The source dials toward the destination: destination listener
		// first, then forwards from the destination side back to the source.
		req := cloneReq(base)
		req.Role = pb.EndpointRole_ENDPOINT_ROLE_DESTINATION
		if sess.Destination, err = dst.Transfers.PrepareEndpoint(ctx, req); err != nil {
			return nil, fmt.Errorf("prepare destination endpoint on %s: %w", dst.Node.Name, err)
		}
		next := sess.Destination.GetData()
		hops := make([]*pb.Hop, len(spec.Via))
		for i := len(spec.Via) - 1; i >= 0; i-- {
			hop, err := createForward(ctx, pool, spec, sess.SessionId, i, next)
			if err != nil {
				return nil, err
			}
			hops[i] = hop
			sess.Hops = append(sess.Hops, hop) // registered immediately for cleanup
			next = hop.Ingress
		}
		sess.Hops = hops
		sess.Data = next
	}
	created, err := src.Sessions.CreateSession(ctx, sess)
	if err != nil {
		return nil, fmt.Errorf("record session on %s: %w", src.Node.Name, err)
	}
	slog.Info("session ready", "session_id", created.SessionId)
	return created, nil
}

func createForward(ctx context.Context, pool *client.Pool, spec Spec, sessionID string, i int, egress *pb.DataEndpoint) (*pb.Hop, error) {
	relay, err := pool.Get(ctx, spec.Via[i])
	if err != nil {
		return nil, err
	}
	fw, err := relay.Relays.CreateForward(ctx, &pb.CreateForwardRequest{
		RouteId: spec.RouteID, SessionId: sessionID, HopIndex: uint32(i + 1), Protocol: spec.Protocol, Egress: egress,
	})
	if err != nil {
		return nil, fmt.Errorf("create forward on relay %s: %w", relay.Node.Name, err)
	}
	return &pb.Hop{Index: uint32(i + 1), Control: relay.Node.Addr, ForwardId: fw.ForwardId, Ingress: fw.Ingress, Egress: fw.Egress}, nil
}

// Close tears down a session recorded on sourceNode. It refuses while a job
// is running on it. Missing pieces (e.g. after a node restart) are ignored.
func Close(ctx context.Context, pool *client.Pool, sourceNode, sessionID string) error {
	src, err := pool.Get(ctx, sourceNode)
	if err != nil {
		return err
	}
	sess, err := src.Sessions.GetSession(ctx, &pb.SessionRef{SessionId: sessionID})
	if err != nil {
		return err
	}
	if sess.GetActiveJobId() != "" {
		return fmt.Errorf("session %s is running job %s; abort it first (grover job abort %s)", sessionID, sess.GetActiveJobId(), sess.GetActiveJobId())
	}
	if _, err := src.Sessions.DeleteSession(ctx, &pb.SessionRef{SessionId: sessionID}); err != nil && status.Code(err) != codes.NotFound {
		return err
	}
	return release(ctx, pool, sess)
}

// release frees endpoints and forwards named by sess, best effort.
func release(ctx context.Context, pool *client.Pool, sess *pb.RouteSession) error {
	var errs []error
	ignore := func(err error) error {
		if err == nil || status.Code(err) == codes.NotFound {
			return nil
		}
		return err
	}
	for _, hop := range sess.GetHops() {
		if hop == nil || hop.ForwardId == "" {
			continue
		}
		relay, err := pool.Get(ctx, hop.Control)
		if err == nil {
			_, err = relay.Relays.DeleteForward(ctx, &pb.ForwardRef{ForwardId: hop.ForwardId})
		}
		if err = ignore(err); err != nil {
			errs = append(errs, fmt.Errorf("relay %s: %w", hop.Control, err))
		}
	}
	for _, ep := range []struct {
		node string
		ep   *pb.TransferEndpoint
	}{{sess.GetDestinationControl(), sess.GetDestination()}, {sess.GetSourceControl(), sess.GetSource()}} {
		if ep.ep.GetEndpointId() == "" {
			continue
		}
		c, err := pool.Get(ctx, ep.node)
		if err == nil {
			_, err = c.Transfers.CloseEndpoint(ctx, &pb.EndpointRef{EndpointId: ep.ep.GetEndpointId()})
		}
		if err = ignore(err); err != nil {
			errs = append(errs, fmt.Errorf("endpoint on %s: %w", ep.node, err))
		}
	}
	return errors.Join(errs...)
}

// Pick returns the named session, or the newest READY session of the route.
func Pick(ctx context.Context, src *client.Conn, routeID, sessionID string) (*pb.RouteSession, error) {
	if sessionID != "" {
		return src.Sessions.GetSession(ctx, &pb.SessionRef{SessionId: sessionID})
	}
	list, err := src.Sessions.ListSessions(ctx, &pb.ListSessionsRequest{RouteId: routeID})
	if err != nil {
		return nil, err
	}
	busy := 0
	for _, s := range list.GetSessions() { // newest first
		if s.GetState() == pb.RuntimeState_RUNTIME_STATE_READY {
			return s, nil
		}
		busy++
	}
	if busy > 0 {
		return nil, fmt.Errorf("all %d sessions of route %s are busy; wait, or run: grover route prepare %s", busy, routeID, routeID)
	}
	return nil, fmt.Errorf("route %s has no prepared session on %s; run: grover route prepare %s", routeID, src.Node.Name, routeID)
}

// Watch streams job updates to fn until the job reaches a terminal state and
// returns the final snapshot.
func Watch(ctx context.Context, c *client.Conn, jobID string, interval time.Duration, view pb.FileView, fn func(*pb.Job)) (*pb.Job, error) {
	stream, err := c.Transfers.WatchJob(ctx, &pb.WatchJobRequest{JobId: jobID, IntervalMs: uint32(interval.Milliseconds()), Files: view})
	if err != nil {
		return nil, err
	}
	var last *pb.Job
	for {
		j, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if last == nil {
				return nil, fmt.Errorf("watch %s ended without updates", jobID)
			}
			return last, nil
		}
		if err != nil {
			return last, err
		}
		last = j
		if fn != nil {
			fn(j)
		}
	}
}

// IsTerminal reports whether a job state is final.
func IsTerminal(s pb.RuntimeState) bool {
	return s == pb.RuntimeState_RUNTIME_STATE_DONE || s == pb.RuntimeState_RUNTIME_STATE_FAILED || s == pb.RuntimeState_RUNTIME_STATE_ABORTED
}

func cloneReq(r *pb.PrepareEndpointRequest) *pb.PrepareEndpointRequest {
	return &pb.PrepareEndpointRequest{SessionId: r.SessionId, RouteId: r.RouteId, Protocol: r.Protocol, ConnectFrom: r.ConnectFrom}
}
