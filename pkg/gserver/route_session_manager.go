package gserver

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

type ConnectionOrigin string

const (
	ConnectionOriginSource      ConnectionOrigin = "source"
	ConnectionOriginDestination ConnectionOrigin = "destination"
)

type DataDirection string

const (
	DataDirectionSourceToDestination DataDirection = "source_to_destination"
	DataDirectionDestinationToSource DataDirection = "destination_to_source"
)

type RouteSessionHop struct {
	HopIndex uint32
	NodeID   string
	Control  string
	Ingress  *pb.DataEndpoint
	Egress   *pb.DataEndpoint
	State    pb.RuntimeState
	Stats    *pb.StatsSnapshot
	Error    string
}

type RouteSession struct {
	SessionID        string
	RouteID          string
	JobID            string
	Protocol         pb.DataProtocol
	ConnectionOrigin ConnectionOrigin
	DataDirection    DataDirection
	Source           *pb.TransferEndpoint
	Destination      *pb.TransferEndpoint
	Hops             []RouteSessionHop
	ReverseSource    *pb.TransferEndpoint
	ReverseDest      *pb.TransferEndpoint
	ReverseHops      []RouteSessionHop
	State            pb.RuntimeState
	Stats            *pb.StatsSnapshot
	Error            string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type CreateRouteSessionRequest struct {
	SessionID        string
	RouteID          string
	JobID            string
	Protocol         pb.DataProtocol
	ConnectionOrigin ConnectionOrigin
	DataDirection    DataDirection
	Source           *pb.TransferEndpoint
	Destination      *pb.TransferEndpoint
	Hops             []RouteSessionHop
	ReverseSource    *pb.TransferEndpoint
	ReverseDest      *pb.TransferEndpoint
	ReverseHops      []RouteSessionHop
}

type RouteSessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*RouteSession
}

func NewRouteSessionManager() *RouteSessionManager {
	return &RouteSessionManager{sessions: make(map[string]*RouteSession)}
}

func (m *RouteSessionManager) Create(req CreateRouteSessionRequest) (*RouteSession, error) {
	if m == nil {
		return nil, errors.New("route session manager is required")
	}
	if err := validateRouteSessionRequest(req); err != nil {
		return nil, err
	}
	sessionID := strings.TrimSpace(req.SessionID)
	if sessionID == "" {
		sessionID = uuid.NewString()
	}
	now := time.Now().UTC()
	session := &RouteSession{
		SessionID:        sessionID,
		RouteID:          strings.TrimSpace(req.RouteID),
		JobID:            strings.TrimSpace(req.JobID),
		Protocol:         normalizeRouteSessionProtocol(req.Protocol),
		ConnectionOrigin: req.ConnectionOrigin,
		DataDirection:    req.DataDirection,
		Source:           cloneTransferEndpoint(req.Source),
		Destination:      cloneTransferEndpoint(req.Destination),
		Hops:             cloneRouteSessionHops(req.Hops),
		ReverseSource:    cloneTransferEndpoint(req.ReverseSource),
		ReverseDest:      cloneTransferEndpoint(req.ReverseDest),
		ReverseHops:      cloneRouteSessionHops(req.ReverseHops),
		State:            pb.RuntimeState_RUNTIME_STATE_READY,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.sessions[sessionID]; exists {
		return nil, fmt.Errorf("route session %q already exists", sessionID)
	}
	m.sessions[sessionID] = session
	return cloneRouteSession(session), nil
}

func (m *RouteSessionManager) Get(sessionID string) (*RouteSession, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.RLock()
	session := m.sessions[strings.TrimSpace(sessionID)]
	m.mu.RUnlock()
	if session == nil {
		return nil, false
	}
	return cloneRouteSession(session), true
}

func (m *RouteSessionManager) List(routeID, jobID string) []*RouteSession {
	if m == nil {
		return nil
	}
	routeID = strings.TrimSpace(routeID)
	jobID = strings.TrimSpace(jobID)
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*RouteSession, 0, len(m.sessions))
	for _, session := range m.sessions {
		if routeID != "" && session.RouteID != routeID {
			continue
		}
		if jobID != "" && session.JobID != jobID {
			continue
		}
		out = append(out, cloneRouteSession(session))
	}
	return out
}

func (m *RouteSessionManager) MarkRunning(sessionID string) (*RouteSession, error) {
	return m.updateState(sessionID, pb.RuntimeState_RUNTIME_STATE_RUNNING, "")
}

func (m *RouteSessionManager) MarkDone(sessionID string) (*RouteSession, error) {
	return m.updateState(sessionID, pb.RuntimeState_RUNTIME_STATE_DONE, "")
}

func (m *RouteSessionManager) MarkFailed(sessionID string, errText string) (*RouteSession, error) {
	return m.updateState(sessionID, pb.RuntimeState_RUNTIME_STATE_FAILED, strings.TrimSpace(errText))
}

func (m *RouteSessionManager) Abort(sessionID string) (*RouteSession, error) {
	return m.updateState(sessionID, pb.RuntimeState_RUNTIME_STATE_ABORTED, "")
}

func (m *RouteSessionManager) Delete(sessionID string) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	sessionID = strings.TrimSpace(sessionID)
	if _, ok := m.sessions[sessionID]; !ok {
		return false
	}
	delete(m.sessions, sessionID)
	return true
}

func (m *RouteSessionManager) updateState(sessionID string, state pb.RuntimeState, errText string) (*RouteSession, error) {
	if m == nil {
		return nil, errors.New("route session manager is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	session := m.sessions[strings.TrimSpace(sessionID)]
	if session == nil {
		return nil, fmt.Errorf("route session %q not found", strings.TrimSpace(sessionID))
	}
	session.State = state
	session.Error = errText
	session.UpdatedAt = time.Now().UTC()
	return cloneRouteSession(session), nil
}

func validateRouteSessionRequest(req CreateRouteSessionRequest) error {
	if strings.TrimSpace(req.RouteID) == "" {
		return errors.New("route_id is required")
	}
	protocol := normalizeRouteSessionProtocol(req.Protocol)
	if protocol != pb.DataProtocol_DATA_PROTOCOL_TCP && protocol != pb.DataProtocol_DATA_PROTOCOL_UDP {
		return fmt.Errorf("unsupported route session protocol %s", req.Protocol.String())
	}
	if req.ConnectionOrigin != ConnectionOriginSource && req.ConnectionOrigin != ConnectionOriginDestination {
		return fmt.Errorf("unsupported connection origin %q", req.ConnectionOrigin)
	}
	if req.DataDirection != DataDirectionSourceToDestination && req.DataDirection != DataDirectionDestinationToSource {
		return fmt.Errorf("unsupported data direction %q", req.DataDirection)
	}
	if req.Source == nil {
		return errors.New("source endpoint is required")
	}
	if req.Destination == nil {
		return errors.New("destination endpoint is required")
	}
	return nil
}

func normalizeRouteSessionProtocol(protocol pb.DataProtocol) pb.DataProtocol {
	if protocol == pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED {
		return pb.DataProtocol_DATA_PROTOCOL_TCP
	}
	return protocol
}

func cloneRouteSession(session *RouteSession) *RouteSession {
	if session == nil {
		return nil
	}
	return &RouteSession{
		SessionID:        session.SessionID,
		RouteID:          session.RouteID,
		JobID:            session.JobID,
		Protocol:         session.Protocol,
		ConnectionOrigin: session.ConnectionOrigin,
		DataDirection:    session.DataDirection,
		Source:           cloneTransferEndpoint(session.Source),
		Destination:      cloneTransferEndpoint(session.Destination),
		Hops:             cloneRouteSessionHops(session.Hops),
		ReverseSource:    cloneTransferEndpoint(session.ReverseSource),
		ReverseDest:      cloneTransferEndpoint(session.ReverseDest),
		ReverseHops:      cloneRouteSessionHops(session.ReverseHops),
		State:            session.State,
		Stats:            cloneStatsSnapshot(session.Stats),
		Error:            session.Error,
		CreatedAt:        session.CreatedAt,
		UpdatedAt:        session.UpdatedAt,
	}
}

func cloneRouteSessionHops(hops []RouteSessionHop) []RouteSessionHop {
	if len(hops) == 0 {
		return nil
	}
	out := make([]RouteSessionHop, 0, len(hops))
	for _, hop := range hops {
		out = append(out, RouteSessionHop{
			HopIndex: hop.HopIndex,
			NodeID:   strings.TrimSpace(hop.NodeID),
			Control:  strings.TrimSpace(hop.Control),
			Ingress:  cloneEndpoint(hop.Ingress),
			Egress:   cloneEndpoint(hop.Egress),
			State:    hop.State,
			Stats:    cloneStatsSnapshot(hop.Stats),
			Error:    hop.Error,
		})
	}
	return out
}

func cloneStatsSnapshot(stats *pb.StatsSnapshot) *pb.StatsSnapshot {
	if stats == nil {
		return nil
	}
	return &pb.StatsSnapshot{
		IngressBytes:         stats.GetIngressBytes(),
		EgressBytes:          stats.GetEgressBytes(),
		Packets:              stats.GetPackets(),
		CurrentThroughputBps: stats.GetCurrentThroughputBps(),
		AverageThroughputBps: stats.GetAverageThroughputBps(),
		ActiveConnections:    stats.GetActiveConnections(),
		ActiveStreams:        stats.GetActiveStreams(),
		Errors:               stats.GetErrors(),
		Drops:                stats.GetDrops(),
		LatencyMs:            stats.GetLatencyMs(),
		SampledAtUnixNano:    stats.GetSampledAtUnixNano(),
	}
}
