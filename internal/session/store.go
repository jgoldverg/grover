// Package session stores materialized route sessions on a route's source node.
//
// A session records the data path a job should use: the destination endpoint
// (address + token) or, for connect_from=destination, the source endpoint whose
// connection pool the destination fills. Sessions are created by the CLI after
// it prepares endpoints and relay forwards, and are used by the transfer
// Manager, which claims a session for the duration of one job.
package session

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/jgoldverg/grover/internal/pb"
)

var (
	ErrNotFound = errors.New("session not found")
	ErrBusy     = errors.New("session busy")
	ErrInvalid  = errors.New("invalid session")
)

type Store struct {
	mu       sync.Mutex
	sessions map[string]*pb.RouteSession
	active   map[string]string // session id -> job id
}

func NewStore() *Store {
	return &Store{sessions: map[string]*pb.RouteSession{}, active: map[string]string{}}
}

func (s *Store) Create(in *pb.RouteSession) (*pb.RouteSession, error) {
	sess := proto.Clone(in).(*pb.RouteSession)
	sess.SessionId = strings.TrimSpace(sess.GetSessionId())
	if sess.SessionId == "" {
		return nil, fmt.Errorf("%w: session_id is required", ErrInvalid)
	}
	switch sess.GetConnectFrom() {
	case pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION:
		if sess.GetSource().GetEndpointId() == "" {
			return nil, fmt.Errorf("%w: connect_from=destination needs the source endpoint", ErrInvalid)
		}
	default:
		sess.ConnectFrom = pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE
		if sess.GetData().GetPort() == 0 || len(sess.GetDestination().GetToken()) == 0 {
			return nil, fmt.Errorf("%w: connect_from=source needs a data endpoint and destination token", ErrInvalid)
		}
	}
	if sess.CreatedAtUnixNano == 0 {
		sess.CreatedAtUnixNano = time.Now().UnixNano()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sessions[sess.SessionId]; exists {
		return nil, fmt.Errorf("%w: session %q already exists", ErrInvalid, sess.SessionId)
	}
	s.sessions[sess.SessionId] = sess
	return s.viewLocked(sess), nil
}

func (s *Store) Get(id string) (*pb.RouteSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess := s.sessions[id]
	if sess == nil {
		return nil, fmt.Errorf("%q: %w", id, ErrNotFound)
	}
	return s.viewLocked(sess), nil
}

// List returns sessions for a route (all when empty), newest first.
func (s *Store) List(routeID string) []*pb.RouteSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*pb.RouteSession
	for _, sess := range s.sessions {
		if routeID == "" || sess.GetRouteId() == routeID {
			out = append(out, s.viewLocked(sess))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAtUnixNano > out[j].CreatedAtUnixNano })
	return out
}

// Delete removes a session record. It refuses while a job is using it.
func (s *Store) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[id]; !ok {
		return fmt.Errorf("%q: %w", id, ErrNotFound)
	}
	if job := s.active[id]; job != "" {
		return fmt.Errorf("%w: job %s is running on session %s; abort it first", ErrBusy, job, id)
	}
	delete(s.sessions, id)
	return nil
}

// Claim implements transfer.Sessions.
func (s *Store) Claim(id, jobID string) (*pb.RouteSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess := s.sessions[id]
	if sess == nil {
		return nil, fmt.Errorf("%q: %w (sessions live on the route's source node)", id, ErrNotFound)
	}
	if job := s.active[id]; job != "" {
		return nil, fmt.Errorf("%w: session %s is running job %s", ErrBusy, id, job)
	}
	s.active[id] = jobID
	return proto.Clone(sess).(*pb.RouteSession), nil
}

// Release implements transfer.Sessions.
func (s *Store) Release(id, jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active[id] == jobID {
		delete(s.active, id)
	}
}

func (s *Store) viewLocked(sess *pb.RouteSession) *pb.RouteSession {
	out := proto.Clone(sess).(*pb.RouteSession)
	out.State = pb.RuntimeState_RUNTIME_STATE_READY
	if job := s.active[sess.SessionId]; job != "" {
		out.State, out.ActiveJobId = pb.RuntimeState_RUNTIME_STATE_RUNNING, job
	}
	return out
}
