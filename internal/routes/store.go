// Package routes persists named route configs as a small JSON file:
//
//	{"routes": [{"name": "uc-tacc", "source": "uc:22444", "destination": "tacc:22444",
//	             "via": [], "protocol": "tcp", "connection_origin": "source", ...}]}
//
// Routes are network metadata only: never file paths or job state.
package routes

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/jgoldverg/grover/internal/pb"
)

var (
	ErrNotFound = errors.New("route not found")
	ErrInvalid  = errors.New("invalid route")
)

type Store struct {
	mu     sync.Mutex
	path   string
	routes map[string]*pb.Route
}

type fileJSON struct {
	Routes []routeJSON `json:"routes"`
}

type routeJSON struct {
	Name        string    `json:"name"`
	Source      string    `json:"source"`
	Destination string    `json:"destination"`
	Via         []string  `json:"via,omitempty"`
	Protocol    string    `json:"protocol"`
	ConnectFrom string    `json:"connection_origin"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Open loads the store; a missing file is an empty store.
func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("route store path is required")
	}
	s := &Store{path: path, routes: map[string]*pb.Route{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) || (err == nil && len(strings.TrimSpace(string(data))) == 0) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	var f fileJSON
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse route store %s: %w", path, err)
	}
	for _, rj := range f.Routes {
		r, err := fromJSON(rj)
		if err == nil {
			r, err = Normalize(r)
		}
		if err != nil {
			return nil, fmt.Errorf("route store %s: route %q: %w", path, rj.Name, err)
		}
		s.routes[r.Name] = r
	}
	return s, nil
}

// Normalize validates a route and fills defaults.
func Normalize(in *pb.Route) (*pb.Route, error) {
	r := proto.Clone(in).(*pb.Route)
	r.Name = strings.TrimSpace(r.Name)
	r.Source = strings.TrimSpace(r.Source)
	r.Destination = strings.TrimSpace(r.Destination)
	switch {
	case r.Name == "":
		return nil, fmt.Errorf("%w: name is required", ErrInvalid)
	case strings.ContainsAny(r.Name, " \t\r\n/\\"):
		return nil, fmt.Errorf("%w: name %q must not contain whitespace or slashes", ErrInvalid, r.Name)
	case r.Source == "" || r.Destination == "":
		return nil, fmt.Errorf("%w: source and destination are required", ErrInvalid)
	}
	if r.Protocol == pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED {
		r.Protocol = pb.DataProtocol_DATA_PROTOCOL_TCP
	}
	if r.ConnectFrom == pb.ConnectionOrigin_CONNECTION_ORIGIN_UNSPECIFIED {
		r.ConnectFrom = pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE
	}
	if r.ConnectFrom == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION && r.Protocol != pb.DataProtocol_DATA_PROTOCOL_TCP {
		return nil, fmt.Errorf("%w: connect_from=destination is only supported for tcp", ErrInvalid)
	}
	var via []string
	for _, hop := range r.Via {
		if hop = strings.TrimSpace(hop); hop == "" {
			continue
		}
		if strings.Contains(hop, "/") {
			return nil, fmt.Errorf("%w: relay %q must be host:port", ErrInvalid, hop)
		}
		via = append(via, hop)
	}
	r.Via = via
	return r, nil
}

func (s *Store) Put(in *pb.Route) (*pb.Route, error) {
	r, err := Normalize(in)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixNano()
	r.CreatedAtUnixNano, r.UpdatedAtUnixNano = now, now
	if old := s.routes[r.Name]; old != nil {
		r.CreatedAtUnixNano = old.CreatedAtUnixNano
	}
	prev := s.routes[r.Name]
	s.routes[r.Name] = r
	if err := s.saveLocked(); err != nil {
		if prev == nil {
			delete(s.routes, r.Name)
		} else {
			s.routes[r.Name] = prev
		}
		return nil, err
	}
	return proto.Clone(r).(*pb.Route), nil
}

func (s *Store) Get(name string) (*pb.Route, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.routes[strings.TrimSpace(name)]
	if r == nil {
		return nil, fmt.Errorf("%q: %w", name, ErrNotFound)
	}
	return proto.Clone(r).(*pb.Route), nil
}

func (s *Store) List() []*pb.Route {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*pb.Route, 0, len(s.routes))
	for _, r := range s.routes {
		out = append(out, proto.Clone(r).(*pb.Route))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *Store) Delete(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.routes[name]
	if r == nil {
		return fmt.Errorf("%q: %w", name, ErrNotFound)
	}
	delete(s.routes, name)
	if err := s.saveLocked(); err != nil {
		s.routes[name] = r
		return err
	}
	return nil
}

func (s *Store) saveLocked() error {
	var f fileJSON
	names := make([]string, 0, len(s.routes))
	for name := range s.routes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f.Routes = append(f.Routes, toJSON(s.routes[name]))
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func toJSON(r *pb.Route) routeJSON {
	return routeJSON{
		Name: r.Name, Source: r.Source, Destination: r.Destination, Via: r.Via,
		Protocol:    ProtocolName(r.Protocol),
		ConnectFrom: OriginName(r.ConnectFrom),
		CreatedAt:   time.Unix(0, r.CreatedAtUnixNano).UTC(),
		UpdatedAt:   time.Unix(0, r.UpdatedAtUnixNano).UTC(),
	}
}

func fromJSON(j routeJSON) (*pb.Route, error) {
	p, err := ParseProtocol(j.Protocol)
	if err != nil {
		return nil, err
	}
	o, err := ParseOrigin(j.ConnectFrom)
	if err != nil {
		return nil, err
	}
	return &pb.Route{
		Name: j.Name, Source: j.Source, Destination: j.Destination, Via: j.Via, Protocol: p, ConnectFrom: o,
		CreatedAtUnixNano: j.CreatedAt.UnixNano(), UpdatedAtUnixNano: j.UpdatedAt.UnixNano(),
	}, nil
}

// ProtocolName is the lowercase name used in JSON, flags and output.
func ProtocolName(p pb.DataProtocol) string {
	if p == pb.DataProtocol_DATA_PROTOCOL_UDP {
		return "udp"
	}
	return "tcp"
}

func ParseProtocol(s string) (pb.DataProtocol, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "tcp":
		return pb.DataProtocol_DATA_PROTOCOL_TCP, nil
	case "udp":
		return pb.DataProtocol_DATA_PROTOCOL_UDP, nil
	}
	return 0, fmt.Errorf("protocol must be tcp or udp, got %q", s)
}

// OriginName is "source" or "destination".
func OriginName(o pb.ConnectionOrigin) string {
	if o == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION {
		return "destination"
	}
	return "source"
}

func ParseOrigin(s string) (pb.ConnectionOrigin, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "source", "src":
		return pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE, nil
	case "destination", "dest", "dst":
		return pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION, nil
	}
	return 0, fmt.Errorf("connect-from must be source or destination, got %q", s)
}
