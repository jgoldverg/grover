package gserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

type RouteConfig struct {
	Name             string
	Source           string
	Destination      string
	Via              []string
	Protocol         pb.DataProtocol
	ConnectionOrigin pb.ConnectionOrigin
	DataDirection    pb.DataDirection
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type RouteStore interface {
	Put(context.Context, RouteConfig) (RouteConfig, error)
	Get(context.Context, string) (RouteConfig, bool, error)
	List(context.Context) ([]RouteConfig, error)
	Delete(context.Context, string) (bool, error)
}

type JSONRouteStore struct {
	mu     sync.Mutex
	path   string
	routes map[string]RouteConfig
}

type routeStoreFile struct {
	Routes []RouteConfig `json:"routes"`
}

type routeConfigJSON struct {
	Name             string    `json:"name"`
	Source           string    `json:"source"`
	Destination      string    `json:"destination"`
	Via              []string  `json:"via,omitempty"`
	Protocol         string    `json:"protocol"`
	ConnectionOrigin string    `json:"connection_origin"`
	DataDirection    string    `json:"data_direction"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func (r RouteConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(routeConfigJSON{
		Name:             r.Name,
		Source:           r.Source,
		Destination:      r.Destination,
		Via:              append([]string(nil), r.Via...),
		Protocol:         routeProtocolString(r.Protocol),
		ConnectionOrigin: routeConnectionOriginString(r.ConnectionOrigin),
		DataDirection:    routeDataDirectionString(r.DataDirection),
		CreatedAt:        r.CreatedAt,
		UpdatedAt:        r.UpdatedAt,
	})
}

func (r *RouteConfig) UnmarshalJSON(data []byte) error {
	var in routeConfigJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	protocol, err := parseRouteProtocol(in.Protocol)
	if err != nil {
		return err
	}
	origin, err := parseRouteConnectionOrigin(in.ConnectionOrigin)
	if err != nil {
		return err
	}
	direction, err := parseRouteDataDirection(in.DataDirection)
	if err != nil {
		return err
	}
	*r = RouteConfig{
		Name:             in.Name,
		Source:           in.Source,
		Destination:      in.Destination,
		Via:              append([]string(nil), in.Via...),
		Protocol:         protocol,
		ConnectionOrigin: origin,
		DataDirection:    direction,
		CreatedAt:        in.CreatedAt,
		UpdatedAt:        in.UpdatedAt,
	}
	return nil
}

func NewJSONRouteStore(cfg *internal.ServerConfig) (*JSONRouteStore, error) {
	path := ""
	if cfg != nil {
		path = strings.TrimSpace(cfg.RouteStoreFile)
	}
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		path = filepath.Join(home, ".grover", "routes.json")
	}
	store := &JSONRouteStore{path: path, routes: make(map[string]RouteConfig)}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *JSONRouteStore) Put(ctx context.Context, route RouteConfig) (RouteConfig, error) {
	if s == nil {
		return RouteConfig{}, errors.New("route store is required")
	}
	if err := ctx.Err(); err != nil {
		return RouteConfig{}, err
	}
	normalized, err := normalizeRouteConfig(route)
	if err != nil {
		return RouteConfig{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.routes[normalized.Name]; ok && !existing.CreatedAt.IsZero() {
		normalized.CreatedAt = existing.CreatedAt
	}
	if normalized.CreatedAt.IsZero() {
		normalized.CreatedAt = time.Now().UTC()
	}
	normalized.UpdatedAt = time.Now().UTC()
	s.routes[normalized.Name] = normalized
	if err := s.saveLocked(); err != nil {
		return RouteConfig{}, err
	}
	return normalized, nil
}

func (s *JSONRouteStore) Get(ctx context.Context, name string) (RouteConfig, bool, error) {
	if s == nil {
		return RouteConfig{}, false, errors.New("route store is required")
	}
	if err := ctx.Err(); err != nil {
		return RouteConfig{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	route, ok := s.routes[strings.TrimSpace(name)]
	return route, ok, nil
}

func (s *JSONRouteStore) List(ctx context.Context) ([]RouteConfig, error) {
	if s == nil {
		return nil, errors.New("route store is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	routes := make([]RouteConfig, 0, len(s.routes))
	for _, route := range s.routes {
		routes = append(routes, route)
	}
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].Name < routes[j].Name
	})
	return routes, nil
}

func (s *JSONRouteStore) Delete(ctx context.Context, name string) (bool, error) {
	if s == nil {
		return false, errors.New("route store is required")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	name = strings.TrimSpace(name)
	if _, ok := s.routes[name]; !ok {
		return false, nil
	}
	delete(s.routes, name)
	if err := s.saveLocked(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *JSONRouteStore) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(s.path) == "" {
		return errors.New("route store path is required")
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil
	}
	var file routeStoreFile
	if err := json.Unmarshal(data, &file); err != nil {
		return fmt.Errorf("load route store %s: %w", s.path, err)
	}
	for _, route := range file.Routes {
		normalized, err := normalizeRouteConfig(route)
		if err != nil {
			return fmt.Errorf("load route %q: %w", route.Name, err)
		}
		s.routes[normalized.Name] = normalized
	}
	return nil
}

func (s *JSONRouteStore) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	routes := make([]RouteConfig, 0, len(s.routes))
	for _, route := range s.routes {
		routes = append(routes, route)
	}
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].Name < routes[j].Name
	})
	data, err := json.MarshalIndent(routeStoreFile{Routes: routes}, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := fmt.Sprintf("%s.tmp.%s", s.path, uuid.NewString())
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func normalizeRouteConfig(route RouteConfig) (RouteConfig, error) {
	route.Name = strings.TrimSpace(route.Name)
	if route.Name == "" {
		return RouteConfig{}, errors.New("route name is required")
	}
	if strings.ContainsAny(route.Name, " \t\r\n/\\") {
		return RouteConfig{}, fmt.Errorf("route name %q must not contain whitespace or path separators", route.Name)
	}
	route.Source = strings.TrimSpace(route.Source)
	if route.Source == "" {
		return RouteConfig{}, errors.New("route source is required")
	}
	route.Destination = strings.TrimSpace(route.Destination)
	if route.Destination == "" {
		return RouteConfig{}, errors.New("route destination is required")
	}
	if route.Protocol == pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED {
		route.Protocol = pb.DataProtocol_DATA_PROTOCOL_TCP
	}
	if route.Protocol != pb.DataProtocol_DATA_PROTOCOL_TCP && route.Protocol != pb.DataProtocol_DATA_PROTOCOL_UDP {
		return RouteConfig{}, fmt.Errorf("unsupported route protocol %s", route.Protocol.String())
	}
	if route.ConnectionOrigin == pb.ConnectionOrigin_CONNECTION_ORIGIN_UNSPECIFIED {
		route.ConnectionOrigin = pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE
	}
	if route.ConnectionOrigin != pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE && route.ConnectionOrigin != pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION {
		return RouteConfig{}, fmt.Errorf("unsupported connection origin %s", route.ConnectionOrigin.String())
	}
	if route.DataDirection == pb.DataDirection_DATA_DIRECTION_UNSPECIFIED {
		route.DataDirection = pb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION
	}
	if route.DataDirection != pb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION && route.DataDirection != pb.DataDirection_DATA_DIRECTION_DESTINATION_TO_SOURCE {
		return RouteConfig{}, fmt.Errorf("unsupported data direction %s", route.DataDirection.String())
	}
	via := make([]string, 0, len(route.Via))
	for _, hop := range route.Via {
		hop = strings.TrimSpace(hop)
		if hop == "" {
			continue
		}
		if strings.Contains(hop, "/") {
			return RouteConfig{}, fmt.Errorf("invalid route relay %q: relay hops must be names or host:port values, not paths", hop)
		}
		via = append(via, hop)
	}
	route.Via = via
	return route, nil
}

func routeProtocolString(protocol pb.DataProtocol) string {
	switch protocol {
	case pb.DataProtocol_DATA_PROTOCOL_UDP:
		return "udp"
	case pb.DataProtocol_DATA_PROTOCOL_TCP, pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED:
		return "tcp"
	default:
		return protocol.String()
	}
}

func parseRouteProtocol(protocol string) (pb.DataProtocol, error) {
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case "", "tcp", "data_protocol_tcp":
		return pb.DataProtocol_DATA_PROTOCOL_TCP, nil
	case "udp", "data_protocol_udp":
		return pb.DataProtocol_DATA_PROTOCOL_UDP, nil
	default:
		return pb.DataProtocol_DATA_PROTOCOL_UNSPECIFIED, fmt.Errorf("unsupported route protocol %q", protocol)
	}
}

func routeConnectionOriginString(origin pb.ConnectionOrigin) string {
	switch origin {
	case pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION:
		return "destination"
	case pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE, pb.ConnectionOrigin_CONNECTION_ORIGIN_UNSPECIFIED:
		return "source"
	default:
		return origin.String()
	}
}

func parseRouteConnectionOrigin(origin string) (pb.ConnectionOrigin, error) {
	switch strings.ToLower(strings.TrimSpace(origin)) {
	case "", "source", "connection_origin_source":
		return pb.ConnectionOrigin_CONNECTION_ORIGIN_SOURCE, nil
	case "destination", "dest", "connection_origin_destination":
		return pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION, nil
	default:
		return pb.ConnectionOrigin_CONNECTION_ORIGIN_UNSPECIFIED, fmt.Errorf("unsupported connection origin %q", origin)
	}
}

func routeDataDirectionString(direction pb.DataDirection) string {
	switch direction {
	case pb.DataDirection_DATA_DIRECTION_DESTINATION_TO_SOURCE:
		return "destination_to_source"
	case pb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION, pb.DataDirection_DATA_DIRECTION_UNSPECIFIED:
		return "source_to_destination"
	default:
		return direction.String()
	}
}

func parseRouteDataDirection(direction string) (pb.DataDirection, error) {
	normalized := strings.ToLower(strings.TrimSpace(direction))
	normalized = strings.ReplaceAll(normalized, "-", "_")
	switch normalized {
	case "", "source_to_destination", "source_destination", "src_to_dst", "data_direction_source_to_destination":
		return pb.DataDirection_DATA_DIRECTION_SOURCE_TO_DESTINATION, nil
	case "destination_to_source", "destination_source", "dst_to_src", "data_direction_destination_to_source":
		return pb.DataDirection_DATA_DIRECTION_DESTINATION_TO_SOURCE, nil
	default:
		return pb.DataDirection_DATA_DIRECTION_UNSPECIFIED, fmt.Errorf("unsupported data direction %q", direction)
	}
}

var _ RouteStore = (*JSONRouteStore)(nil)
