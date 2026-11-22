// Package config loads the groverd server config and the grover CLI config.
//
// Both are plain TOML. Loading never writes files; missing files mean defaults.
// Precedence is: built-in defaults < config file < command-line flags.
package config

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"

	"github.com/jgoldverg/grover/internal/tuning"
)

// GroverDir is ~/.grover, the default home for config and state.
func GroverDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".grover"
	}
	return filepath.Join(home, ".grover")
}

// ExpandPath expands a leading ~ and environment variables.
func ExpandPath(p string) string {
	p = strings.TrimSpace(os.ExpandEnv(p))
	if p == "~" || strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			p = filepath.Join(home, strings.TrimPrefix(p, "~"))
		}
	}
	return p
}

// ---------------------------------------------------------------- server

// Server is the groverd configuration.
type Server struct {
	Port            int    `toml:"port"`
	InsecureControl bool   `toml:"insecure_control"`
	CertFile        string `toml:"server_certificate_path"`
	KeyFile         string `toml:"server_key_path"`
	LogLevel        string `toml:"log_level"`
	LogFormat       string `toml:"log_format"`

	DataBindHost      string `toml:"data_bind_host"`
	DataAdvertiseHost string `toml:"data_advertise_host"`
	DataPortMin       int    `toml:"data_port_min"`
	DataPortMax       int    `toml:"data_port_max"`

	RouteStoreFile string `toml:"route_store_file"`
	JobLogDir      string `toml:"job_log_dir"` // empty disables job logs
	EnergyMonitor  bool   `toml:"energy_monitor"`
	EnergySampleMs int    `toml:"energy_sample_ms"`

	// Tuning is the server-wide default for jobs started on this node.
	Tuning tuning.Tuning `toml:"tuning"`
}

// DefaultServer returns the built-in groverd defaults.
func DefaultServer() Server {
	dir := GroverDir()
	return Server{
		Port:              22444,
		CertFile:          filepath.Join(dir, "certs", "server.crt"),
		KeyFile:           filepath.Join(dir, "certs", "server.key"),
		LogLevel:          "info",
		LogFormat:         "text",
		DataBindHost:      "0.0.0.0",
		DataAdvertiseHost: "127.0.0.1",
		RouteStoreFile:    filepath.Join(dir, "routes.json"),
		JobLogDir:         filepath.Join(dir, "jobs"),
		EnergySampleMs:    1000,
		Tuning:            tuning.Defaults(),
	}
}

// DefaultServerConfigPath is ~/.grover/server_config.toml.
func DefaultServerConfigPath() string { return filepath.Join(GroverDir(), "server_config.toml") }

// LoadServer reads path (or the default path when empty) over the defaults.
// A missing file is not an error. Unknown keys are logged as warnings so stale
// configs are visible instead of silently ignored.
func LoadServer(path string) (Server, error) {
	cfg := DefaultServer()
	explicit := strings.TrimSpace(path) != ""
	if !explicit {
		path = DefaultServerConfigPath()
	}
	path = ExpandPath(path)
	md, err := toml.DecodeFile(path, &cfg)
	switch {
	case errors.Is(err, fs.ErrNotExist) && !explicit:
		// defaults only
	case err != nil:
		return cfg, fmt.Errorf("read server config %s: %w", path, err)
	default:
		logUndecoded(slog.LevelWarn, path, md)
	}
	cfg.Tuning = tuning.Defaults().Merge(cfg.Tuning)
	cfg.normalize()
	return cfg, cfg.Validate()
}

func (c *Server) normalize() {
	c.CertFile = ExpandPath(c.CertFile)
	c.KeyFile = ExpandPath(c.KeyFile)
	c.RouteStoreFile = ExpandPath(c.RouteStoreFile)
	c.JobLogDir = ExpandPath(c.JobLogDir)
	if c.EnergySampleMs <= 0 {
		c.EnergySampleMs = 1000
	}
}

// Validate checks values that would otherwise fail deep inside the server.
func (c Server) Validate() error {
	var errs []error
	if c.Port <= 0 || c.Port > 65535 {
		errs = append(errs, fmt.Errorf("port must be 1-65535, got %d", c.Port))
	}
	if err := ValidatePortRange(c.DataPortMin, c.DataPortMax); err != nil {
		errs = append(errs, err)
	}
	if err := c.Tuning.Validate(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// ValidatePortRange accepts 0-0 (OS ephemeral ports) or a valid inclusive range.
func ValidatePortRange(minPort, maxPort int) error {
	if minPort == 0 && maxPort == 0 {
		return nil
	}
	if minPort <= 0 || maxPort <= 0 || minPort > maxPort || maxPort > 65535 {
		return fmt.Errorf("data port range must be 0-0 or 1 <= min <= max <= 65535, got %d-%d", minPort, maxPort)
	}
	return nil
}

func logUndecoded(level slog.Level, path string, md toml.MetaData) {
	keys := md.Undecoded()
	if len(keys) == 0 {
		return
	}
	names := make([]string, 0, len(keys))
	for _, k := range keys {
		names = append(names, k.String())
	}
	slog.Log(context.Background(), level, "ignoring unknown config keys", "path", path, "keys", strings.Join(names, ","))
}

// ---------------------------------------------------------------- client

// Profile is a named groverd control endpoint. Profile names double as node
// aliases in transfer locations, e.g. "uc:/data/file".
type Profile struct {
	ServerURL       string `toml:"server_url"`
	CACertFile      string `toml:"ca_cert_file,omitempty"`
	InsecureControl bool   `toml:"insecure_control,omitempty"`
}

// Client is the grover CLI configuration (~/.grover/cli_config.toml).
type Client struct {
	ActiveProfile string             `toml:"active_profile,omitempty"`
	LogLevel      string             `toml:"log_level,omitempty"`
	Profiles      map[string]Profile `toml:"profiles,omitempty"`

	// Default endpoint used when no profile is active.
	ServerURL       string `toml:"server_url,omitempty"`
	CACertFile      string `toml:"ca_cert_file,omitempty"`
	InsecureControl bool   `toml:"insecure_control,omitempty"`
}

// DefaultClientConfigPath is ~/.grover/cli_config.toml.
func DefaultClientConfigPath() string { return filepath.Join(GroverDir(), "cli_config.toml") }

// LoadClient reads the CLI config; a missing file yields defaults.
func LoadClient(path string) (Client, error) {
	cfg := Client{LogLevel: "warn", ServerURL: "localhost:22444", Profiles: map[string]Profile{}}
	if strings.TrimSpace(path) == "" {
		path = DefaultClientConfigPath()
	}
	path = ExpandPath(path)
	md, err := toml.DecodeFile(path, &cfg)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return cfg, fmt.Errorf("read cli config %s: %w", path, err)
	}
	if err == nil {
		logUndecoded(slog.LevelDebug, path, md) // stale keys are harmless for the CLI
	}
	if cfg.Profiles == nil {
		cfg.Profiles = map[string]Profile{}
	}
	return cfg, nil
}

// SaveClient writes the CLI config with 0600 permissions.
func SaveClient(path string, cfg Client) error {
	if strings.TrimSpace(path) == "" {
		path = DefaultClientConfigPath()
	}
	path = ExpandPath(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := toml.NewEncoder(f).Encode(cfg); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// ProfileNames returns profile names in sorted order.
func (c Client) ProfileNames() []string {
	names := make([]string, 0, len(c.Profiles))
	for name := range c.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Node is everything needed to dial one groverd control endpoint.
type Node struct {
	Name     string // profile name, or the address when no profile matched
	Addr     string
	CACert   string
	Insecure bool
}

// Default returns the node commands talk to when none is named: the active
// profile, else the top-level server_url.
func (c Client) Default() Node {
	if p, ok := c.Profiles[c.ActiveProfile]; ok && c.ActiveProfile != "" {
		return Node{Name: c.ActiveProfile, Addr: p.ServerURL, CACert: ExpandPath(p.CACertFile), Insecure: p.InsecureControl}
	}
	return Node{Name: c.ServerURL, Addr: c.ServerURL, CACert: ExpandPath(c.CACertFile), Insecure: c.InsecureControl}
}

// Resolve turns a profile name or host:port into a Node. Addresses that match
// a profile's server_url pick up that profile's TLS settings; other addresses
// inherit the default node's TLS settings.
func (c Client) Resolve(nameOrAddr string) (Node, error) {
	key := strings.TrimSpace(nameOrAddr)
	if key == "" {
		return c.Default(), nil
	}
	if p, ok := c.Profiles[key]; ok {
		if strings.TrimSpace(p.ServerURL) == "" {
			return Node{}, fmt.Errorf("profile %q has no server_url", key)
		}
		return Node{Name: key, Addr: p.ServerURL, CACert: ExpandPath(p.CACertFile), Insecure: p.InsecureControl}, nil
	}
	if !strings.Contains(key, ":") {
		return Node{}, fmt.Errorf("unknown node %q: not a profile name or host:port", key)
	}
	for _, name := range c.ProfileNames() {
		p := c.Profiles[name]
		if p.ServerURL == key {
			return Node{Name: name, Addr: key, CACert: ExpandPath(p.CACertFile), Insecure: p.InsecureControl}, nil
		}
	}
	def := c.Default()
	return Node{Name: key, Addr: key, CACert: def.CACert, Insecure: def.Insecure}, nil
}
