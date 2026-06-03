package internal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppConfigUsesExecutionAndKeepsRouteFallback(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old.toml")
	if err := os.WriteFile(oldPath, []byte(`route = "server"`), 0o600); err != nil {
		t.Fatal(err)
	}
	oldCfg, err := LoadAppConfig(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	if oldCfg.Execution != "server" {
		t.Fatalf("old config execution = %q, want server", oldCfg.Execution)
	}

	newPath := filepath.Join(dir, "new.toml")
	if err := os.WriteFile(newPath, []byte(`
route = "client"
execution = "server"
`), 0o600); err != nil {
		t.Fatal(err)
	}
	newCfg, err := LoadAppConfig(newPath)
	if err != nil {
		t.Fatal(err)
	}
	if newCfg.Execution != "server" {
		t.Fatalf("new config execution = %q, want server", newCfg.Execution)
	}
}

func TestLoadAppConfigDoesNotWriteServerConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	if _, err := LoadAppConfig(""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(home, ".grover", "cli_config.toml")); err != nil {
		t.Fatalf("expected cli config to be written: %v", err)
	}
	if _, err := os.Stat(filepath.Join(home, ".grover", "server_config.toml")); !os.IsNotExist(err) {
		t.Fatalf("server config should not be written by LoadAppConfig, stat err=%v", err)
	}
}

func TestLoadServerConfigRejectsInvalidDataPortRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.toml")
	if err := os.WriteFile(path, []byte(`
data_port_min = 5000
data_port_max = 0
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadServerConfig(path); err == nil {
		t.Fatal("expected invalid data port range error")
	}
}

func TestLoadServerConfigNormalizesUDPFlowControl(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.toml")
	if err := os.WriteFile(path, []byte(`udp_flow_control = "bbr"`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadServerConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.UDPFlowControl != "bbr" {
		t.Fatalf("udp flow control = %q, want bbr", cfg.UDPFlowControl)
	}

	path = filepath.Join(t.TempDir(), "server.toml")
	if err := os.WriteFile(path, []byte(`udp_flow_control = "unknown"`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadServerConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.UDPFlowControl != "fixed" {
		t.Fatalf("udp flow control = %q, want fixed", cfg.UDPFlowControl)
	}
}

func TestLoadServerConfigDefaultsJobLogDir(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.toml")
	if err := os.WriteFile(path, []byte(`port = 22444`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadServerConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.JobLogDir != "/var/log/grover" {
		t.Fatalf("job log dir = %q, want /var/log/grover", cfg.JobLogDir)
	}
	if got, want := cfg.RouteStoreFile, filepath.Join(os.Getenv("HOME"), ".grover", "routes.json"); got != want {
		t.Fatalf("route store file = %q, want %q", got, want)
	}
	if cfg.EnergyMonitor {
		t.Fatal("energy monitor should be disabled unless requested")
	}
	if cfg.EnergySampleMs != 1000 {
		t.Fatalf("energy sample ms = %d, want 1000", cfg.EnergySampleMs)
	}

	override := filepath.Join(t.TempDir(), "jobs")
	path = filepath.Join(t.TempDir(), "server.toml")
	routeStore := filepath.Join(t.TempDir(), "routes.json")
	if err := os.WriteFile(path, []byte(`job_log_dir = "`+override+`"
route_store_file = "`+routeStore+`"`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadServerConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.JobLogDir != override {
		t.Fatalf("job log dir = %q, want %q", cfg.JobLogDir, override)
	}
	if cfg.RouteStoreFile != routeStore {
		t.Fatalf("route store file = %q, want %q", cfg.RouteStoreFile, routeStore)
	}
}
