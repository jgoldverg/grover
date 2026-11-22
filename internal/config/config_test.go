package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadServerDefaultsWhenMissing(t *testing.T) {
	cfg, err := LoadServer(filepath.Join(t.TempDir(), "nope.toml"))
	if err == nil {
		t.Fatal("explicit missing config path should fail")
	}
	_ = cfg
}

func TestLoadServerFileOverridesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.toml")
	body := `
port = 23000
data_port_min = 30000
data_port_max = 30010
[tuning]
parallelism = 4
udp_flow_control = "bbr"
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadServer(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 23000 || cfg.DataPortMin != 30000 || cfg.Tuning.Parallelism != 4 || cfg.Tuning.UDPFlowControl != "bbr" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.Tuning.Concurrency == 0 || cfg.Tuning.UDPMTU == 0 {
		t.Fatalf("defaults lost under [tuning]: %+v", cfg.Tuning)
	}
}

func TestLoadServerRejectsBadPortRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.toml")
	_ = os.WriteFile(path, []byte("data_port_min = 10\n"), 0o600)
	if _, err := LoadServer(path); err == nil {
		t.Fatal("expected error for half-set port range")
	}
}

func TestClientResolve(t *testing.T) {
	c := Client{
		ServerURL: "localhost:22444",
		Profiles: map[string]Profile{
			"uc":   {ServerURL: "uc.example:22444", CACertFile: "/certs/uc.crt"},
			"edu":  {ServerURL: "edu.example:22444", InsecureControl: true},
			"tacc": {ServerURL: "tacc.example:22444"},
		},
		ActiveProfile: "edu",
	}
	n, err := c.Resolve("uc")
	if err != nil || n.Addr != "uc.example:22444" || n.CACert != "/certs/uc.crt" {
		t.Fatalf("resolve by name: %+v %v", n, err)
	}
	n, err = c.Resolve("uc.example:22444")
	if err != nil || n.Name != "uc" || n.CACert != "/certs/uc.crt" {
		t.Fatalf("resolve by matching address: %+v %v", n, err)
	}
	n, err = c.Resolve("10.0.0.5:22444")
	if err != nil || n.Addr != "10.0.0.5:22444" || !n.Insecure {
		t.Fatalf("unknown address should inherit default TLS settings: %+v %v", n, err)
	}
	if _, err := c.Resolve("nosuch"); err == nil {
		t.Fatal("expected error for unknown bare name")
	}
	if d := c.Default(); d.Name != "edu" {
		t.Fatalf("default should be active profile: %+v", d)
	}
}

func TestClientSaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cli.toml")
	in := Client{ActiveProfile: "uc", Profiles: map[string]Profile{"uc": {ServerURL: "uc:1"}}}
	if err := SaveClient(path, in); err != nil {
		t.Fatal(err)
	}
	out, err := LoadClient(path)
	if err != nil {
		t.Fatal(err)
	}
	if out.ActiveProfile != "uc" || out.Profiles["uc"].ServerURL != "uc:1" {
		t.Fatalf("round trip: %+v", out)
	}
}
