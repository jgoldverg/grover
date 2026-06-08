package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jgoldverg/grover/internal"
)

func TestProfileSetUseAndList(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "cli_config.toml")
	if err := os.WriteFile(configPath, []byte(""), 0o600); err != nil {
		t.Fatal(err)
	}

	set := NewRootCommand()
	var setOut bytes.Buffer
	set.SetOut(&setOut)
	set.SetErr(&setOut)
	set.SetArgs([]string{
		"--app-config", configPath,
		"profile", "set", "tacc",
		"--server-url", "129.114.108.86:22444",
		"--insecure-control",
	})
	if err := set.Execute(); err != nil {
		t.Fatal(err)
	}
	if got := setOut.String(); !strings.Contains(got, "saved profile tacc") || !strings.Contains(got, "129.114.108.86:22444") {
		t.Fatalf("profile set output missing profile:\n%s", got)
	}

	use := NewRootCommand()
	var useOut bytes.Buffer
	use.SetOut(&useOut)
	use.SetErr(&useOut)
	use.SetArgs([]string{"--app-config", configPath, "profile", "use", "tacc"})
	if err := use.Execute(); err != nil {
		t.Fatal(err)
	}

	list := NewRootCommand()
	var listOut bytes.Buffer
	list.SetOut(&listOut)
	list.SetErr(&listOut)
	list.SetArgs([]string{"--app-config", configPath, "profile", "list"})
	if err := list.Execute(); err != nil {
		t.Fatal(err)
	}
	got := listOut.String()
	for _, want := range []string{"PROFILE", "ACTIVE", "SERVER", "CONTROL", "tacc", "*", "insecure"} {
		if !strings.Contains(got, want) {
			t.Fatalf("profile list missing %q:\n%s", want, got)
		}
	}

	cfg, err := internal.LoadAppConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ActiveProfile != "tacc" {
		t.Fatalf("active profile = %q, want tacc", cfg.ActiveProfile)
	}
	if profile := cfg.Profiles["tacc"]; profile.ServerURL != "129.114.108.86:22444" || !profile.InsecureControl {
		t.Fatalf("profile not persisted correctly: %+v", profile)
	}
}

func TestProfileSetCanSwitchBackToSecureControl(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "cli_config.toml")
	if err := os.WriteFile(configPath, []byte(""), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{
		"--app-config", configPath,
		"profile", "set", "uc",
		"--server-url", "192.5.86.187:22444",
		"--insecure-control",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}

	cmd = NewRootCommand()
	cmd.SetArgs([]string{
		"--app-config", configPath,
		"profile", "set", "uc",
		"--secure-control",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}

	cfg, err := internal.LoadAppConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Profiles["uc"].InsecureControl {
		t.Fatalf("profile should be secure after --secure-control: %+v", cfg.Profiles["uc"])
	}
}
