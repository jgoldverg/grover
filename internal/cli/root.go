// Package cli implements the grover command. Commands are thin: they parse
// flags, call internal/orchestrate or a groverd client, and print results.
package cli

import (
	"context"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/jgoldverg/grover/internal/client"
	"github.com/jgoldverg/grover/internal/config"
	"github.com/jgoldverg/grover/internal/logging"
)

// app is the state shared by every command in one invocation.
type app struct {
	configPath string
	profile    string
	server     string
	insecure   bool
	logLevel   string
	jsonOut    bool

	cfg  config.Client
	pool *client.Pool
}

// conn dials a node (profile name or host:port; empty = default node).
func (a *app) conn(ctx context.Context, node string) (*client.Conn, error) {
	return a.pool.Get(ctx, node)
}

func NewRootCommand() *cobra.Command {
	a := &app{}
	root := &cobra.Command{
		Use:   "grover",
		Short: "Control groverd nodes: routes, sessions, transfers, and job history",
		Long: `grover is the control plane for groverd. It never moves file bytes itself:
it tells groverd nodes what to prepare and which jobs to run, then reports progress.

Nodes are named by profile (grover profile set uc --server uc.example:22444) or by
host:port. Transfer locations are node:/absolute/path, or a bare /path on the
default node (--profile / --server).`,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.LoadClient(a.configPath)
			if err != nil {
				return err
			}
			if a.profile != "" {
				if _, ok := cfg.Profiles[a.profile]; !ok {
					return errUnknownProfile(a.profile, cfg)
				}
				cfg.ActiveProfile = a.profile
			}
			if a.server != "" {
				cfg.ActiveProfile, cfg.ServerURL = "", a.server
			}
			if a.insecure {
				cfg.InsecureControl = true
				for name, p := range cfg.Profiles {
					p.InsecureControl = true
					cfg.Profiles[name] = p
				}
			}
			level := cfg.LogLevel
			if a.logLevel != "" {
				level = a.logLevel
			}
			if err := logging.Setup(level, "text", os.Stderr); err != nil {
				return err
			}
			a.cfg = cfg
			a.pool = client.NewPool(cfg)
			return nil
		},
		PersistentPostRun: func(*cobra.Command, []string) {
			if a.pool != nil {
				a.pool.Close()
			}
		},
	}
	f := root.PersistentFlags()
	f.StringVar(&a.configPath, "config", "", "CLI config file (default ~/.grover/cli_config.toml)")
	f.StringVarP(&a.profile, "profile", "p", "", "profile to use as the default node")
	f.StringVar(&a.server, "server", "", "host:port of the default node (overrides --profile)")
	f.BoolVar(&a.insecure, "insecure", false, "dial every node without TLS")
	f.StringVar(&a.logLevel, "log-level", "", "debug|info|warn|error (default from config, else warn)")
	f.BoolVar(&a.jsonOut, "json", false, "print machine-readable JSON")

	root.AddCommand(
		profileCommand(a),
		routeCommand(a),
		transferCommand(a),
		jobCommand(a),
		scheduleCommand(a),
	)
	return root
}

func errUnknownProfile(name string, cfg config.Client) error {
	names := cfg.ProfileNames()
	if len(names) == 0 {
		return errorf("unknown profile %q (no profiles defined; see: grover profile set --help)", name)
	}
	return errorf("unknown profile %q (have: %s)", name, strings.Join(names, ", "))
}
