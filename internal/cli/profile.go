package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/jgoldverg/grover/internal/config"
)

func profileCommand(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage named groverd nodes (also usable as node:/path aliases)",
	}
	cmd.AddCommand(profileSet(a), profileUse(a), profileList(a), profileDelete(a))
	return cmd
}

func profileSet(a *app) *cobra.Command {
	var server, caCert string
	var insecure, secure bool
	cmd := &cobra.Command{
		Use:     "set <name>",
		Short:   "Create or update a profile",
		Example: "  grover profile set uc --server uc.example.edu:22444 --ca-cert ~/.grover/certs/uc.crt\n  grover profile set local --server localhost:22444 --insecure-control",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := strings.TrimSpace(args[0])
			if name == "" || strings.ContainsAny(name, ":/ ") {
				return errorf("profile name %q must not contain ':', '/', or spaces", name)
			}
			cfg, err := config.LoadClient(a.configPath) // unmodified by global flags
			if err != nil {
				return err
			}
			p := cfg.Profiles[name]
			if cmd.Flags().Changed("server") {
				p.ServerURL = strings.TrimSpace(server)
			}
			if cmd.Flags().Changed("ca-cert") {
				p.CACertFile = strings.TrimSpace(caCert)
			}
			if insecure && secure {
				return errorf("choose --insecure-control or --tls, not both")
			}
			if insecure {
				p.InsecureControl = true
			}
			if secure {
				p.InsecureControl = false
			}
			if p.ServerURL == "" {
				return errorf("profile %q needs --server host:port", name)
			}
			cfg.Profiles[name] = p
			if cfg.ActiveProfile == "" {
				cfg.ActiveProfile = name
			}
			if err := config.SaveClient(a.configPath, cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "saved profile %s\n", name)
			printProfiles(cmd, cfg)
			return nil
		},
	}
	cmd.Flags().StringVar(&server, "server", "", "groverd control address host:port")
	cmd.Flags().StringVar(&caCert, "ca-cert", "", "CA certificate for TLS")
	cmd.Flags().BoolVar(&insecure, "insecure-control", false, "dial this node without TLS")
	cmd.Flags().BoolVar(&secure, "tls", false, "dial this node with TLS (undo --insecure-control)")
	return cmd
}

func profileUse(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "use <name>",
		Short: "Make a profile the default node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.LoadClient(a.configPath)
			if err != nil {
				return err
			}
			if _, ok := cfg.Profiles[args[0]]; !ok {
				return errUnknownProfile(args[0], cfg)
			}
			cfg.ActiveProfile = args[0]
			if err := config.SaveClient(a.configPath, cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "default node is now %s\n", args[0])
			return nil
		},
	}
}

func profileList(a *app) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List profiles",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.LoadClient(a.configPath)
			if err != nil {
				return err
			}
			if a.jsonOut {
				return printJSON(cmd.OutOrStdout(), cfg)
			}
			printProfiles(cmd, cfg)
			return nil
		},
	}
}

func profileDelete(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.LoadClient(a.configPath)
			if err != nil {
				return err
			}
			if _, ok := cfg.Profiles[args[0]]; !ok {
				return errUnknownProfile(args[0], cfg)
			}
			delete(cfg.Profiles, args[0])
			if cfg.ActiveProfile == args[0] {
				cfg.ActiveProfile = ""
			}
			if err := config.SaveClient(a.configPath, cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "deleted profile %s\n", args[0])
			return nil
		},
	}
}

func printProfiles(cmd *cobra.Command, cfg config.Client) {
	t := table(cmd.OutOrStdout())
	fmt.Fprintln(t, "\tNAME\tSERVER\tTLS\tCA CERT")
	for _, name := range cfg.ProfileNames() {
		p := cfg.Profiles[name]
		mark := ""
		if name == cfg.ActiveProfile {
			mark = "*"
		}
		tls := "yes"
		if p.InsecureControl {
			tls = "no"
		}
		fmt.Fprintf(t, "%s\t%s\t%s\t%s\t%s\n", mark, name, p.ServerURL, tls, dash(p.CACertFile))
	}
	t.Flush()
	if len(cfg.Profiles) == 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "no profiles; default node is %s\n", cfg.ServerURL)
	}
}
