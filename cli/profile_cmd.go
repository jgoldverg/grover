package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jgoldverg/grover/internal"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

type profileOptions struct {
	serverURL       string
	caCertFile      string
	insecureControl bool
	secureControl   bool
	jsonOutput      bool
}

func ProfileCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage named groverd control-plane profiles",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(profileSetCommand())
	cmd.AddCommand(profileUseCommand())
	cmd.AddCommand(profileListCommand())
	cmd.AddCommand(profileShowCommand())
	cmd.AddCommand(profileDeleteCommand())
	return cmd
}

func profileSetCommand() *cobra.Command {
	opts := profileOptions{}
	cmd := &cobra.Command{
		Use:          "set <name>",
		Short:        "Create or update a control-plane profile",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := writableAppConfig(cmd)
			if err != nil {
				return err
			}
			name := strings.TrimSpace(args[0])
			if name == "" {
				return fmt.Errorf("profile name is required")
			}
			if opts.insecureControl && opts.secureControl {
				return fmt.Errorf("choose either --insecure-control or --secure-control")
			}
			if cfg.Profiles == nil {
				cfg.Profiles = map[string]internal.AppProfile{}
			}
			profile := cfg.Profiles[name]
			if cmd.Flags().Changed("server-url") {
				profile.ServerURL = strings.TrimSpace(opts.serverURL)
			}
			if cmd.Flags().Changed("ca-cert") {
				profile.CACertFile = strings.TrimSpace(opts.caCertFile)
			}
			if cmd.Flags().Changed("insecure-control") {
				profile.InsecureControl = true
			}
			if cmd.Flags().Changed("secure-control") {
				profile.InsecureControl = false
			}
			if strings.TrimSpace(profile.ServerURL) == "" {
				return fmt.Errorf("profile %q requires --server-url", name)
			}
			cfg.Profiles[name] = profile
			if _, err := cfg.Save(getAppConfigPath(cmd)); err != nil {
				return fmt.Errorf("saving CLI config: %w", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "saved profile %s\n", name)
			return printProfileTable(cmd.OutOrStdout(), cfg, []string{name})
		},
	}
	cmd.Flags().StringVar(&opts.serverURL, "server-url", "", "groverd control endpoint for this profile")
	cmd.Flags().StringVar(&opts.caCertFile, "ca-cert", "", "CA certificate for secure control-plane dialing")
	cmd.Flags().BoolVar(&opts.insecureControl, "insecure-control", false, "Use insecure control-plane dialing for this profile")
	cmd.Flags().BoolVar(&opts.secureControl, "secure-control", false, "Use TLS control-plane dialing for this profile")
	return cmd
}

func profileUseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "use <name>",
		Short:        "Make a profile the default for future commands",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := writableAppConfig(cmd)
			if err != nil {
				return err
			}
			name := strings.TrimSpace(args[0])
			if _, ok := cfg.Profiles[name]; !ok {
				return fmt.Errorf("profile %q not found", name)
			}
			cfg.ActiveProfile = name
			if _, err := cfg.Save(getAppConfigPath(cmd)); err != nil {
				return fmt.Errorf("saving CLI config: %w", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "active profile: %s\n", name)
			return nil
		},
	}
	return cmd
}

func profileListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "list",
		Short:        "List configured profiles",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := GetAppConfig(cmd)
			if cfg == nil {
				return fmt.Errorf("client config unavailable")
			}
			names := sortedProfileNames(cfg)
			if len(names) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no profiles configured")
				return nil
			}
			return printProfileTable(cmd.OutOrStdout(), cfg, names)
		},
	}
	return cmd
}

func profileShowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "show [name]",
		Short:        "Show one profile or the active profile",
		Args:         cobra.MaximumNArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := GetAppConfig(cmd)
			if cfg == nil {
				return fmt.Errorf("client config unavailable")
			}
			name := strings.TrimSpace(cfg.ActiveProfile)
			if len(args) == 1 {
				name = strings.TrimSpace(args[0])
			}
			if name == "" {
				return fmt.Errorf("no active profile; pass a profile name or run profile use <name>")
			}
			if _, ok := cfg.Profiles[name]; !ok {
				return fmt.Errorf("profile %q not found", name)
			}
			return printProfileTable(cmd.OutOrStdout(), cfg, []string{name})
		},
	}
	return cmd
}

func profileDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "delete <name>",
		Aliases:      []string{"rm"},
		Short:        "Delete a control-plane profile",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := writableAppConfig(cmd)
			if err != nil {
				return err
			}
			name := strings.TrimSpace(args[0])
			if _, ok := cfg.Profiles[name]; !ok {
				return fmt.Errorf("profile %q not found", name)
			}
			delete(cfg.Profiles, name)
			if cfg.ActiveProfile == name {
				cfg.ActiveProfile = ""
			}
			if _, err := cfg.Save(getAppConfigPath(cmd)); err != nil {
				return fmt.Errorf("saving CLI config: %w", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "deleted profile %s\n", name)
			return nil
		},
	}
	return cmd
}

func writableAppConfig(cmd *cobra.Command) (*internal.AppConfig, error) {
	cfg := GetAppConfig(cmd)
	if cfg == nil {
		return nil, fmt.Errorf("client config unavailable")
	}
	if cfg.Profiles == nil {
		cfg.Profiles = map[string]internal.AppProfile{}
	}
	return cfg, nil
}

func sortedProfileNames(cfg *internal.AppConfig) []string {
	if cfg == nil || len(cfg.Profiles) == 0 {
		return nil
	}
	names := make([]string, 0, len(cfg.Profiles))
	for name := range cfg.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func printProfileTable(w interface {
	Write([]byte) (int, error)
}, cfg *internal.AppConfig, names []string) error {
	if w == nil {
		return nil
	}
	rows := pterm.TableData{{"PROFILE", "ACTIVE", "SERVER", "CONTROL", "CA CERT"}}
	for _, name := range names {
		profile, ok := cfg.Profiles[name]
		if !ok {
			continue
		}
		control := "tls"
		if profile.InsecureControl {
			control = "insecure"
		}
		active := ""
		if cfg.ActiveProfile == name {
			active = "*"
		}
		caCert := strings.TrimSpace(profile.CACertFile)
		if caCert == "" {
			caCert = "-"
		}
		rows = append(rows, []string{name, active, profile.ServerURL, control, caCert})
	}
	rendered, err := pterm.DefaultTable.WithHasHeader().WithData(rows).Srender()
	if err != nil {
		return err
	}
	fmt.Fprintln(w, rendered)
	return nil
}
