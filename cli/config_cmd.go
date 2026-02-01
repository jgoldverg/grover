package cli

import (
	"fmt"
	"strings"

	"github.com/jgoldverg/grover/internal"
	"github.com/spf13/cobra"
)

func ConfigCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage grover CLI configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(configSetServerCommand())
	return cmd
}

func configSetServerCommand() *cobra.Command {
	var serverURL string
	var caPath string

	cmd := &cobra.Command{
		Use:   "set-server",
		Short: "Persist a default server URL (and optional CA certificate) in the CLI config",
		RunE: func(cmd *cobra.Command, args []string) error {
			url := strings.TrimSpace(serverURL)
			if url == "" {
				return fmt.Errorf("--server-url is required")
			}
			cfg := GetAppConfig(cmd)
			if cfg == nil {
				return fmt.Errorf("app config unavailable in context")
			}

			cfg.ServerURL = url
			if ca := strings.TrimSpace(caPath); ca != "" {
				cfg.CACertFile = ca
			}

			path := getAppConfigPath(cmd)
			if _, err := cfg.Save(path); err != nil {
				return fmt.Errorf("saving CLI config: %w", err)
			}

			internal.Info("CLI configuration updated", internal.Fields{
				"server_url": cfg.ServerURL,
				"config":     path,
			})
			return nil
		},
	}
	cmd.Flags().StringVar(&serverURL, "server-url", "", "Target server address (host:port or URI)")
	cmd.Flags().StringVar(&caPath, "ca-cert", "", "Path to the custom CA certificate for TLS")
	return cmd
}
