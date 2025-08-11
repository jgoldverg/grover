package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jgoldverg/grover/config"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

type ctxKey string

const appCtxKey ctxKey = "appData"

func NewRootCommand() *cobra.Command {
	var appConfigPath string
	var serverURLFlag string

	rootCmd := &cobra.Command{
		Use:   "grover",
		Short: "grover is a file transfer tool for common protocols",
		Long:  `grover is a CLI tool that can perform scatter and gather operations while supporting high levels of parallelism. Best of all we do network monitoring and reporting as well!`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Load app config
			cfg, err := config.LoadAppConfig(appConfigPath)
			if err != nil {
				return fmt.Errorf("failed to load app config: %w", err)
			}

			// Override ServerURL if flag is set
			if serverURLFlag != "" {
				cfg.ServerURL = serverURLFlag
			}

			pterm.Println("Using credentials file:", cfg.CredentialsFile)

			// Ensure credentials file directory exists
			dir := filepath.Dir(cfg.CredentialsFile)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("failed to create directory for credentials file: %w", err)
			}

			ctx := context.WithValue(cmd.Context(), appCtxKey, cfg)
			cmd.SetContext(ctx)

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	rootCmd.PersistentFlags().StringVar(&appConfigPath, "app-config", "", "Path to app config file (TOML)")
	rootCmd.PersistentFlags().StringVar(&serverURLFlag, "server-url", "", "URL of the server to connect to")

	// Notice we pass no context or credentialStore here — subcommands get them from cmd.Context()
	rootCmd.AddCommand(BackendCommand())
	rootCmd.AddCommand(RemoteCredentialsCommand())

	return rootCmd
}

// Helper function for subcommands to get appData
func GetAppConfig(cmd *cobra.Command) *config.AppConfig {
	if v := cmd.Context().Value(appCtxKey); v != nil {
		if data, ok := v.(*config.AppConfig); ok {
			return data
		}
	}
	return nil
}
