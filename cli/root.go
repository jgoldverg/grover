package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jgoldverg/grover/internal"
	"github.com/spf13/cobra"
)

type ctxKey string

const appCtxKey ctxKey = "appData"
const appConfigPathKey ctxKey = "appConfigPath"

func NewRootCommand() *cobra.Command {
	var appConfigPath string
	var serverURLFlag string

	rootCmd := &cobra.Command{
		Use:   "grover",
		Short: "grover is a file transfer tool for common protocols",
		Long:  `grover is a CLI tool that can perform scatter and gather operations while supporting high levels of parallelism. Best of all we do network monitoring and reporting as well!`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Load app config
			cfg, err := internal.LoadAppConfig(appConfigPath)
			if err != nil {
				return fmt.Errorf("failed to load app config: %w", err)
			}

			// Override ServerURL if flag is set
			if serverURLFlag != "" {
				cfg.ServerURL = serverURLFlag
			}
			if err := internal.ConfigureLogger(cfg.LogLevel); err != nil {
				internal.Warn("invalid log level in app config, defaulting to info", internal.Fields{
					internal.FieldError: err.Error(),
				})
			}

			internal.Info("using credentials file", internal.Fields{
				internal.CredentialPath: cfg.CredentialsFile,
			})

			// Ensure credentials file directory exists
			dir := filepath.Dir(cfg.CredentialsFile)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("failed to create directory for credentials file: %w", err)
			}

			cfgPath := appConfigPath
			if strings.TrimSpace(cfgPath) == "" {
				home, err := os.UserHomeDir()
				if err != nil {
					return err
				}
				cfgPath = filepath.Join(home, ".grover", "cli_config.toml")
			}

			ctx := context.WithValue(cmd.Context(), appCtxKey, cfg)
			ctx = context.WithValue(ctx, appConfigPathKey, cfgPath)
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
	rootCmd.AddCommand(CredentialCommand())
	rootCmd.AddCommand(GroverServerOps())
	rootCmd.AddCommand(SimpleCopy())
	rootCmd.AddCommand(DownloadCommand())
	rootCmd.AddCommand(ConfigCommand())

	return rootCmd
}

// Helper function for subcommands to get appData
func GetAppConfig(cmd *cobra.Command) *internal.AppConfig {
	if v := cmd.Context().Value(appCtxKey); v != nil {
		if data, ok := v.(*internal.AppConfig); ok {
			return data
		}
	}
	return nil
}

func getAppConfigPath(cmd *cobra.Command) string {
	if v := cmd.Context().Value(appConfigPathKey); v != nil {
		if path, ok := v.(string); ok {
			return path
		}
	}
	return ""
}
