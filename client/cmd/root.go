package cmd

import (
	"context"
	"os"
	"path/filepath"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/fs"
	"github.com/jgoldverg/grover/config"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var (
	appConfigPath   string
	appCfg          *config.AppConfig
	credentialStore fs.CredentialStorage
)

func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "gorover",
		Short: "grover is a file transfer tool for common protocols",
		Long:  `grover is a CLI tool that can perform scatter and gather operations while supporting high levels of parallelism. Best of all we do network monitoring and reporting as well!`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	// Persistent flag for app config file path
	rootCmd.PersistentFlags().StringVar(&appConfigPath, "app-config", "", "Path to app config file (TOML)")

	// Initialize config and credential storage before running any command
	cobra.OnInitialize(initConfigAndStorage)

	// Pass a pointer to the credential store so commands can use it
	rootCmd.AddCommand(BackendCommand(context.Background(), credentialStore))
	rootCmd.AddCommand(RemoteCredentialsCommand(context.Background(), credentialStore))

	return rootCmd
}

func initConfigAndStorage() {
	cfg, err := config.LoadAppConfig(appConfigPath)
	if err != nil {
		pterm.Error.Printf("Failed to load app config: %v\n", err)
		os.Exit(1)
	}
	appCfg = cfg

	pterm.Println("Using credentials file:", appCfg.CredentialsFile)

	// Make sure the directory for the credentials file exists
	dir := filepath.Dir(appCfg.CredentialsFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		pterm.Error.Printf("Failed to create directory for credentials file: %v\n", err)
		os.Exit(1)
	}

	store, err := backend.NewTomlCredentialStorage(appCfg.CredentialsFile)
	if err != nil {
		pterm.Error.Printf("Failed to initialize credential storage: %v\n", err)
		os.Exit(1)
	}
	credentialStore = store
}
