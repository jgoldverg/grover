package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jgoldverg/grover/internal"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func ConfigCommand() *cobra.Command {
	var serverConfigPath string
	cmd := &cobra.Command{
		Use:   "config",
		Short: "View or update grover configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.PersistentFlags().StringVar(&serverConfigPath, "server-config", "", "Path to the grover server config file")
	cmd.AddCommand(configSetCommand(&serverConfigPath))
	return cmd
}

func configSetCommand(serverConfigPath *string) *cobra.Command {
	var target string
	var clientServerURL string
	var clientCACert string

	var serverPort int
	var serverCert string
	var serverKey string
	var serverCreds string
	var serverLogLevel string

	cmd := &cobra.Command{
		Use:   "set",
		Short: "Update the client (app) or server configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			scope := strings.ToLower(strings.TrimSpace(target))
			if scope == "" {
				scope = "client"
			}
			switch scope {
			case "client":
				return updateClientConfig(cmd, clientServerURL, clientCACert)
			case "server":
				return updateServerConfig(cmd, serverConfigPath, cmd.Flags(), serverPort, serverCert, serverKey, serverCreds, serverLogLevel)
			default:
				return fmt.Errorf("--target must be either client or server")
			}
		},
	}

	cmd.Flags().StringVar(&target, "target", "client", "Which config to update: client or server")
	cmd.Flags().StringVar(&clientServerURL, "server-url", "", "Client mode: default server URL (host:port)")
	cmd.Flags().StringVar(&clientCACert, "ca-cert", "", "Client mode: path to custom CA certificate")

	cmd.Flags().IntVar(&serverPort, "listen-port", 0, "Server mode: gRPC listen port")
	cmd.Flags().StringVar(&serverCert, "server-cert", "", "Server mode: path to TLS certificate PEM")
	cmd.Flags().StringVar(&serverKey, "server-key", "", "Server mode: path to TLS private key PEM")
	cmd.Flags().StringVar(&serverCreds, "credentials-file", "", "Server mode: credential store path")
	cmd.Flags().StringVar(&serverLogLevel, "log-level", "", "Server mode: log level (info, debug, ...)")
	return cmd
}

func updateClientConfig(cmd *cobra.Command, serverURL, caPath string) error {
	url := strings.TrimSpace(serverURL)
	ca := strings.TrimSpace(caPath)
	if url == "" && ca == "" {
		return fmt.Errorf("client config: provide --server-url and/or --ca-cert")
	}

	cfg := GetAppConfig(cmd)
	if cfg == nil {
		return fmt.Errorf("client config unavailable")
	}

	if url != "" {
		cfg.ServerURL = url
	}
	if ca != "" {
		cfg.CACertFile = ca
	}

	path := getAppConfigPath(cmd)
	if _, err := cfg.Save(path); err != nil {
		return fmt.Errorf("saving CLI config: %w", err)
	}
	internal.Info("CLI configuration updated", internal.Fields{
		"server_url": cfg.ServerURL,
		"ca_cert":    cfg.CACertFile,
		"config":     path,
	})
	return nil
}

func updateServerConfig(cmd *cobra.Command, cfgPathPtr *string, flagSet *pflag.FlagSet, port int, cert, key, credFile, logLevel string) error {
	path := strings.TrimSpace(*cfgPathPtr)
	if path == "" {
		path = defaultServerConfigPath()
	}

	if err := ensureServerConfigFile(path); err != nil {
		return err
	}

	cfg, err := internal.LoadServerConfig(path)
	if err != nil {
		return fmt.Errorf("load server config: %w", err)
	}

	if flagSet.Changed("listen-port") {
		if port <= 0 {
			return fmt.Errorf("server port must be > 0")
		}
		cfg.Port = port
	}
	if flagSet.Changed("server-cert") {
		cfg.ServerCertificatePath = cert
	}
	if flagSet.Changed("server-key") {
		cfg.ServerKeyPath = key
	}
	if flagSet.Changed("credentials-file") {
		cfg.CredentialsFile = credFile
	}
	if flagSet.Changed("log-level") {
		cfg.LogLevel = logLevel
	}

	if _, err := cfg.Save(path); err != nil {
		return fmt.Errorf("saving server config: %w", err)
	}
	internal.Info("Server configuration updated", internal.Fields{
		"config": path,
	})
	return nil
}

func defaultServerConfigPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "server_config.toml"
	}
	return filepath.Join(home, ".grover", "server_config.toml")
}

func ensureServerConfigFile(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat server config: %w", err)
	}

	defaultCfg, err := internal.LoadServerConfig("")
	if err != nil {
		return fmt.Errorf("load default server config: %w", err)
	}
	if _, err := defaultCfg.Save(path); err != nil {
		return fmt.Errorf("create server config: %w", err)
	}
	return nil
}
