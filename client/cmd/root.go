package cmd

import (
	"context"

	"github.com/jgoldverg/GoRover/backend"
	"github.com/jgoldverg/GoRover/config"
	"github.com/spf13/cobra"
)

func NewRootCommand(credentialStorage backend.CredentialStorage, ac *config.AutoConfig) *cobra.Command {
	var rootCmd = &cobra.Command{
		Use:   "gorover",
		Short: "grover is a file transfer tool for common protocol",
		Long:  `grover is a CLI tool that can perform scatter and gather operations while supporting high levels of parallelism. Best of all we do network monitornig and reporting as well!`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	rootCmd.AddCommand(BackendCommand(context.Background(), credentialStorage))
	rootCmd.AddCommand(RemoteCredentialsCommand(context.Background(), credentialStorage))

	return rootCmd
}
