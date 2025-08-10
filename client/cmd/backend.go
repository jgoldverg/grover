package cmd

import (
	"context"
	"errors"

	"github.com/jgoldverg/GoRover/backend"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

type DryRunOpts struct {
	CredentialName string
	CredentialUUID string
}

func BackendCommand(ctx context.Context, credentialStorage backend.CredentialStorage) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "backend",
		Short:   "Commands related to managing local and remote backend file servers",
		Long:    "Commands related to managing local and remote backend file servers",
		Aliases: []string{"b"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(DryRunCommand(credentialStorage))
	cmd.AddCommand(listCommand(ctx))

	return cmd
}

func DryRunCommand(credentialStorage backend.CredentialStorage) *cobra.Command {
	var opts DryRunOpts

	cmd := &cobra.Command{
		Use:   "dry-run",
		Short: "Command to dry run a credential, aka just connect and exit",
		Long:  "Command to dry run a credential, aka just connect and exit",
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.CredentialName == "" && opts.CredentialUUID == "" {
				return errors.New("Must provide either the credential uuid or the name")
			}
			// var credential backend.Credential
			// if opts.CredentialName != "" {
			// 	credential, err := credentialStorage.GetCredentialByName(opts.CredentialName)
			// 	if err != nil {
			// 		pterm.Error.Printf("Failed to get credential for name = %s", credential)
			// 	}
			// }
			// if opts.CredentialUUID != "" {
			// 	credUuid, err := uuid.Parse(opts.CredentialUUID)
			// 	if err != nil {
			// 		pterm.Error.Printf("Failed to parse passing in UUID = %s", opts.CredentialUUID)
			// 	}
			// 	credential, err := credentialStorage.GetCredentialByUUID(credUuid)
			// }

			return nil
		},
	}

	cmd.Flags().StringVar(&opts.CredentialName, "credential-name", "n", "The Credential name to use for doing a dry run")
	cmd.Flags().StringVar(&opts.CredentialUUID, "credential-uuid", "u", "The credential UUID to use for doing a dry run")

	return cmd
}

type ListCommandOpts struct {
	credentialName string
	path           string
}

func listCommand(ctx context.Context) *cobra.Command {
	opts := &ListCommandOpts{}
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "list files in of a backend",
		Aliases: []string{"l", "ls"},
		RunE: func(cmd *cobra.Command, args []string) error {
			pterm.Success.Printfln("Listing files on backend credential=%s path=%s", opts.credentialName, opts.path)
			return nil
		},
	}

	return cmd
}
