package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/fs"
	"github.com/jgoldverg/grover/client/cli_output"
	"github.com/jgoldverg/grover/config"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

type DryRunOpts struct {
	CredentialName string
	CredentialUUID string
}

type ListCommandOpts struct {
	credentialName string
	path           string
}

func BackendCommand(ctx context.Context, credentialStorage fs.CredentialStorage) *cobra.Command {
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
	cmd.AddCommand(listCommand(ctx, credentialStorage))

	return cmd
}

func DryRunCommand(credentialStorage fs.CredentialStorage) *cobra.Command {
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

func listCommand(ctx context.Context, storage fs.CredentialStorage) *cobra.Command {
	opts := &ListCommandOpts{}

	cmd := &cobra.Command{
		Use:     "list <endpoint-type>",
		Short:   "List files of a backend",
		Aliases: []string{"l", "ls"},
		Args:    cobra.ExactArgs(1), // require exactly 1 positional argument: endpointType
		RunE: func(cmd *cobra.Command, args []string) error {
			endpointType := fs.ListerType(args[0])

			// Validate endpointType
			switch endpointType {
			case fs.ListerFilesystem, fs.ListerHTTP:
				// valid
			default:
				return fmt.Errorf("invalid endpoint-type: %s, must be one of [%s, %s]",
					endpointType, fs.ListerFilesystem, fs.ListerHTTP)
			}

			if opts.path == "" {
				pwd, err := os.Getwd()
				if err != nil {
					return err
				}
				opts.path = pwd
			}
			
			pterm.DefaultSection.Println("Listing files on backend")
			pterm.DefaultBasicText.Println("  Endpoint Type:", endpointType)
			pterm.DefaultBasicText.Println("  Path:", opts.path)
			pterm.DefaultBasicText.Println("  Credential Name:", opts.credentialName)

			var cfg config.ListerConfig
			if opts.credentialName == "" {
				cfg.Credential = nil
			} else {
				cred, err := storage.GetCredentialByName(opts.credentialName)
				if err != nil {
					return err
				}
				cfg.Credential = cred
			}

			lister := backend.ListerFactory(endpointType, cfg)

			fileInfo := lister.List(opts.path)

			if err := cli_output.PrintFileTable(fileInfo); err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&opts.credentialName, "credential-name", "", "The Credential name to use for doing a list")
	cmd.Flags().StringVar(&opts.path, "path", "", "The path to list (default is pwd)")

	return cmd
}
