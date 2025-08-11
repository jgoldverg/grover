package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/fs"
	"github.com/jgoldverg/grover/cmd/grover-client/cli_output"
	"github.com/jgoldverg/grover/cmd/grover-client/remote"
	"github.com/jgoldverg/grover/config"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

type DryRunOpts struct {
	CredentialName string
	CredentialUUID string
}

type ListCommandOpts struct {
	CredentialName string
	Path           string
}

func BackendCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "backend",
		Short:   "Commands related to managing local and remote backend file servers",
		Long:    "Commands related to managing local and remote backend file servers",
		Aliases: []string{"b"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(DryRunCommand())
	cmd.AddCommand(listCommand())

	return cmd
}

func DryRunCommand() *cobra.Command {
	var opts DryRunOpts

	cmd := &cobra.Command{
		Use:   "dry-run",
		Short: "Command to dry run a credential, aka just connect and exit",
		Long:  "Command to dry run a credential, aka just connect and exit",
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.CredentialName == "" && opts.CredentialUUID == "" {
				return errors.New("Must provide either the credential uuid or the name")
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.CredentialName, "credential-name", "n", "The Credential name to use for doing a dry run")
	cmd.Flags().StringVar(&opts.CredentialUUID, "credential-uuid", "u", "The credential UUID to use for doing a dry run")
	return cmd
}

func listCommand() *cobra.Command {
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

			default:
				return fmt.Errorf("invalid endpoint-type: %s, must be one of [%s, %s]",
					endpointType, fs.ListerFilesystem, fs.ListerHTTP)
			}

			if opts.Path == "" {
				pwd, err := os.Getwd()
				if err != nil {
					return err
				}
				opts.Path = pwd
			}

			pterm.DefaultSection.Println("Listing files on backend")
			pterm.DefaultBasicText.Println("  Endpoint Type:", endpointType)
			pterm.DefaultBasicText.Println("  Path:", opts.Path)
			pterm.DefaultBasicText.Println("  Credential Name:", opts.CredentialName)

			appConfig := GetAppConfig(cmd)
			if len(appConfig.ServerURL) > 0 {
				//now we want to issue the proto request and not use the local ls
				files, err := remote.ListOnRemote(endpointType, appConfig.ServerURL, opts.Path)
				if err != nil {
					return err
				}

				if err := cli_output.PrintFileTable(files); err != nil {
					return err
				}
				return nil
			} else {
				storage, err := backend.NewTomlCredentialStorage(appConfig.CredentialsFile)
				if err != nil {
					return fmt.Errorf("failed to create credential store: path we got %s: %w", appConfig.CredentialsFile, err)
				}

				var cfg config.ListerConfig
				if opts.CredentialName == "" {
					cfg.Credential = nil
				} else {
					cred, err := storage.GetCredentialByName(opts.CredentialName)
					if err != nil {
						return err
					}
					cfg.Credential = cred
				}

				lister := backend.ListerFactory(endpointType, cfg)

				fileInfo := lister.List(opts.Path)

				if err := cli_output.PrintFileTable(fileInfo); err != nil {
					return err
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.CredentialName, "credential-name", "", "The Credential name to use for doing a list")
	cmd.Flags().StringVar(&opts.Path, "path", "", "The path to list (default is pwd)")

	return cmd
}
