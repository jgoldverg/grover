package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/fs"
	"github.com/jgoldverg/grover/cli/output"
	"github.com/jgoldverg/grover/server/log"
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

type RmCommandOpts struct {
	CredentialName string
	CredentialUUID string
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

	cmd.AddCommand(listResources())
	cmd.AddCommand(deleteResource())

	return cmd
}

func deleteResource() *cobra.Command {
	opts := &RmCommandOpts{}
	cmd := &cobra.Command{
		Use:     "delete <endpoint-type>",
		Short:   "Delete a file or folder from the backend",
		Aliases: []string{"rm", "r"},
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			endpointType := fs.BackendType(args[0])
			switch {
			case fs.IsBackendTypeValid(endpointType):
			default:
				return fmt.Errorf("invalid endpoint-type: %s, must be one of [%s, %s, %s]",
					endpointType, fs.LOCALFSBackend, fs.HTTPBackend, fs.GROVERBackend)
			}
			if opts.Path == "" {
				log.Structured(&pterm.Error, "must specify a path to delete from", nil)
			}
			pterm.DefaultSection.Println("Listing files on backend")
			pterm.DefaultBasicText.Println("  Endpoint Type:", endpointType)
			pterm.DefaultBasicText.Println("  Path:", opts.Path)
			pterm.DefaultBasicText.Println("  Credential Name:", opts.CredentialName)

			appConfig := GetAppConfig(cmd)

			if opts.CredentialUUID != "" && opts.CredentialName != "" {
				return errors.New("must specify the credential UUID or the credential name")
			}

			if len(appConfig.ServerURL) > 0 {
				//do this operation on the remote backend
				return fmt.Errorf("unsupported to rm from remote")
			} else {
				storage, err := backend.NewTomlCredentialStorage(appConfig.CredentialsFile)
				if err != nil {
					return fmt.Errorf("failed to create credential store: path we got %s: %w", appConfig.CredentialsFile, err)
				}
				var credential backend.Credential
				if opts.CredentialName != "" {
					credential, err = storage.GetCredentialByName(opts.CredentialName)
					if err != nil {
						return fmt.Errorf("failed to get credential by name: %w", err)
					}
				} else {
					credUuid, err := uuid.Parse(opts.CredentialUUID)
					if err != nil {
						return fmt.Errorf("invalid credential UUID '%s': %w", opts.CredentialUUID, err)
					}
					credential, err = storage.GetCredentialByUUID(credUuid)
					if err != nil {
						return fmt.Errorf("invalid credential UUID '%s': %w", opts.CredentialUUID, err)
					}
				}
				rmFactory := backend.RmFactory(fs.BackendType(credential.GetType()), credential)
				res := rmFactory.Rm(opts.Path)
				pterm.DefaultBasicText.Println("Successfully deleted files %s with value %t", opts.Path, res)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.CredentialName, "credential-name", "", "The Credential name to use for doing a list")
	cmd.Flags().StringVar(&opts.CredentialUUID, "credential-uuid", "", "The Credential UUID to use for doing a list")
	cmd.Flags().StringVar(&opts.Path, "path", "", "")

	return cmd
}

func listResources() *cobra.Command {
	opts := &ListCommandOpts{}

	cmd := &cobra.Command{
		Use:     "list <endpoint-type>",
		Short:   "List files of a backend",
		Aliases: []string{"l", "ls"},
		Args:    cobra.ExactArgs(1), // require exactly 1 positional argument: endpointType
		RunE: func(cmd *cobra.Command, args []string) error {
			endpointType := fs.BackendType(args[0])

			// Validate endpointType
			switch endpointType {
			case fs.LOCALFSBackend, fs.HTTPBackend:

			default:
				return fmt.Errorf("invalid endpoint-type: %s, must be one of [%s, %s, %s]",
					endpointType, fs.LOCALFSBackend, fs.HTTPBackend, fs.GROVERBackend)
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
				files, err := ListOnRemote(endpointType, appConfig.ServerURL, opts.Path)
				if err != nil {
					return err
				}

				if err := output.PrintFileTable(files); err != nil {
					return err
				}
				return nil
			} else {
				storage, err := backend.NewTomlCredentialStorage(appConfig.CredentialsFile)
				if err != nil {
					return fmt.Errorf("failed to create credential store: path we got %s: %w", appConfig.CredentialsFile, err)
				}

				var cred backend.Credential
				if opts.CredentialName != "" {
					cred, err = storage.GetCredentialByName(opts.CredentialName)
					if err != nil {
						return err
					}
				}

				lister := backend.ListerFactory(endpointType, cred)

				fileInfo := lister.List(opts.Path)

				if err := output.PrintFileTable(fileInfo); err != nil {
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
