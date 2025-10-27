package cli

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/cli/output"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/gclient"
	"github.com/jgoldverg/grover/pkg/util"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

type ListCommandOpts struct {
	CredentialName string
	CredentialUUID string
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

	cmd.PersistentFlags().String("via", "", "Where to execute: auto|client|server")
	cmd.PersistentFlags().Lookup("via").NoOptDefVal = "auto" // optional
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
			endpointType := backend.BackendType(args[0])
			switch {
			case backend.IsBackendTypeValid(endpointType):
			default:
				return fmt.Errorf("invalid endpoint-type: %s, must be one of [%s, %s, %s]",
					endpointType, backend.LOCALFSBackend, backend.HTTPBackend, backend.GROVERBackend)
			}

			if opts.Path == "" {
				internal.Error("must specify a path to delete from", nil)
			}
			pterm.DefaultSection.Println("Deleting files on backend")
			pterm.DefaultBasicText.Println("  Endpoint Type:", endpointType)
			pterm.DefaultBasicText.Println("  Path:", opts.Path)
			pterm.DefaultBasicText.Println("  Credential Name:", opts.CredentialName)

			appConfig := GetAppConfig(cmd)
			route := appConfig.Route
			if f := cmd.Flags().Lookup("via"); f != nil && f.Changed {
				route, _ = cmd.Flags().GetString("via")
			}

			if opts.CredentialUUID != "" {
				if _, err := uuid.Parse(opts.CredentialUUID); err != nil {
					return fmt.Errorf("invalid credential UUID '%s': %w", opts.CredentialUUID, err)
				}
			}

			policy := util.ParseRoutePolicy(route)
			gc := gclient.NewClient(*appConfig)
			if err := gc.Initialize(cmd.Context(), policy); err != nil {
				return err
			}
			defer gc.Close()

			endpoint := backend.Endpoint{
				Scheme:         string(endpointType),
				Paths:          []string{opts.Path},
				CredentialHint: opts.CredentialName,
				CredentialID:   opts.CredentialUUID,
			}
			if err := gc.Files().Remove(cmd.Context(), endpoint, opts.Path); err != nil {
				return err
			}
			pterm.DefaultBasicText.Printf("\n Successfully deleted files %s", opts.Path)
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
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			endpointType := backend.BackendType(args[0])
			switch endpointType {
			case backend.LOCALFSBackend, backend.HTTPBackend, backend.GROVERBackend:
			default:
				return fmt.Errorf("invalid endpoint-type: %s, must be one of [%s, %s, %s]",
					endpointType, backend.LOCALFSBackend, backend.HTTPBackend, backend.GROVERBackend)
			}

			pterm.DefaultSection.Println("Grover listing files")
			pterm.DefaultBasicText.Println("  Endpoint Type:", endpointType)
			pterm.DefaultBasicText.Println("  Path:", opts.Path)
			pterm.DefaultBasicText.Println("  Credential Name:", opts.CredentialName)

			appConfig := GetAppConfig(cmd)
			route := appConfig.Route
			if f := cmd.Flags().Lookup("via"); f != nil && f.Changed {
				route, _ = cmd.Flags().GetString("via")
			}
			policy := util.ParseRoutePolicy(route)

			gc := gclient.NewClient(*appConfig)
			if err := gc.Initialize(cmd.Context(), policy); err != nil {
				return err
			}
			defer gc.Close()

			if opts.CredentialUUID != "" {
				if _, err := uuid.Parse(opts.CredentialUUID); err != nil {
					return fmt.Errorf("invalid credential UUID: %w", err)
				}
			}

			endpoint := backend.Endpoint{
				Scheme:         string(endpointType),
				Paths:          []string{opts.Path},
				CredentialHint: opts.CredentialName,
				CredentialID:   opts.CredentialUUID,
			}

			files, err := gc.Files().List(cmd.Context(), endpoint)
			if err != nil {
				return err
			}
			err = output.PrintFileTable(files)
			if err != nil {
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.CredentialName, "credential-name", "", "The Credential name to use for doing a list")
	cmd.Flags().StringVar(&opts.CredentialUUID, "credential-uuid", "", "The Credential UUID to use for doing a list")
	cmd.Flags().StringVar(&opts.Path, "path", "", "The path to list (default is pwd)")
	return cmd
}
