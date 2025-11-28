package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/filesystem"
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

type RenameCommandOpts struct {
	CredentialName string
	CredentialUUID string
	OldPath        string
	NewPath        string
}

type MkdirCommandOpts struct {
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
	cmd.PersistentFlags().Lookup("via").NoOptDefVal = "auto"
	cmd.AddCommand(listResources())
	cmd.AddCommand(deleteResource())
	cmd.AddCommand(mkdirResource())
	cmd.AddCommand(renameResource())

	return cmd
}

func mkdirResource() *cobra.Command {
	opts := &MkdirCommandOpts{}
	cmd := &cobra.Command{
		Use:     "mkdir <endpoint-type>",
		Short:   "Make a directory",
		Aliases: []string{"m"},
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			endpointType := backend.BackendType(args[0])
			switch {
			case backend.IsBackendTypeValid(endpointType):
			default:
				return fmt.Errorf("invalid endpoint-type: %s, must be one of [%s, %s, %s]",
					endpointType, backend.LOCALFSBackend, backend.HTTPBackend, backend.GROVERBackend)
			}

			path := strings.TrimSpace(opts.Path)
			if path == "" {
				return fmt.Errorf("--path is required")
			}

			ctx, err := newFileCommandContext(cmd, endpointType, opts.CredentialName, opts.CredentialUUID)
			if err != nil {
				return err
			}

			pterm.DefaultSection.Println("Mkdir files on backend")
			pterm.DefaultBasicText.Println("	Endpoint Type:", endpointType)
			pterm.DefaultBasicText.Println("	Path:", path)
			pterm.DefaultBasicText.Println("	Credential:", ctx.credentialLabel())

			if ctx.useRemote {
				return ctx.withRemoteClient(cmd.Context(), func(gc *gclient.Client) error {
					endpoint := ctx.buildEndpoint(path)
					if err := gc.Files().Mkdir(cmd.Context(), endpoint, path); err != nil {
						return err
					}
					pterm.DefaultBasicText.Printf("\n Successfully created directory %s", path)
					return nil
				})
			}

			cred, err := ctx.loadLocalCredential()
			if err != nil {
				return err
			}
			ops, err := backend.OpsFactory(endpointType, cred)
			if err != nil {
				return err
			}
			if err := ops.Mkdir(cmd.Context(), path); err != nil {
				return err
			}
			pterm.DefaultBasicText.Printf("\n Successfully created directory %s", path)

			return nil
		},
	}
	cmd.Flags().StringVar(&opts.CredentialName, "credential-name", "", "The Credential name to use for doing a list")
	cmd.Flags().StringVar(&opts.CredentialUUID, "credential-uuid", "", "The Credential UUID to use for doing a list")
	cmd.Flags().StringVar(&opts.Path, "path", "", "The path to make")

	return cmd
}

func renameResource() *cobra.Command {
	opts := RenameCommandOpts{}
	cmd := &cobra.Command{
		Use:   "rename <endpoint-type>",
		Short: "Rename the directory or the file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			endpointType := backend.BackendType(args[0])
			switch {
			case backend.IsBackendTypeValid(endpointType):
			default:
				return fmt.Errorf("invalid endpoint-type: %s, must be one of [%s, %s, %s]",
					endpointType, backend.LOCALFSBackend, backend.HTTPBackend, backend.GROVERBackend)
			}

			oldPath := strings.TrimSpace(opts.OldPath)
			newPath := strings.TrimSpace(opts.NewPath)
			if oldPath == "" || newPath == "" {
				return fmt.Errorf("--old-path and --new-path are required")
			}
			ctx, err := newFileCommandContext(cmd, endpointType, opts.CredentialName, opts.CredentialUUID)
			if err != nil {
				return err
			}

			pterm.DefaultSection.Println("Rename files on backend")
			pterm.DefaultBasicText.Println("	EndpointType:", endpointType)
			pterm.DefaultBasicText.Println("	OldPath:", oldPath)
			pterm.DefaultBasicText.Println("	NewPath:", newPath)
			pterm.DefaultBasicText.Println("	Credential Name:", ctx.credentialLabel())

			if ctx.useRemote {
				return ctx.withRemoteClient(cmd.Context(), func(gc *gclient.Client) error {
					endpoint := ctx.buildEndpoint()
					if err := gc.Files().Rename(cmd.Context(), endpoint, oldPath, newPath); err != nil {
						return err
					}
					internal.Info("renamed old path %s to new path %s", internal.Fields{
						"old_path": oldPath,
						"new_path": newPath,
					})
					return nil
				})
			}

			cred, err := ctx.loadLocalCredential()
			if err != nil {
				return err
			}

			ops, err := backend.OpsFactory(endpointType, cred)
			if err != nil {
				return err
			}
			if err := ops.Rename(cmd.Context(), oldPath, newPath); err != nil {
				return err
			}

			internal.Info("renamed old path %s to new path %s", internal.Fields{
				"old_path": oldPath,
				"new_path": newPath,
			})
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.OldPath, "old-path", "", "The path to rename from")
	cmd.Flags().StringVar(&opts.NewPath, "new-path", "", "The path to rename to")
	cmd.Flags().StringVar(&opts.CredentialName, "credential-name", "", "The Credential name to use for doing a list")
	cmd.Flags().StringVar(&opts.CredentialUUID, "credential-uuid", "", "The Credential UUID to use for doing a list")

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

			path := strings.TrimSpace(opts.Path)
			if path == "" {
				return fmt.Errorf("--path is required")
			}

			ctx, err := newFileCommandContext(cmd, endpointType, opts.CredentialName, opts.CredentialUUID)
			if err != nil {
				return err
			}

			pterm.DefaultSection.Println("Deleting files on backend")
			pterm.DefaultBasicText.Println("  Endpoint Type:", endpointType)
			pterm.DefaultBasicText.Println("  Path:", path)
			pterm.DefaultBasicText.Println("  Credential Name:", ctx.credentialLabel())

			if ctx.useRemote {
				return ctx.withRemoteClient(cmd.Context(), func(gc *gclient.Client) error {
					endpoint := ctx.buildEndpoint(path)
					if err := gc.Files().Remove(cmd.Context(), endpoint, path); err != nil {
						return err
					}
					pterm.DefaultBasicText.Printf("\n Successfully deleted files %s", path)
					return nil
				})
			}

			cred, err := ctx.loadLocalCredential()
			if err != nil {
				return err
			}
			ops, err := backend.OpsFactory(endpointType, cred)
			if err != nil {
				return err
			}
			if err := ops.Remove(cmd.Context(), path); err != nil {
				return err
			}
			pterm.DefaultBasicText.Printf("\n Successfully deleted files %s", path)
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

			path := strings.TrimSpace(opts.Path)
			if path == "" {
				return fmt.Errorf("--path is required")
			}

			pterm.DefaultSection.Println("Grover listing files")
			pterm.DefaultBasicText.Println("  Endpoint Type:", endpointType)
			pterm.DefaultBasicText.Println("  Path:", path)
			ctx, err := newFileCommandContext(cmd, endpointType, opts.CredentialName, opts.CredentialUUID)
			if err != nil {
				return err
			}
			pterm.DefaultBasicText.Println("  Credential Name:", ctx.credentialLabel())

			var files []filesystem.FileInfo
			switch endpointType {
			case backend.GROVERBackend:
				files, err = listGroverBackend(cmd.Context(), ctx.appConfig, ctx.credName, ctx.credUUID, path)
			default:
				if ctx.useRemote {
					err = ctx.withRemoteClient(cmd.Context(), func(gc *gclient.Client) error {
						endpoint := ctx.buildEndpoint(path)
						results, listErr := gc.Files().List(cmd.Context(), endpoint)
						if listErr != nil {
							return listErr
						}
						files = results
						return nil
					})
				} else {
					cred, loadErr := ctx.loadLocalCredential()
					if loadErr != nil {
						return loadErr
					}
					ops, opsErr := backend.OpsFactory(endpointType, cred)
					if opsErr != nil {
						return opsErr
					}
					files, err = ops.List(cmd.Context(), path, false)
				}
			}
			if err != nil {
				return err
			}
			return output.PrintFileTable(files)
		},
	}

	cmd.Flags().StringVar(&opts.CredentialName, "credential-name", "", "The Credential name to use for doing a list")
	cmd.Flags().StringVar(&opts.CredentialUUID, "credential-uuid", "", "The Credential UUID to use for doing a list")
	cmd.Flags().StringVar(&opts.Path, "path", "", "The path to list")
	_ = cmd.MarkFlagRequired("path")
	return cmd
}

type fileCommandContext struct {
	endpointType    backend.BackendType
	needsCredential bool
	credName        string
	credUUID        uuid.UUID
	credUUIDStr     string
	appConfig       *internal.AppConfig
	policy          util.RoutePolicy
	useRemote       bool
}

func newFileCommandContext(cmd *cobra.Command, endpointType backend.BackendType, credName, credUUIDStr string) (*fileCommandContext, error) {
	appConfig := GetAppConfig(cmd)
	trimmedName := strings.TrimSpace(credName)
	trimmedUUID := strings.TrimSpace(credUUIDStr)
	var parsedID uuid.UUID
	var err error
	if trimmedUUID != "" {
		if parsedID, err = uuid.Parse(trimmedUUID); err != nil {
			return nil, fmt.Errorf("invalid credential UUID: %w", err)
		}
	}

	needsCredential := endpointType != backend.LOCALFSBackend
	if needsCredential && trimmedName == "" && trimmedUUID == "" {
		return nil, fmt.Errorf("--credential-name or --credential-uuid is required for %s backends", endpointType)
	}

	policy := resolveRoutePolicy(cmd, appConfig.Route)
	useRemote := policy == util.RouteForceRemote ||
		(policy == util.RouteAuto && strings.TrimSpace(appConfig.ServerURL) != "")

	return &fileCommandContext{
		endpointType:    endpointType,
		needsCredential: needsCredential,
		credName:        trimmedName,
		credUUID:        parsedID,
		credUUIDStr:     trimmedUUID,
		appConfig:       appConfig,
		policy:          policy,
		useRemote:       useRemote,
	}, nil
}

func (c *fileCommandContext) credentialLabel() string {
	if !c.needsCredential {
		return "<local-only>"
	}
	if c.credName != "" {
		return c.credName
	}
	if c.credUUIDStr != "" {
		return c.credUUIDStr
	}
	if c.credUUID != uuid.Nil {
		return c.credUUID.String()
	}
	return "<unspecified>"
}

func (c *fileCommandContext) withRemoteClient(ctx context.Context, fn func(*gclient.Client) error) error {
	gc := gclient.NewClient(*c.appConfig)
	if err := gc.Initialize(ctx, c.policy); err != nil {
		return err
	}
	defer gc.Close()
	return fn(gc)
}

func (c *fileCommandContext) loadLocalCredential() (backend.Credential, error) {
	if !c.needsCredential {
		return nil, nil
	}
	cred, err := loadCredentialByRef(c.appConfig, c.credName, c.credUUID)
	if err != nil {
		return nil, err
	}
	if c.credName == "" {
		c.credName = cred.GetName()
	}
	if c.credUUID == uuid.Nil {
		if id := cred.GetUUID(); id != uuid.Nil {
			c.credUUID = id
			c.credUUIDStr = id.String()
		}
	}
	return cred, nil
}

func (c *fileCommandContext) buildEndpoint(paths ...string) backend.Endpoint {
	hint, id := c.remoteCredentialRefs()
	ep := backend.Endpoint{
		Scheme:         string(c.endpointType),
		CredentialHint: hint,
		CredentialID:   id,
	}
	if len(paths) > 0 {
		ep.Paths = append([]string(nil), paths...)
	}
	return ep
}

func (c *fileCommandContext) remoteCredentialRefs() (string, string) {
	if !c.needsCredential {
		return "", ""
	}
	id := c.credUUIDStr
	if id == "" && c.credUUID != uuid.Nil {
		id = c.credUUID.String()
	}
	return c.credName, id
}

func listGroverBackend(ctx context.Context, cfg *internal.AppConfig, credName string, credID uuid.UUID, path string) ([]filesystem.FileInfo, error) {
	cred, err := loadCredentialByRef(cfg, credName, credID)
	if err != nil {
		return nil, err
	}
	basic, ok := cred.(*backend.BasicAuthCredential)
	if !ok {
		return nil, fmt.Errorf("credential %q must be a basic credential to connect to a grover server", cred.GetName())
	}

	remoteCfg := *cfg
	remoteCfg.ServerURL = basic.GetUrl()

	client := gclient.NewClient(remoteCfg)
	if err := client.Initialize(ctx, util.RouteForceRemote); err != nil {
		return nil, fmt.Errorf("connect to grover server %q: %w", remoteCfg.ServerURL, err)
	}
	defer client.Close()

	endpoint := backend.Endpoint{
		Scheme: string(backend.LOCALFSBackend),
		Paths:  []string{path},
	}
	return client.Files().List(ctx, endpoint)
}

func loadCredentialByRef(cfg *internal.AppConfig, credName string, credID uuid.UUID) (backend.Credential, error) {
	store, err := backend.NewTomlCredentialStorage(cfg.CredentialsFile)
	if err != nil {
		return nil, fmt.Errorf("load credential store: %w", err)
	}
	switch {
	case credID != uuid.Nil:
		return store.GetCredentialByUUID(credID)
	case credName != "":
		return store.GetCredentialByName(credName)
	default:
		return nil, fmt.Errorf("credential reference required")
	}
}
