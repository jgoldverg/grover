package cli

import (
	"errors"
	"os/user"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/cli/output"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/gclient"
	"github.com/jgoldverg/grover/pkg/util"
	"github.com/spf13/cobra"
)

type AddCredentialOpts struct {
	URL            string
	CredentialName string
	SSH            SSHCredentialOpts
	Basic          BasicAuthCredentialOpts
	JWT            JWTCredentialOpts
	S3             S3CredentialOpts
}

type SSHCredentialOpts struct {
	Username   string
	Password   string
	KeyPath    string
	Passphrase string
	UseAgent   bool
	Port       int
}

type BasicAuthCredentialOpts struct {
	Username string
	Password string
}

type JWTCredentialOpts struct {
	Token              string
	RefreshToken       string
	TokenExpiry        int64
	RefreshTokenExpiry int64
}

type S3CredentialOpts struct {
	AccessKey    string
	SecretKey    string
	SessionToken string
	Region       string
	Bucket       string
	Endpoint     string
}

type DeleteCredentialOpts struct {
	CredentialName string
	CredentialUUID string
}

func CredentialCommand() *cobra.Command {
	var commonOpts AddCredentialOpts

	cmd := &cobra.Command{
		Use:     "credential",
		Short:   "Manage credentials",
		Aliases: []string{"creds", "c"},
	}

	// Add persistent flags
	cmd.PersistentFlags().StringVarP(&commonOpts.URL, "--server-url", "u", "", "Backend URL")
	cmd.PersistentFlags().StringVarP(&commonOpts.CredentialName, "name", "n", "", "Credential name")
	cmd.PersistentFlags().String("via", "", "Where to execute: auto|client|server")
	cmd.PersistentFlags().Lookup("via").NoOptDefVal = "auto" // optional

	// Store subcommand constructors (not instances)
	cmd.AddCommand(ListCredentialCommand())
	cmd.AddCommand(DeleteCredentialCommand())
	cmd.AddCommand(AddBasicCredentialCommand(&commonOpts))
	cmd.AddCommand(AddSShCredentialCommand(&commonOpts))
	return cmd
}

func AddBasicCredentialCommand(commonOpts *AddCredentialOpts) *cobra.Command {
	var (
		basicOpts BasicAuthCredentialOpts
	)
	cmd := &cobra.Command{
		Use:   "add-basic",
		Long:  "Add a basic credential for a remote server",
		Short: "Add a basic credential for a remote server",
		RunE: func(cmd *cobra.Command, args []string) error {
			if commonOpts.URL == "" {
				return errors.New("must specify a URL to the server")
			}
			if commonOpts.CredentialName == "" {
				return errors.New("must specify a credential name")
			}
			if basicOpts.Username == "" {
				currentUser, err := user.Current()
				if err != nil {
					internal.Error("failed to get current user", internal.Fields{
						internal.FieldError: err.Error(),
					})
					return err
				}
				internal.Info("using current user for username", internal.Fields{
					internal.FieldKey("username"): currentUser.Username,
				})
				basicOpts.Username = currentUser.Username
			}
			credential := &backend.BasicAuthCredential{
				Name:     commonOpts.CredentialName,
				Username: basicOpts.Username,
				Password: basicOpts.Password,
				URL:      commonOpts.URL,
				UUID:     uuid.New(),
			}
			err := credential.Validate()
			if err != nil {
				return errors.New("Failed to validate basic-credential: " + err.Error())
			}

			cfg := GetAppConfig(cmd)
			route := cfg.Route
			if f := cmd.Flags().Lookup("via"); f != nil && f.Changed {
				route, _ = cmd.Flags().GetString("via")
			}
			policy := util.ParseRoutePolicy(route)
			gc := gclient.NewClient(*cfg)
			err = gc.Initialize(cmd.Context(), policy)
			if err != nil {
				return err
			}
			defer gc.Close()

			return gc.Credentials().AddCredential(cmd.Context(), credential)
		},
	}
	cmd.Flags().StringVar(&commonOpts.URL, "url", "", "Backend URL")
	cmd.Flags().StringVar(&commonOpts.CredentialName, "name", "", "Credential name")
	_ = cmd.MarkFlagRequired("url")
	_ = cmd.MarkFlagRequired("name")
	cmd.Flags().StringVar(&basicOpts.Username, "username", "", "Basic auth username")
	cmd.Flags().StringVar(&basicOpts.Password, "password", "", "Basic auth password")
	return cmd
}

func AddSShCredentialCommand(commonOpts *AddCredentialOpts) *cobra.Command {
	var (
		sshOpts SSHCredentialOpts
	)
	cmd := &cobra.Command{
		Use:  "add-ssh",
		Long: "Adds a new SSH credential",
		RunE: func(cmd *cobra.Command, args []string) error {
			if commonOpts.URL == "" {
				return errors.New("URL flag is required")
			}
			if commonOpts.CredentialName == "" {
				return errors.New("credential name flag is required")
			}
			if sshOpts.Username == "" {
				currentUser, err := user.Current()
				if err != nil {
					internal.Error("failed to get current user", internal.Fields{
						internal.FieldError: err.Error(),
					})
					return err
				}
				internal.Info("using current user for username", internal.Fields{
					internal.FieldKey("username"): currentUser.Username,
				})
				sshOpts.Username = currentUser.Username
			}
			credential := &backend.SSHCredential{
				Name:           commonOpts.CredentialName,
				Username:       sshOpts.Username,
				Host:           commonOpts.URL,
				Port:           sshOpts.Port,
				PrivateKeyPath: sshOpts.KeyPath,
				PublicKeyPath:  sshOpts.KeyPath,
				UUID:           uuid.New(),
				UseAgent:       sshOpts.UseAgent,
			}
			err := credential.Validate()
			if err != nil {
				return errors.New("Failed in validating SSH credential: " + err.Error())
			}
			cfg := GetAppConfig(cmd)
			route := cfg.Route
			if f := cmd.Flags().Lookup("via"); f != nil && f.Changed {
				route, _ = cmd.Flags().GetString("via")
			}
			policy := util.ParseRoutePolicy(route)
			gc := gclient.NewClient(*cfg)
			err = gc.Initialize(cmd.Context(), policy)
			if err != nil {
				return err
			}
			defer gc.Close()

			return gc.Credentials().AddCredential(cmd.Context(), credential)
		},
	}

	// SSH-specific flags
	cmd.Flags().StringVar(&sshOpts.Username, "username", "", "SSH username")
	cmd.Flags().StringVar(&sshOpts.Password, "password", "", "SSH password")
	cmd.Flags().StringVar(&sshOpts.KeyPath, "key-path", "", "Path to private key")
	cmd.Flags().StringVar(&sshOpts.Passphrase, "passphrase", "", "Key passphrase")
	cmd.Flags().BoolVar(&sshOpts.UseAgent, "use-agent", false, "Use SSH agent")
	cmd.Flags().IntVar(&sshOpts.Port, "port", 22, "SSH port")
	return cmd
}

func ListCredentialCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "l"},
		Long:    "List credentials stored in the credential storage",
		Short:   "List credentials stored in the credential storage",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := GetAppConfig(cmd)
			route := cfg.Route
			if f := cmd.Flags().Lookup("via"); f != nil && f.Changed {
				route, _ = cmd.Flags().GetString("via")
			}
			policy := util.ParseRoutePolicy(route)
			gc := gclient.NewClient(*cfg)
			err := gc.Initialize(cmd.Context(), policy)
			if err != nil {
				return err
			}
			defer gc.Close()
			internal.Info("created and initialized grover client", nil)

			creds, err := gc.Credentials().ListCredentials(cmd.Context(), "")
			if err != nil {
				return errors.New("Failed to list the stored credentials: " + err.Error())
			}
			if len(creds) == 0 {
				internal.Info("no credentials added", nil)
			}
			output.VisualizeCredentialList(creds)
			return nil
		},
	}

	return cmd
}

func DeleteCredentialCommand() *cobra.Command {
	var deleteCredOpts DeleteCredentialOpts

	cmd := &cobra.Command{
		Use:     "delete",
		Aliases: []string{"rm", "d"},
		Long:    "Delete a credential from the configured credential store path",
		RunE: func(cmd *cobra.Command, args []string) error {
			if deleteCredOpts.CredentialName == "" && deleteCredOpts.CredentialUUID == "" {
				return errors.New("must pass in either the credential name or the credential uuid")
			}

			cfg := GetAppConfig(cmd)
			route := cfg.Route
			if f := cmd.Flags().Lookup("via"); f != nil && f.Changed {
				route, _ = cmd.Flags().GetString("via")
			}
			policy := util.ParseRoutePolicy(route)
			gc := gclient.NewClient(*cfg)
			if err := gc.Initialize(cmd.Context(), policy); err != nil {
				return err
			}
			defer gc.Close()

			var credUUID uuid.UUID
			if deleteCredOpts.CredentialUUID != "" {
				parsed, err := uuid.Parse(deleteCredOpts.CredentialUUID)
				if err != nil {
					return errors.New("the credential uuid is not valid: " + err.Error())
				}
				credUUID = parsed
			}

			return gc.Credentials().DeleteCredential(cmd.Context(), credUUID, deleteCredOpts.CredentialName)
		},
	}

	cmd.Flags().StringVar(&deleteCredOpts.CredentialName, "name", "", "The name of the stored credential")
	cmd.Flags().StringVar(&deleteCredOpts.CredentialUUID, "uuid", "", "The uuid assigned to the credential")

	return cmd
}
