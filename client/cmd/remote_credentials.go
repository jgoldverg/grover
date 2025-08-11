package cmd

import (
	"errors"
	"fmt"
	"os/user"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/fs"
	"github.com/pterm/pterm"
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

func RemoteCredentialsCommand() *cobra.Command {
	var commonOpts AddCredentialOpts

	cmd := &cobra.Command{
		Use:     "credential",
		Short:   "Manage credentials",
		Aliases: []string{"creds", "c"},
	}

	// Add persistent flags
	cmd.PersistentFlags().StringVarP(&commonOpts.URL, "url", "u", "", "Backend URL")
	cmd.PersistentFlags().StringVarP(&commonOpts.CredentialName, "name", "n", "", "Credential name")

	// Store subcommand constructors (not instances)
	subcommands := []func(*AddCredentialOpts) *cobra.Command{
		func(opts *AddCredentialOpts) *cobra.Command {
			return AddBasicCredentialCommand(opts)
		},
		func(opts *AddCredentialOpts) *cobra.Command {
			return AddSShCredentialCommand(opts)
		},
	}

	basicCommands := []func() *cobra.Command{
		func() *cobra.Command {
			return ListCredentialCommand()
		},
		func() *cobra.Command {
			return DeleteCredentialCommand()
		},
	}

	// Initialize subcommands only when needed
	for _, newCmd := range subcommands {
		cmd.AddCommand(newCmd(&commonOpts))
	}

	for _, lsCmd := range basicCommands {
		cmd.AddCommand(lsCmd())
	}
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
					pterm.Error.Printfln("Failed to get current user")
					return err
				}
				pterm.Printfln("Using current user for username: %s", currentUser.Username)
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
			appConfig := GetAppConfig(cmd)
			storage, err := backend.NewTomlCredentialStorage(appConfig.CredentialsFile)
			if err != nil {
				return fmt.Errorf("failed to create credential store: path we got %s: %w", appConfig.CredentialsFile, err)
			}

			err = storage.AddCredential(credential)
			if err != nil {
				return errors.New("failed to add credential: " + err.Error())
			}

			return nil
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
					pterm.Error.Println("Failed to get current user")
					return err
				}
				pterm.Printfln("Using current user for username: %s", currentUser.Username)
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
			appConfig := GetAppConfig(cmd)
			storage, err := backend.NewTomlCredentialStorage(appConfig.CredentialsFile)
			if err != nil {
				return fmt.Errorf("failed to create credential store: path we got %s: %w", appConfig.CredentialsFile, err)
			}

			err = storage.AddCredential(credential)
			if err != nil {
				return errors.New("Failed to add credential: " + err.Error())
			}
			return nil
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
			appConfig := GetAppConfig(cmd)
			storage, err := backend.NewTomlCredentialStorage(appConfig.CredentialsFile)
			if err != nil {
				return fmt.Errorf("failed to create credential store: path we got %s: %w", appConfig.CredentialsFile, err)
			}

			credList, err := storage.ListCredentials()
			if err != nil {
				return errors.New("Failed to list the stored credentials: " + err.Error())
			}
			if len(credList) == 0 {
				pterm.Print("No credentials added.")
			}
			VisualizeCredentialList(credList)
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
			appConfig := GetAppConfig(cmd)
			storage, err := backend.NewTomlCredentialStorage(appConfig.CredentialsFile)
			if err != nil {
				return fmt.Errorf("failed to create credential store path: we got %s: %w", appConfig.CredentialsFile, err)
			}
			if deleteCredOpts.CredentialName == "" && deleteCredOpts.CredentialUUID == "" {
				return errors.New("must pass in either the credential name or the credential uuid")
			}

			if deleteCredOpts.CredentialName != "" {
				return storage.DeleteCredentialByName(deleteCredOpts.CredentialName)
			}

			if deleteCredOpts.CredentialUUID != "" {
				credUuid, err := uuid.Parse(deleteCredOpts.CredentialUUID)
				if err != nil {
					return errors.New("the credential uuid is not valid: " + err.Error())
				}
				return storage.DeleteCredential(credUuid)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&deleteCredOpts.CredentialName, "name", "", "The name of the stored credential")
	cmd.Flags().StringVar(&deleteCredOpts.CredentialUUID, "uuid", "", "The uuid assigned to the credential")

	return cmd

}

func VisualizeCredentialList(credList []fs.Credential) {
	for _, cred := range credList {
		pterm.Printfln("[%s]", cred.GetName())
		pterm.Printfln("type = %s", cred.GetType())
		pterm.Printfln("url = %s", cred.GetUrl())
		pterm.Printfln("uuid = %s", cred.GetUUID())
		switch c := cred.(type) {
		case *backend.SSHCredential:
			pterm.Printfln("username = %s", c.Username)
			pterm.Printfln("private-key-path = %s", c.PrivateKeyPath)
			pterm.Printfln("public-key-path = %s", c.PublicKeyPath)
			pterm.Printfln("ssh-agent = %t", c.UseAgent)
		case *backend.BasicAuthCredential:
			pterm.Printfln("username = %s", c.Username)
			pterm.Printfln("password = %s", c.Password)
			pterm.Println() // Empty line between credentials
		}
		pterm.Println("")
	}
}
