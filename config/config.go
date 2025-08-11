package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jgoldverg/grover/backend/fs"
	"github.com/spf13/viper"
)

type AppConfig struct {
	CredentialsFile string
	ServerURL       string
}

type ListerConfig struct {
	Credential fs.Credential
}

func LoadAppConfig(configPath string) (*AppConfig, error) {
	v := viper.New()

	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		// default config location
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		v.AddConfigPath(filepath.Join(home, ".grover"))
		v.SetConfigName("config")
		v.SetConfigType("toml")
	}

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Set default for credentials file path
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	v.SetDefault("credentials_file", filepath.Join(home, ".grover_credentials.toml"))

	// Read config file (if exists)
	err = v.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}
	cfg := &AppConfig{
		CredentialsFile: v.GetString("credentials_file"),
	}

	return cfg, nil
}
