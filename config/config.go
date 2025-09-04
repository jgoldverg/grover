package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
	"google.golang.org/grpc/credentials"
)

type AppConfig struct {
	CredentialsFile string `mapstructure:"credentials_file"`
	ServerURL       string `mapstructure:"server_url"`
}

type ServerConfig struct {
	Port                  int    `mapstructure:"port"`
	ServerCertificatePath string `mapstructure:"server_certificate_path"`
	ServerKeyPath         string `mapstructure:"server_key_path"`
	CredentialsFile       string `mapstructure:"credentials_file"`
}

func LoadAppConfig(configPath string) (*AppConfig, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	v, err := initViper(configPath, filepath.Join(home, ".grover"), "app", "toml", "GROVER_APP")
	if err != nil {
		return nil, err
	}

	v.SetDefault("credentials_file", filepath.Join(home, ".grover", "credentials_store.toml"))
	v.SetDefault("server_url", "")

	var cfg AppConfig
	if err := readInto(v, &cfg); err != nil {
		return nil, err
	}

	cfg.CredentialsFile = expandPath(cfg.CredentialsFile)

	return &cfg, nil
}

func LoadServerConfig(configPath string) (*ServerConfig, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	v, err := initViper(configPath, filepath.Join(home, ".grover"), "server", "toml", "GROVER_SERVER")
	if err != nil {
		return nil, err
	}

	// Defaults
	v.SetDefault("port", 22444)
	v.SetDefault("server_certificate_path", filepath.Join(home, ".grover", "certs", "public", "server.crt"))
	v.SetDefault("server_key_path", filepath.Join(home, ".grover", "certs", "private", "server.key"))
	v.SetDefault("credentials_file", filepath.Join(home, ".grover", "credentials_store.toml"))

	var cfg ServerConfig
	if err := readInto(v, &cfg); err != nil {
		return nil, err
	}

	cfg.ServerCertificatePath = expandPath(cfg.ServerCertificatePath)
	cfg.ServerKeyPath = expandPath(cfg.ServerKeyPath)
	cfg.CredentialsFile = expandPath(cfg.CredentialsFile)

	return &cfg, nil
}

func (cfg *ServerConfig) LoadTLSCredentials() (credentials.TransportCredentials, error) {
	cert := expandPath(cfg.ServerCertificatePath)
	key := expandPath(cfg.ServerKeyPath)
	return credentials.NewServerTLSFromFile(cert, key)
}

func initViper(configPath, defaultDir, defaultName, defaultType, envPrefix string) (*viper.Viper, error) {
	v := viper.New()
	v.SetConfigType(defaultType)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.SetEnvPrefix(envPrefix) // e.g. GROVER_APP, GROVER_SERVER
	v.AutomaticEnv()

	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.AddConfigPath(defaultDir)
		v.AddConfigPath(".")
		v.SetConfigName(defaultName)
	}

	if err := v.ReadInConfig(); err != nil {
		_, notFound := err.(viper.ConfigFileNotFoundError)
		if !notFound {
			return nil, fmt.Errorf("read config: %w", err)
		}
	}
	return v, nil
}

func readInto(v *viper.Viper, out any) error {
	if err := v.Unmarshal(out); err != nil {
		return fmt.Errorf("unmarshal config: %w", err)
	}
	return nil
}

func expandPath(p string) string {
	if p == "" {
		return p
	}
	p = os.ExpandEnv(p)
	if strings.HasPrefix(p, "~") {
		if home, err := os.UserHomeDir(); err == nil {
			p = filepath.Join(home, strings.TrimPrefix(p, "~"))
		}
	}
	return p
}
