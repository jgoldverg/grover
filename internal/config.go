package internal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/spf13/viper"
	"google.golang.org/grpc/credentials"
)

type AppConfig struct {
	CredentialsFile     string `mapstructure:"credentials_file"`
	ServerURL           string `mapstructure:"server_url"`
	CACertFile          string `mapstructure:"ca_cert_file"`
	Route               string `mapstructure:"route"`
	HeartBeatInterval   int    `mapstructure:"heart_beat_interval"`
	HeartBeatErrorCount int    `mapstructure:"heart_beat_error_count"`
	HeartBeatTimeout    int    `mapstructure:"heart_beat_timeout"`
	HeartBeatRtts       int    `mapstructure:"heart_beat_rtts"`
	ClientUuid          string `mapstructure:"client_uuid"`
	LogLevel            string `mapstructure:"log_level"`
}

type UdpClientConfig struct {
	AckTimeout         int  `mapstructure:"ack_timeout"`
	SocketBufferSize   int  `mapstructure:"socket_buffer_size"`
	ParallelSenders    uint `mapstructure:"parallel_senders"`
	QueueSize          int  `mapstructure:"queue_size"`
	MaxInFlightPackets int  `mapstructure:"max_in_flight_packets"`
	RateLimitMbps      int  `mapstructure:"rate_limit_mbps"`
	MaxRetries         int  `mapstructure:"max_retries"`
	EnableSack         bool `mapstructure:"enable_sack"`
	MtuSize            int  `mapstructure:"mtu_size"`
	CheckSum           bool `mapstructure:"check_sum"`
	SessionTTL         int  `mapstructure:"session_ttl"`
	SessionScan        int  `mapstructure:"scan_time"`
}

func LoadUdpClientConfig(configPath string) (*UdpClientConfig, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	v, err := initViper(configPath, filepath.Join(home, ".grover"), "udp_client_config", "toml", "GUDP_CLIENT_CONFIG")
	if err != nil {
		return nil, err
	}

	v.SetDefault("ack_timeout", 50)
	v.SetDefault("socket_buffer_size", 8<<20)
	v.SetDefault("parallel_senders", 1)
	v.SetDefault("queue_size", 65536)
	v.SetDefault("max_in_flight_packets", 4096)
	v.SetDefault("rate_limit_mbps", 0)
	v.SetDefault("max_retries", 5)
	v.SetDefault("enable_sack", true)
	v.SetDefault("mtu_size", 1500)
	v.SetDefault("check_sum", true)

	var cfg UdpClientConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if v.ConfigFileUsed() == "" {
		writePath := configPath
		if writePath == "" {
			writePath = filepath.Join(home, ".grover", "udp_client_config.toml")
		}
		if _, statErr := os.Stat(writePath); errors.Is(statErr, os.ErrNotExist) {
			if _, err := cfg.Save(writePath); err != nil {
				return nil, fmt.Errorf("persist default app config: %w", err)
			}
		}
		Info("client config written", Fields{
			ConfigPath: writePath,
		})
	}
	return &cfg, nil
}

func LoadAppConfig(configPath string) (*AppConfig, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	v, err := initViper(configPath, filepath.Join(home, ".grover"), "cli_config", "toml", "GROVER_CLI_CONFIG")
	if err != nil {
		return nil, err
	}

	// Defaults
	v.SetDefault("credentials_file", filepath.Join(home, ".grover", "credentials_store.toml"))
	v.SetDefault("server_url", "localhost:22444")
	v.SetDefault("ca_cert_file", filepath.Join(home, ".grover", "certs", "public", "server.crt"))
	v.SetDefault("route", "auto")
	v.SetDefault("heart_beat_interval", 10)
	v.SetDefault("heart_beat_error_count", 5)
	v.SetDefault("heart_beat_timeout", 30)
	v.SetDefault("heart_beat_rtts", 64)
	v.SetDefault("client_uuid", uuid.New().String())
	v.SetDefault("log_level", "info")

	var cfg AppConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	// expand paths
	cfg.CredentialsFile = expandPath(cfg.CredentialsFile)
	cfg.CACertFile = expandPath(cfg.CACertFile)

	// Create-on-first-run ONLY:
	// If Viper didn't read any file, pick a path and write it if missing.
	if v.ConfigFileUsed() == "" {
		writePath := configPath
		if writePath == "" {
			writePath = filepath.Join(home, ".grover", "cli_config.toml")
		}
		if _, statErr := os.Stat(writePath); errors.Is(statErr, os.ErrNotExist) {
			if _, err := cfg.Save(writePath); err != nil {
				return nil, fmt.Errorf("persist default app config: %w", err)
			}
		}
		Info("client config written", Fields{
			ConfigPath: writePath,
		})
	}

	// Create-on-first-run ONLY (no config file was read)
	if v.ConfigFileUsed() == "" {
		writePath := configPath
		if writePath == "" {
			writePath = filepath.Join(home, ".grover", "server_config.toml")
		}
		if _, statErr := os.Stat(writePath); errors.Is(statErr, os.ErrNotExist) {
			if _, err := cfg.Save(writePath); err != nil {
				return nil, fmt.Errorf("persist default server config: %w", err)
			}
		}
		Info("server config written", Fields{
			ConfigPath: writePath,
		})
	}

	return &cfg, nil
}

type ServerConfig struct {
	Port                  int    `mapstructure:"port"`
	ServerCertificatePath string `mapstructure:"server_certificate_path"`
	ServerKeyPath         string `mapstructure:"server_key_path"`
	CredentialsFile       string `mapstructure:"credentials_file"`
	HeartBeatInterval     int    `mapstructure:"heart_beat_interval"`
	ServerId              string `mapstructure:"server_id"`
	LogLevel              string `mapstructure:"log_level"`
	UDPReadBufferSize     int    `mapstructure:"udp_read_buffer_size"`
	UDPWriteBufferSize    int    `mapstructure:"udp_write_buffer_size"`
	UDPPacketWorkers      int    `mapstructure:"udp_packet_workers"`
	UDPReadTimeoutMs      int    `mapstructure:"udp_read_timeout_ms"`
	UDPQueueDepth         int    `mapstructure:"udp_queue_depth"`
}

func LoadServerConfig(configPath string) (*ServerConfig, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, errors.New("failed to load users home directory: " + err.Error())
	}
	v, err := initViper(configPath, filepath.Join(home, ".grover"), "server_config", "toml", "GROVER_SERVER")
	if err != nil {
		return nil, errors.New("failed to load server config: " + err.Error())
	}

	v.SetDefault("port", 22444)
	v.SetDefault("server_certificate_path", filepath.Join(home, ".grover", "certs", "public", "server.crt"))
	v.SetDefault("server_key_path", filepath.Join(home, ".grover", "certs", "private", "server.key"))
	v.SetDefault("credentials_file", filepath.Join(home, ".grover", "credentials_store.toml"))
	v.SetDefault("heart_beat_interval", 5000)
	v.SetDefault("server_id", uuid.New().String())
	v.SetDefault("log_level", "info")
	v.SetDefault("udp_read_buffer_size", 64*1024)
	v.SetDefault("udp_write_buffer_size", 64*1024)
	v.SetDefault("udp_packet_workers", 10)
	v.SetDefault("udp_read_timeout_ms", 10_000)
	v.SetDefault("udp_queue_depth", 0)

	var cfg ServerConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	cfg.ServerCertificatePath = expandPath(cfg.ServerCertificatePath)
	cfg.ServerKeyPath = expandPath(cfg.ServerKeyPath)
	cfg.CredentialsFile = expandPath(cfg.CredentialsFile)

	Info("TLS cert paths", Fields{
		ServerCertificatePath: cfg.ServerCertificatePath,
		ServerKeyPath:         cfg.ServerKeyPath,
		CredentialPath:        cfg.CredentialsFile,
	})

	// Create-on-first-run ONLY (no config file was read)
	if v.ConfigFileUsed() == "" {
		writePath := configPath
		if writePath == "" {
			writePath = filepath.Join(home, ".grover", "server_config.toml")
		}
		if _, statErr := os.Stat(writePath); errors.Is(statErr, os.ErrNotExist) {
			if _, err := cfg.Save(writePath); err != nil {
				return nil, fmt.Errorf("persist default server config: %w", err)
			}
		}
		Info("server config written", Fields{
			ConfigPath: writePath,
		})
	}

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
	v.SetEnvPrefix(envPrefix)
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
			Error("config file not found", Fields{
				ConfigPath: configPath,
			})
			return nil, fmt.Errorf("read config: %w", err)
		}
	}
	return v, nil
}

func (cfg *UdpClientConfig) Save(path string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if path == "" {
		path = filepath.Join(home, ".grover", "udp_client_config.toml")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	v := viper.New()
	v.SetConfigType("toml")
	v.SetDefault("ack_timeout", cfg.AckTimeout)
	v.SetDefault("socket_buffer_size", cfg.SocketBufferSize)
	v.SetDefault("parallel_senders", cfg.ParallelSenders)
	v.SetDefault("queue_size", cfg.QueueSize)
	v.SetDefault("max_in_flight_packets", cfg.MaxInFlightPackets)
	v.SetDefault("rate_limit_mbps", cfg.RateLimitMbps)
	v.SetDefault("max_retries", cfg.MaxRetries)
	v.SetDefault("enable_sack", cfg.EnableSack)
	v.SetDefault("mtu_size", cfg.MtuSize)
	v.SetDefault("check_sum", cfg.CheckSum)
	v.SetDefault("session_ttl", 10)
	v.SetDefault("scan_time", 10)

	if err := v.WriteConfigAs(path); err != nil {
		return "", fmt.Errorf("write udp client config: %w", err)
	}
	_ = os.Chmod(path, 0o600)
	return path, nil
}

func (cfg *AppConfig) Save(path string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if path == "" {
		path = filepath.Join(home, ".grover", "app.toml")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}

	v := viper.New()
	v.SetConfigType("toml")
	v.Set("credentials_file", cfg.CredentialsFile)
	v.Set("server_url", cfg.ServerURL)
	v.Set("ca_cert_file", cfg.CACertFile)
	v.Set("route", cfg.Route)
	v.Set("heart_beat_interval", cfg.HeartBeatInterval)
	v.Set("heart_beat_error_count", cfg.HeartBeatErrorCount)
	v.Set("heart_beat_timeout", cfg.HeartBeatTimeout)
	v.Set("heart_beat_rtts", cfg.HeartBeatRtts)
	v.Set("client_uuid", cfg.ClientUuid)
	v.Set("log_level", cfg.LogLevel)

	if err := v.WriteConfigAs(path); err != nil {
		return "", fmt.Errorf("write app config: %w", err)
	}
	_ = os.Chmod(path, 0o600)
	return path, nil
}

func (cfg *ServerConfig) Save(path string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if path == "" {
		path = filepath.Join(home, ".grover", "server_config.toml")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}

	v := viper.New()
	v.SetConfigType("toml")
	v.Set("port", cfg.Port)
	v.Set("server_certificate_path", cfg.ServerCertificatePath)
	v.Set("server_key_path", cfg.ServerKeyPath)
	v.Set("credentials_file", cfg.CredentialsFile)
	v.Set("heart_beat_interval", cfg.HeartBeatInterval)
	v.Set("server_id", cfg.ServerId)
	v.Set("log_level", cfg.LogLevel)
	v.Set("udp_read_buffer_size", cfg.UDPReadBufferSize)
	v.Set("udp_write_buffer_size", cfg.UDPWriteBufferSize)
	v.Set("udp_packet_workers", cfg.UDPPacketWorkers)
	v.Set("udp_read_timeout_ms", cfg.UDPReadTimeoutMs)
	v.Set("udp_queue_depth", cfg.UDPQueueDepth)

	if err := v.WriteConfigAs(path); err != nil {
		return "", fmt.Errorf("write server config: %w", err)
	}
	_ = os.Chmod(path, 0o600)
	return path, nil
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
