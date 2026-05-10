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
	TransferProtocol    string `mapstructure:"transfer_protocol"`
	InsecureControl     bool   `mapstructure:"insecure_control"`
	Route               string `mapstructure:"route"`
	HeartBeatInterval   int    `mapstructure:"heart_beat_interval"`
	HeartBeatErrorCount int    `mapstructure:"heart_beat_error_count"`
	HeartBeatTimeout    int    `mapstructure:"heart_beat_timeout"`
	HeartBeatRtts       int    `mapstructure:"heart_beat_rtts"`
	ClientUuid          string `mapstructure:"client_uuid"`
	LogLevel            string `mapstructure:"log_level"`
}

type UdpClientConfig struct {
	AckTimeout         int    `mapstructure:"ack_timeout"`
	SocketBufferSize   int    `mapstructure:"socket_buffer_size"`
	ParallelSenders    uint   `mapstructure:"parallel_senders"`
	ParallelStreams    uint   `mapstructure:"parallel_streams"`
	QueueSize          int    `mapstructure:"queue_size"`
	MaxInFlightPackets int    `mapstructure:"max_in_flight_packets"`
	RateLimitMbps      int    `mapstructure:"rate_limit_mbps"`
	LinkBandwidthMbps  int    `mapstructure:"link_bandwidth_mbps"`
	TargetLossPercent  int    `mapstructure:"target_loss_percent"`
	MaxRetries         int    `mapstructure:"max_retries"`
	EnableSack         bool   `mapstructure:"enable_sack"`
	MtuSize            int    `mapstructure:"mtu_size"`
	FlowControl        string `mapstructure:"flow_control"`
	WindowPackets      int    `mapstructure:"window_packets"`
	WindowBytes        int    `mapstructure:"window_bytes"`
	AckEveryPackets    int    `mapstructure:"ack_every_packets"`
	AckEveryMs         int    `mapstructure:"ack_every_ms"`
	CheckSum           bool   `mapstructure:"check_sum"`
	SessionTTL         int    `mapstructure:"session_ttl"`
	SessionScan        int    `mapstructure:"scan_time"`
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
	v.SetDefault("parallel_streams", 1)
	v.SetDefault("queue_size", 65536)
	v.SetDefault("max_in_flight_packets", 4096)
	v.SetDefault("rate_limit_mbps", 0)
	v.SetDefault("link_bandwidth_mbps", 0)
	v.SetDefault("target_loss_percent", 1)
	v.SetDefault("max_retries", 5)
	v.SetDefault("enable_sack", true)
	v.SetDefault("mtu_size", 1500)
	v.SetDefault("flow_control", "fixed")
	v.SetDefault("window_packets", 4096)
	v.SetDefault("window_bytes", 0)
	v.SetDefault("ack_every_packets", 32)
	v.SetDefault("ack_every_ms", 5)
	v.SetDefault("check_sum", true)

	var cfg UdpClientConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if cfg.ParallelStreams == 0 {
		cfg.ParallelStreams = cfg.ParallelSenders
	}
	if cfg.ParallelStreams == 0 {
		cfg.ParallelStreams = 1
	}
	cfg.FlowControl = strings.ToLower(strings.TrimSpace(cfg.FlowControl))
	if cfg.FlowControl == "" {
		cfg.FlowControl = "fixed"
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
	v.SetDefault("transfer_protocol", "udp")
	v.SetDefault("insecure_control", false)
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
	cfg.TransferProtocol = normalizeTransferProtocol(cfg.TransferProtocol)

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
	TransferProtocol      string `mapstructure:"transfer_protocol"`
	InsecureControl       bool   `mapstructure:"insecure_control"`
	HeartBeatInterval     int    `mapstructure:"heart_beat_interval"`
	ServerId              string `mapstructure:"server_id"`
	LogLevel              string `mapstructure:"log_level"`
	UDPReadBufferSize     int    `mapstructure:"udp_read_buffer_size"`
	UDPWriteBufferSize    int    `mapstructure:"udp_write_buffer_size"`
	UDPMTUSize            int    `mapstructure:"udp_mtu_size"`
	UDPWindowPackets      int    `mapstructure:"udp_window_packets"`
	UDPAckEveryPackets    int    `mapstructure:"udp_ack_every_packets"`
	UDPAckEveryMs         int    `mapstructure:"udp_ack_every_ms"`
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
	v.SetDefault("server_certificate_path", filepath.Join(home, ".grover", "certs", "server.crt"))
	v.SetDefault("server_key_path", filepath.Join(home, ".grover", "certs", "server.key"))
	v.SetDefault("credentials_file", filepath.Join(home, ".grover", "credentials_store.toml"))
	v.SetDefault("transfer_protocol", "udp")
	v.SetDefault("insecure_control", false)
	v.SetDefault("heart_beat_interval", 5000)
	v.SetDefault("server_id", uuid.New().String())
	v.SetDefault("log_level", "info")
	v.SetDefault("udp_read_buffer_size", 8<<20)
	v.SetDefault("udp_write_buffer_size", 8<<20)
	v.SetDefault("udp_mtu_size", 1500)
	v.SetDefault("udp_window_packets", 4096)
	v.SetDefault("udp_ack_every_packets", 32)
	v.SetDefault("udp_ack_every_ms", 5)
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
	cfg.TransferProtocol = normalizeTransferProtocol(cfg.TransferProtocol)

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
	v.SetDefault("parallel_streams", cfg.ParallelStreams)
	v.SetDefault("queue_size", cfg.QueueSize)
	v.SetDefault("max_in_flight_packets", cfg.MaxInFlightPackets)
	v.SetDefault("rate_limit_mbps", cfg.RateLimitMbps)
	v.SetDefault("link_bandwidth_mbps", cfg.LinkBandwidthMbps)
	v.SetDefault("target_loss_percent", cfg.TargetLossPercent)
	v.SetDefault("max_retries", cfg.MaxRetries)
	v.SetDefault("enable_sack", cfg.EnableSack)
	v.SetDefault("mtu_size", cfg.MtuSize)
	v.SetDefault("flow_control", cfg.FlowControl)
	v.SetDefault("window_packets", cfg.WindowPackets)
	v.SetDefault("window_bytes", cfg.WindowBytes)
	v.SetDefault("ack_every_packets", cfg.AckEveryPackets)
	v.SetDefault("ack_every_ms", cfg.AckEveryMs)
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
	v.Set("transfer_protocol", cfg.TransferProtocol)
	v.Set("insecure_control", cfg.InsecureControl)
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
	v.Set("transfer_protocol", cfg.TransferProtocol)
	v.Set("insecure_control", cfg.InsecureControl)
	v.Set("heart_beat_interval", cfg.HeartBeatInterval)
	v.Set("server_id", cfg.ServerId)
	v.Set("log_level", cfg.LogLevel)
	v.Set("udp_read_buffer_size", cfg.UDPReadBufferSize)
	v.Set("udp_write_buffer_size", cfg.UDPWriteBufferSize)
	v.Set("udp_mtu_size", cfg.UDPMTUSize)
	v.Set("udp_window_packets", cfg.UDPWindowPackets)
	v.Set("udp_ack_every_packets", cfg.UDPAckEveryPackets)
	v.Set("udp_ack_every_ms", cfg.UDPAckEveryMs)
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

func normalizeTransferProtocol(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "tcp":
		return "tcp"
	default:
		return "udp"
	}
}
