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
	CredentialsFile     string                `mapstructure:"credentials_file"`
	ServerURL           string                `mapstructure:"server_url"`
	CACertFile          string                `mapstructure:"ca_cert_file"`
	TransferProtocol    string                `mapstructure:"transfer_protocol"`
	InsecureControl     bool                  `mapstructure:"insecure_control"`
	ActiveProfile       string                `mapstructure:"active_profile"`
	Profiles            map[string]AppProfile `mapstructure:"profiles"`
	Execution           string                `mapstructure:"execution"`
	Route               string                `mapstructure:"route"`
	HeartBeatInterval   int                   `mapstructure:"heart_beat_interval"`
	HeartBeatErrorCount int                   `mapstructure:"heart_beat_error_count"`
	HeartBeatTimeout    int                   `mapstructure:"heart_beat_timeout"`
	HeartBeatRtts       int                   `mapstructure:"heart_beat_rtts"`
	ClientUuid          string                `mapstructure:"client_uuid"`
	LogLevel            string                `mapstructure:"log_level"`
}

type AppProfile struct {
	ServerURL       string `mapstructure:"server_url"`
	CACertFile      string `mapstructure:"ca_cert_file"`
	InsecureControl bool   `mapstructure:"insecure_control"`
}

func (cfg *AppConfig) ApplyProfile(name string) error {
	if cfg == nil {
		return fmt.Errorf("app config unavailable")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	profile, ok := cfg.Profiles[name]
	if !ok {
		return fmt.Errorf("profile %q not found", name)
	}
	if strings.TrimSpace(profile.ServerURL) != "" {
		cfg.ServerURL = profile.ServerURL
	}
	if strings.TrimSpace(profile.CACertFile) != "" {
		cfg.CACertFile = expandPath(profile.CACertFile)
	}
	cfg.InsecureControl = profile.InsecureControl
	return nil
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
	v.SetDefault("execution", "auto")
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
	if cfg.Profiles == nil {
		cfg.Profiles = map[string]AppProfile{}
	}
	for name, profile := range cfg.Profiles {
		profile.CACertFile = expandPath(profile.CACertFile)
		cfg.Profiles[name] = profile
	}
	cfg.TransferProtocol = normalizeTransferProtocol(cfg.TransferProtocol)
	if !v.InConfig("execution") && strings.TrimSpace(cfg.Route) != "" {
		cfg.Execution = cfg.Route
	}
	if strings.TrimSpace(cfg.Execution) == "" {
		cfg.Execution = "auto"
	}

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
	DataBindHost          string `mapstructure:"data_bind_host"`
	DataAdvertiseHost     string `mapstructure:"data_advertise_host"`
	DataPortMin           int    `mapstructure:"data_port_min"`
	DataPortMax           int    `mapstructure:"data_port_max"`
	RouteStoreFile        string `mapstructure:"route_store_file"`
	JobLogDir             string `mapstructure:"job_log_dir"`
	EnergyMonitor         bool   `mapstructure:"energy_monitor"`
	EnergySampleMs        int    `mapstructure:"energy_sample_ms"`
	UDPReadBufferSize     int    `mapstructure:"udp_read_buffer_size"`
	UDPWriteBufferSize    int    `mapstructure:"udp_write_buffer_size"`
	UDPMTUSize            int    `mapstructure:"udp_mtu_size"`
	UDPFlowControl        string `mapstructure:"udp_flow_control"`
	UDPWindowPackets      int    `mapstructure:"udp_window_packets"`
	UDPAckEveryPackets    int    `mapstructure:"udp_ack_every_packets"`
	UDPAckEveryMs         int    `mapstructure:"udp_ack_every_ms"`
	UDPBatchPackets       int    `mapstructure:"udp_batch_packets"`
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
	v.SetDefault("data_bind_host", "0.0.0.0")
	v.SetDefault("data_advertise_host", "127.0.0.1")
	v.SetDefault("data_port_min", 0)
	v.SetDefault("data_port_max", 0)
	v.SetDefault("route_store_file", filepath.Join(home, ".grover", "routes.json"))
	v.SetDefault("job_log_dir", "/var/log/grover")
	v.SetDefault("energy_monitor", false)
	v.SetDefault("energy_sample_ms", 1000)
	v.SetDefault("udp_read_buffer_size", 8<<20)
	v.SetDefault("udp_write_buffer_size", 8<<20)
	v.SetDefault("udp_mtu_size", 1500)
	v.SetDefault("udp_flow_control", "fixed")
	v.SetDefault("udp_window_packets", 4096)
	v.SetDefault("udp_ack_every_packets", 32)
	v.SetDefault("udp_ack_every_ms", 5)
	v.SetDefault("udp_batch_packets", 64)
	v.SetDefault("udp_packet_workers", 10)
	v.SetDefault("udp_read_timeout_ms", 10_000)
	v.SetDefault("udp_queue_depth", 0)

	var cfg ServerConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if err := ValidateDataPortRange(cfg.DataPortMin, cfg.DataPortMax); err != nil {
		return nil, err
	}

	cfg.ServerCertificatePath = expandPath(cfg.ServerCertificatePath)
	cfg.ServerKeyPath = expandPath(cfg.ServerKeyPath)
	cfg.CredentialsFile = expandPath(cfg.CredentialsFile)
	cfg.RouteStoreFile = expandPath(cfg.RouteStoreFile)
	cfg.JobLogDir = expandPath(cfg.JobLogDir)
	cfg.TransferProtocol = normalizeTransferProtocol(cfg.TransferProtocol)
	cfg.UDPFlowControl = normalizeUDPFlowControl(cfg.UDPFlowControl)

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
	v.Set("active_profile", cfg.ActiveProfile)
	v.Set("profiles", appProfilesForSave(cfg.Profiles))
	v.Set("execution", cfg.Execution)
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

func appProfilesForSave(profiles map[string]AppProfile) map[string]map[string]any {
	out := make(map[string]map[string]any, len(profiles))
	for name, profile := range profiles {
		out[name] = map[string]any{
			"server_url":       profile.ServerURL,
			"ca_cert_file":     profile.CACertFile,
			"insecure_control": profile.InsecureControl,
		}
	}
	return out
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
	v.Set("data_bind_host", cfg.DataBindHost)
	v.Set("data_advertise_host", cfg.DataAdvertiseHost)
	v.Set("data_port_min", cfg.DataPortMin)
	v.Set("data_port_max", cfg.DataPortMax)
	v.Set("route_store_file", cfg.RouteStoreFile)
	v.Set("job_log_dir", cfg.JobLogDir)
	v.Set("energy_monitor", cfg.EnergyMonitor)
	v.Set("energy_sample_ms", cfg.EnergySampleMs)
	v.Set("udp_read_buffer_size", cfg.UDPReadBufferSize)
	v.Set("udp_write_buffer_size", cfg.UDPWriteBufferSize)
	v.Set("udp_mtu_size", cfg.UDPMTUSize)
	v.Set("udp_flow_control", cfg.UDPFlowControl)
	v.Set("udp_window_packets", cfg.UDPWindowPackets)
	v.Set("udp_ack_every_packets", cfg.UDPAckEveryPackets)
	v.Set("udp_ack_every_ms", cfg.UDPAckEveryMs)
	v.Set("udp_batch_packets", cfg.UDPBatchPackets)
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

func normalizeUDPFlowControl(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "bbr":
		return "bbr"
	default:
		return "fixed"
	}
}

func ValidateDataPortRange(minPort, maxPort int) error {
	if minPort == 0 && maxPort == 0 {
		return nil
	}
	if minPort <= 0 || maxPort <= 0 {
		return fmt.Errorf("data port range must set both min and max, got %d-%d", minPort, maxPort)
	}
	if minPort > maxPort {
		return fmt.Errorf("data port min %d exceeds max %d", minPort, maxPort)
	}
	if minPort > 65535 || maxPort > 65535 {
		return fmt.Errorf("data port range %d-%d exceeds 65535", minPort, maxPort)
	}
	return nil
}
