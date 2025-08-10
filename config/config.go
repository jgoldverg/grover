package config

import (
	"os"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/viper"
)

var LogLevel = func() pterm.LogLevel {
	switch os.Getenv("GOROVER_LOG_LEVEL") {
	case "debug":
		return pterm.LogLevelDebug
	case "warn":
		return pterm.LogLevelWarn
	default:
		return pterm.LogLevelInfo

	}
}

var Log = pterm.DefaultLogger.
	WithLevel(LogLevel()).
	WithFormatter(pterm.LogFormatterColorful)

type AutoConfig struct {
	TomlFilePath string
}

func NewAutoConfig() *AutoConfig {
	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	ac := AutoConfig{
		TomlFilePath: ".gorover_credentials.toml",
	}
	Log.Debug("Credential file path being used: " + ac.TomlFilePath)
	return &ac
}
