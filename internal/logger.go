package internal

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pterm/pterm"
)

type FieldKey string

const (
	FieldError            FieldKey = "error"
	FieldMsg              FieldKey = "message"
	FieldRtt              FieldKey = "rtt"
	FieldServer           FieldKey = "server"
	FieldPort             FieldKey = "port"
	ServerCertificatePath FieldKey = "certificate_path"
	ServerKeyPath         FieldKey = "key_path"
	CredentialPath        FieldKey = "credential_path"
	ConfigPath            FieldKey = "config_path"
)

type Fields map[FieldKey]interface{}

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

var (
	levelNames = map[string]Level{
		"debug":   LevelDebug,
		"info":    LevelInfo,
		"warn":    LevelWarn,
		"warning": LevelWarn,
		"error":   LevelError,
	}

	levelPrinters = map[Level]*pterm.PrefixPrinter{
		LevelDebug: pterm.Debug.WithPrefix(pterm.Prefix{Text: "DEBUG", Style: pterm.NewStyle(pterm.FgLightBlue)}).
			WithMessageStyle(pterm.NewStyle(pterm.FgLightBlue)),
		LevelInfo: pterm.Info.WithPrefix(pterm.Prefix{Text: "INFO", Style: pterm.NewStyle(pterm.FgGreen)}).
			WithMessageStyle(pterm.NewStyle(pterm.FgLightWhite)),
		LevelWarn: pterm.Warning.WithPrefix(pterm.Prefix{Text: "WARN", Style: pterm.NewStyle(pterm.FgLightYellow)}).
			WithMessageStyle(pterm.NewStyle(pterm.FgLightYellow)),
		LevelError: pterm.Error.WithPrefix(pterm.Prefix{Text: "ERROR", Style: pterm.NewStyle(pterm.FgLightRed)}).
			WithMessageStyle(pterm.NewStyle(pterm.FgLightRed)),
	}

	logLevelMu   sync.RWMutex
	currentLevel = LevelInfo
)

// ConfigureLogger sets the global log level based on the provided string.
// An invalid value falls back to info and returns an error so callers can warn.
func ConfigureLogger(level string) error {
	level = strings.TrimSpace(strings.ToLower(level))
	if level == "" {
		SetLogLevel(LevelInfo)
		return nil
	}
	lvl, ok := levelNames[level]
	if !ok {
		SetLogLevel(LevelInfo)
		return fmt.Errorf("unknown log level %q", level)
	}
	SetLogLevel(lvl)
	return nil
}

func SetLogLevel(level Level) {
	logLevelMu.Lock()
	defer logLevelMu.Unlock()
	currentLevel = level
}

func getLogLevel() Level {
	logLevelMu.RLock()
	defer logLevelMu.RUnlock()
	return currentLevel
}

func shouldLog(level Level) bool {
	return level >= getLogLevel()
}

func log(level Level, msg string, fields Fields) {
	if !shouldLog(level) {
		return
	}
	if fields == nil {
		fields = Fields{}
	}
	printer, ok := levelPrinters[level]
	if !ok {
		printer = levelPrinters[LevelInfo]
	}
	timestamp := time.Now().Format(time.RFC3339)
	fieldStr := formatFields(fields)
	if fieldStr != "" {
		printer.Printfln("[%s] %s %s", timestamp, msg, fieldStr)
	} else {
		printer.Printfln("[%s] %s", timestamp, msg)
	}
}

func formatFields(fields Fields) string {
	if len(fields) == 0 {
		return ""
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, fields[FieldKey(k)]))
	}
	return strings.Join(parts, " ")
}

func Debug(msg string, fields Fields) { log(LevelDebug, msg, fields) }
func Info(msg string, fields Fields)  { log(LevelInfo, msg, fields) }
func Warn(msg string, fields Fields)  { log(LevelWarn, msg, fields) }
func Error(msg string, fields Fields) { log(LevelError, msg, fields) }

// Structured is kept for backward compatibility with existing call sites that still
// pass explicit pterm printers. New code should use Debug/Info/Warn/Error directly.
func Structured(printer *pterm.PrefixPrinter, msg string, fields Fields) {
	level := LevelInfo
	switch printer {
	case &pterm.Debug:
		level = LevelDebug
	case &pterm.Error, &pterm.Fatal:
		level = LevelError
	case &pterm.Warning:
		level = LevelWarn
	default:
		level = LevelInfo
	}
	log(level, msg, fields)
}
