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

type Fields map[FieldKey]any

type Level = pterm.LogLevel

const (
	LevelTrace Level = pterm.LogLevelTrace
	LevelDebug Level = pterm.LogLevelDebug
	LevelInfo  Level = pterm.LogLevelInfo
	LevelWarn  Level = pterm.LogLevelWarn
	LevelError Level = pterm.LogLevelError
	LevelFatal Level = pterm.LogLevelFatal
)

var (
	levelNames = map[string]Level{
		"trace":   LevelTrace,
		"debug":   LevelDebug,
		"info":    LevelInfo,
		"warn":    LevelWarn,
		"warning": LevelWarn,
		"error":   LevelError,
		"fatal":   LevelFatal,
	}

	loggerMu = sync.RWMutex{}

	baseLogger = func() *pterm.Logger {
		template := pterm.DefaultLogger.WithTime(true).
			WithTimeFormat(time.RFC3339).
			WithMaxWidth(120).
			WithCaller(false)
		return template.AppendKeyStyles(map[string]pterm.Style{
			string(FieldError): *pterm.NewStyle(pterm.FgRed, pterm.Bold),
		})
	}()

	currentLevel = LevelInfo
)

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
	loggerMu.Lock()
	defer loggerMu.Unlock()
	currentLevel = level
	baseLogger.Level = pterm.LogLevel(level)
}

func getLevel() Level {
	loggerMu.RLock()
	defer loggerMu.RUnlock()
	return currentLevel
}

func shouldLog(level Level) bool {
	return level >= getLevel()
}

func log(level Level, msg string, fields Fields) {
	if !shouldLog(level) {
		return
	}

	loggerMu.RLock()
	logger := baseLogger.WithLevel(pterm.LogLevel(currentLevel))
	loggerMu.RUnlock()

	args := makeLoggerArgs(fields)

	switch level {
	case LevelTrace:
		logger.Trace(msg, args)
	case LevelDebug:
		logger.Debug(msg, args)
	case LevelWarn:
		logger.Warn(msg, args)
	case LevelError:
		logger.Error(msg, args)
	case LevelFatal:
		logger.Fatal(msg, args)
	default:
		logger.Info(msg, args)
	}
}

func makeLoggerArgs(fields Fields) []pterm.LoggerArgument {
	if len(fields) == 0 {
		return nil
	}

	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)

	args := make([]pterm.LoggerArgument, 0, len(keys))
	for _, key := range keys {
		args = append(args, pterm.LoggerArgument{Key: key, Value: fields[FieldKey(key)]})
	}
	return args
}

func Debug(msg string, fields Fields) { log(LevelDebug, msg, fields) }
func Info(msg string, fields Fields)  { log(LevelInfo, msg, fields) }
func Warn(msg string, fields Fields)  { log(LevelWarn, msg, fields) }
func Error(msg string, fields Fields) { log(LevelError, msg, fields) }
