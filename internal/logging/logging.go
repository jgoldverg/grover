// Package logging configures the process-wide slog logger used by grover and groverd.
//
// Everything in the repo logs through log/slog. Use key/value pairs with the
// common keys below so logs can be grepped across nodes:
//
//	slog.Info("transfer started", "job_id", id, "route_id", route)
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Setup installs a default slog logger. level is debug|info|warn|error and
// format is text|json. Debug level also records the source file and line.
func Setup(level, format string, w io.Writer) error {
	if w == nil {
		w = os.Stderr
	}
	lvl, err := ParseLevel(level)
	opts := &slog.HandlerOptions{Level: lvl, AddSource: lvl <= slog.LevelDebug}
	var h slog.Handler
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json":
		h = slog.NewJSONHandler(w, opts)
	default:
		h = slog.NewTextHandler(w, opts)
	}
	slog.SetDefault(slog.New(h))
	return err
}

// ParseLevel maps a level name to a slog level. Unknown names return info and an error.
func ParseLevel(level string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug", "trace":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unknown log level %q", level)
	}
}
