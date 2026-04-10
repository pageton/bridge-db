// Package logger provides a thin wrapper around log/slog with structured
// fields relevant to the migration pipeline.
package logger

import (
	"log/slog"
	"os"
)

// Level mirrors slog levels for convenience.
type Level = slog.Level

const (
	LevelDebug = slog.LevelDebug
	LevelInfo  = slog.LevelInfo
	LevelWarn  = slog.LevelWarn
	LevelError = slog.LevelError
)

// L returns the default logger.
func L() *slog.Logger {
	return slog.Default()
}

// Init configures the default slog logger with JSON output at the given level.
func Init(level Level, json bool) {
	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	if json {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(handler))
}

// WithComponent returns a logger pre-loaded with a component field.
func WithComponent(component string) *slog.Logger {
	return L().With("component", component)
}

// WithPhase returns a logger pre-loaded with a phase field.
func WithPhase(phase string) *slog.Logger {
	return L().With("phase", phase)
}

// WithProvider returns a logger pre-loaded with a provider field.
func WithProvider(provider string) *slog.Logger {
	return L().With("provider", provider)
}
