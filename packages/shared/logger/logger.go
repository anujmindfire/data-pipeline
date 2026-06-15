package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type ctxKey string

const idKey ctxKey = "correlation_id"

var base *slog.Logger

func init() {
	level := slog.LevelInfo
	if l := os.Getenv("LOG_LEVEL"); l != "" {
		switch strings.ToUpper(l) {
		case "DEBUG":
			level = slog.LevelDebug
		case "WARN", "WARNING":
			level = slog.LevelWarn
		case "ERROR":
			level = slog.LevelError
		}
	}
	base = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

// WithCorrelationID returns a copy of ctx carrying the given correlation ID.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, idKey, id)
}

// FromContext extracts the correlation ID stored by WithCorrelationID.
func FromContext(ctx context.Context) string {
	id, _ := ctx.Value(idKey).(string)
	return id
}

func forCtx(ctx context.Context) *slog.Logger {
	if id := FromContext(ctx); id != "" {
		return base.With("correlation_id", id)
	}
	return base
}

// Info logs at INFO level, including the correlation ID from ctx when present.
func Info(ctx context.Context, msg string, args ...any) {
	forCtx(ctx).Info(msg, args...)
}

// Warn logs at WARN level, including the correlation ID from ctx when present.
func Warn(ctx context.Context, msg string, args ...any) {
	forCtx(ctx).Warn(msg, args...)
}

// Error logs at ERROR level, including the correlation ID from ctx when present.
func Error(ctx context.Context, msg string, args ...any) {
	forCtx(ctx).Error(msg, args...)
}
