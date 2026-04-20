package sl

import (
	"context"
	"log/slog"
)

type loggerKey struct{}

// WithLogger stores the entry into the context so it can be retrieved with LoggerOf later.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

func LoggerOf(ctx context.Context) *slog.Logger {
	return loggerOf(ctx)
}

// ContextLoggerWithFields gets a logger with some fields pre-applied.
// deprecated, use `WithLogger(LoggerOf(ctx).With(fields...))`
func ContextLoggerWithFields(ctx context.Context, fields ...any) context.Context {
	logger := LoggerOf(ctx).With(fields...)
	return WithLogger(ctx, logger)
}

// GetLoggerWithFields gets a logger with some fields pre-applied.
// deprecated, use `LoggerOf(ctx).With(fields...)`
func GetLoggerWithFields(ctx context.Context, fields ...any) *slog.Logger {
	return LoggerOf(ctx).With(fields...)
}

func loggerOf(ctx context.Context) *slog.Logger {
	if ctx == nil {
		return slog.Default()
	}

	logger := ctx.Value(loggerKey{})
	if logger == nil {
		return slog.Default()
	}

	return logger.(*slog.Logger)
}
