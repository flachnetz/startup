package startup_http

import (
	"fmt"
	"log/slog"
)

// SlogRecoveryHandlerLogger implements the handlers.RecoveryHandlerLogger interface
// by logging panic details at slog.LevelError in JSON format.
type SlogRecoveryHandlerLogger struct {
	logger *slog.Logger
}

// NewSlogRecoveryHandlerLogger creates and returns a new SlogRecoveryHandlerLogger.
func NewSlogRecoveryHandlerLogger() *SlogRecoveryHandlerLogger {
	return &SlogRecoveryHandlerLogger{
		logger: slog.Default(),
	}
}

// Println implements the handlers.RecoveryHandlerLogger interface.
// It receives the arguments from RecoveryHandler (typically panic_value, stack_trace_string)
// and logs them as structured JSON at slog.LevelError.
func (l *SlogRecoveryHandlerLogger) Println(v ...interface{}) {
	var panicMessage string
	var stackTrace string
	var attrs []any

	// RecoveryHandler typically calls Println with (panic_value, stack_trace_string)
	// or sometimes just (panic_value)
	if len(v) > 0 {
		panicMessage = fmt.Sprintf("%v", v[0])
		attrs = append(attrs, slog.String("panic_value", panicMessage))
	}

	if len(v) > 1 {
		stackTrace = fmt.Sprintf("%v", v[1])
		attrs = append(attrs, slog.String("stack_trace", stackTrace))
	}

	// If there are more arguments, append them as generic fields.
	// This might happen if other middleware or parts of the handler pass additional context.
	if len(v) > 2 {
		extraArgs := fmt.Sprintf("%v", v[2:])
		attrs = append(attrs, slog.String("additional_recovery_info", extraArgs))
	}

	// Log at Error level always.
	l.logger.Error(
		"PANIC RECOVERED",
		attrs..., // Unpack the attributes slice
	)
}
