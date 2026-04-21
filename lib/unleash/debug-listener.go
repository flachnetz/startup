package unleash

import (
	"log/slog"

	"github.com/Unleash/unleash-go-sdk/v5"
)

var logger = slog.With(slog.String("prefix", "unleash"))

// DebugListener is an implementation of all the listener interfaces that simply logs
// debug info. It is meant for debugging purposes and an example of implementing
// the listener interfaces.
type DebugListener struct {
	Verbose bool
}

// OnError prints out errors.
func (l DebugListener) OnError(err error) {
	logger.Error("unleash error", slog.String("error", err.Error()))
}

// OnWarning prints out warning.
func (l DebugListener) OnWarning(warning error) {
	logger.Warn("unleash warning", slog.String("error", warning.Error()))
}

// OnReady prints to the console when the repository is ready.
func (l DebugListener) OnReady() {
	logger.Info("READY")
}

// OnCount prints to the console when the feature is queried.
func (l DebugListener) OnCount(name string, enabled bool) {
	if l.Verbose {
		logger.Info("Counted feature", slog.String("name", name), slog.Bool("enabled", enabled))
	}
}

// OnSent prints to the console when the server has uploaded metrics.
func (l DebugListener) OnSent(payload unleash.MetricsData) {
	if l.Verbose {
		logger.Info("Sent", slog.Any("payload", payload))
	}
}

// OnRegistered prints to the console when the client has registered.
func (l DebugListener) OnRegistered(payload unleash.ClientData) {
	logger.Info("Registered", slog.Any("payload", payload))
}
