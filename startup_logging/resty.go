package sl

import (
	"fmt"
	"log/slog"
)

// RestyAdapter bridges resty.Logger to *slog.Logger.
type RestyAdapter struct {
	Logger *slog.Logger
}

func (a RestyAdapter) Errorf(format string, v ...any) {
	a.Logger.Error(fmt.Sprintf(format, v...))
}

func (a RestyAdapter) Warnf(format string, v ...any) {
	a.Logger.Warn(fmt.Sprintf(format, v...))
}

func (a RestyAdapter) Debugf(format string, v ...any) {
	a.Logger.Debug(fmt.Sprintf(format, v...))
}
