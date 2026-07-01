package sl

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

// RestyAdapter bridges resty.Logger to *slog.Logger.
type RestyAdapter struct {
	Logger    *slog.Logger
	Multiline bool
}

func (a RestyAdapter) Errorf(format string, v ...any) {
	a.log(slog.LevelError, format, v...)
}

func (a RestyAdapter) Warnf(format string, v ...any) {
	a.log(slog.LevelWarn, format, v...)
}

func (a RestyAdapter) Debugf(format string, v ...any) {
	a.log(slog.LevelDebug, format, v...)
}

// log formats the message and may emit one log record per line. resty often logs
// multi-line request/response dumps in a single call, which we split here so
// each line becomes its own log entry.
func (a RestyAdapter) log(level slog.Level, format string, v ...any) {
	msg := fmt.Sprintf(format, v...)

	if a.Multiline {
		for line := range strings.SplitSeq(strings.TrimRight(msg, "\n"), "\n") {
			a.Logger.Log(context.Background(), level, line)
		}
	} else {
		a.Logger.Log(context.Background(), level, msg)
	}
}
