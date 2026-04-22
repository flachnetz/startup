package kafka

import (
	"fmt"
	"log/slog"
	"strings"
)

//lint:ignore U1000
type slogAdapter struct {
	delegate *slog.Logger
}

func (l slogAdapter) Print(v ...any) {
	l.delegate.Info(strings.TrimSpace(fmt.Sprint(v...)))
}

func (l slogAdapter) Printf(format string, v ...any) {
	l.delegate.Info(strings.TrimSpace(fmt.Sprintf(format, v...)))
}

func (l slogAdapter) Println(v ...any) {
	l.delegate.Info(strings.TrimSpace(fmt.Sprint(v...)))
}
