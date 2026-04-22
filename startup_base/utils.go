package startup_base

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	sl "github.com/flachnetz/startup/v2/startup_logging"
)

var log = slog.With(slog.String("prefix", "startup-base"))

func FatalOnError(err error, reason string, args ...any) {
	if err != nil {
		log.Error("%s: %s", fmt.Sprintf(reason, args...), err)
		os.Exit(1)
		return
	}
}

func OpenWriter(name string) (*os.File, error) {
	switch name {
	case "", "/dev/stderr":
		return os.Stderr, nil

	case "/dev/stdout", "-":
		return os.Stdout, nil

	case "/dev/null":
		return nil, nil

	default:
		// some output file
		return os.Create(name)
	}
}

type StartupError error

func Errorf(msg string, args ...any) error {
	return StartupError(fmt.Errorf(msg, args...))
}

func Panicf(msg string, args ...any) {
	panic(Errorf(msg, args...))
}

func PanicOnError(err error, msg string, args ...any) {
	if err != nil {
		panic(Errorf("%s: %s", err, fmt.Sprintf(msg, args...)))
	}
}

func Close(closer io.Closer, onErrorMessage string) {
	if err := closer.Close(); err != nil {
		log.Warn(onErrorMessage, sl.Error(err))
	}
}
