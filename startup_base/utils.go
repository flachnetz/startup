package startup_base

import (
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

var log = logrus.WithField("prefix", "startup-base")

func FatalOnError(err error, reason string, args ...interface{}) {
	if err != nil {
		log.Fatalf("%s: %s", fmt.Sprintf(reason, args...), err)
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

func Errorf(msg string, args ...interface{}) error {
	return StartupError(fmt.Errorf(msg, args...))
}

func Panicf(msg string, args ...interface{}) {
	panic(Errorf(msg, args...))
}

func PanicOnError(err error, msg string, args ...interface{}) {
	if err != nil {
		panic(Errorf("%s: %s", err, fmt.Sprintf(msg, args...)))
	}
}

func Close(closer io.Closer, onErrorMessage string) {
	if err := closer.Close(); err != nil {
		log.WithError(err).Warn(onErrorMessage)
	}
}
