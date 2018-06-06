package startup

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

func FatalOnError(err error, reason string, args ...interface{}) {
	if err != nil {
		log.Fatalf("%s: %s", fmt.Sprintf(reason, args...), err)
		return
	}
}

func OpenWriter(name string) (io.Writer, error) {
	switch name {
	case "", "/dev/stderr":
		return os.Stderr, nil

	case "/dev/stdout":
		return os.Stdout, nil

	case "/dev/null":
		return ioutil.Discard, nil

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
		panic(Errorf(msg, args...))
	}
}
