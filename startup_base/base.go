package startup_base

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"path"
)

var BuildPackage string
var BuildGitHash string
var BuildVersion string
var BuildUnixTimestamp string

type BaseOptions struct {
	Logfile       string `long:"log-file" description:"Write logs to a different file. Defaults to stdout."`
	ForceColor    bool   `long:"log-color" description:"Forces colored output even on non TTYs."`
	JSONFormatter bool   `long:"log-json" description:"Log using the logrus json formatter."`

	Verbose bool `long:"verbose" description:"Show verbose logging output."`
	Version bool `long:"version" description:"Prints the build information about this application if available."`
}

func (opts *BaseOptions) Initialize() {
	if opts.Version {
		fmt.Printf("%s (%s)\n", path.Base(os.Args[0]), BuildPackage)
		fmt.Printf("  version: %s\n", BuildVersion)
		fmt.Printf("  git hash: %s\n", BuildGitHash)
		fmt.Printf("  build time: %s\n", BuildUnixTimestamp)
		os.Exit(0)
	}

	if opts.JSONFormatter {
		logrus.SetFormatter(&logrus.JSONFormatter{})

	} else {
		logrus.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:          true,
			DisableSorting:         false,
			DisableLevelTruncation: true,
			PadLevelText:           true,
			ForceColors:            opts.ForceColor,
		})
	}

	fp, err := OpenWriter(opts.Logfile)
	PanicOnError(err, "Cannot open log file")

	logrus.SetOutput(fp)

	if opts.Verbose {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.WithField("prefix", "main").Debug("Enabled verbose logging")
	}
}
