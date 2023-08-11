package startup_base

import (
	"context"
	"fmt"
	"github.com/flachnetz/startup/v2/startup_base/tint"
	"github.com/mattn/go-isatty"
	"log/slog"
	"os"
	"path"
)

var (
	BuildPackage       string
	BuildGitHash       string
	BuildVersion       string
	BuildUnixTimestamp string
)

var LogLevel slog.LevelVar

func init() {
	LogLevel.Set(slog.LevelInfo)
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
}

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

	var handler slog.Handler

	writer, err := OpenWriter(opts.Logfile)
	FatalOnError(err, "Failed to open ")

	if writer != nil {
		if opts.JSONFormatter {
			handler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
				AddSource: true,
				Level:     &LogLevel,
			})
		} else {
			handler = tint.NewHandler(os.Stderr, &tint.Options{
				AddSource:  true,
				Level:      &LogLevel,
				TimeFormat: "2006-01-02 15:04:05.000",
				NoColor:    !isatty.IsTerminal(os.Stderr.Fd()),
			})
		}
	} else {
		// discard logging
		handler = nilhandler{}
	}

	// use the handler for the default handler
	logger := slog.New(handler)
	slog.SetDefault(logger)

	if opts.Verbose {
		LogLevel.Set(slog.LevelDebug)
		logger.Debug("Enabled verbose logging")
	}
}

type nilhandler struct{}

func (n nilhandler) Enabled(ctx context.Context, level slog.Level) bool {
	return false
}

func (n nilhandler) Handle(ctx context.Context, record slog.Record) error {
	return nil
}

func (n nilhandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return n
}

func (n nilhandler) WithGroup(name string) slog.Handler {
	return n
}
