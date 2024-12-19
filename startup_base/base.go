package startup_base

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/flachnetz/startup/v2/lib/clock"
	"github.com/flachnetz/startup/v2/startup_base/tint"
	sl "github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/mattn/go-isatty"
)

var (
	BuildPackage       string
	BuildGitHash       string
	BuildVersion       string
	BuildUnixTimestamp string
)

var LogLevel slog.LevelVar

var handlerVar slog.Handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{AddSource: true})

var lock sync.RWMutex
var baseOptions BaseOptions

func init() {
	lazy := &LazyHandler{
		Delegate: func() slog.Handler {
			return handlerVar
		},
	}

	LogLevel.Set(slog.LevelInfo)
	slog.SetDefault(slog.New(lazy))
}

type BaseOptions struct {
	Logfile       string `long:"log-file" description:"Write logs to a different file. Defaults to stdout."`
	ForceColor    bool   `long:"log-color" description:"Forces colored output even on non TTYs."`
	JSONFormatter bool   `long:"log-json" description:"Log using the logrus json formatter."`

	Verbose     bool   `long:"verbose" description:"Show verbose logging output."`
	Version     bool   `long:"version" description:"Prints the build information about this application if available."`
	Environment string `long:"environment" description:"The environment this application is running in."`
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
			handler = slog.NewJSONHandler(writer, &slog.HandlerOptions{
				AddSource: true,
				Level:     &LogLevel,
			})
		} else {
			handler = tint.NewHandler(writer, &tint.Options{
				AddSource:  true,
				Level:      &LogLevel,
				TimeFormat: "2006-01-02 15:04:05.000",
				NoColor:    !isatty.IsTerminal(writer.Fd()),
			})
		}
	} else {
		// discard logging
		handler = nilhandler{}
	}

	handler = sl.Wrap(handler, sl.WithTraceId)
	handler = sl.Wrap(handler, clock.AdjustTimeInLog)

	handlerVar = handler

	// use the handler for the default handler
	logger := slog.New(handler)
	slog.SetDefault(logger)

	if opts.Verbose {
		LogLevel.Set(slog.LevelDebug)
		logger.Debug("Enabled verbose logging")
	}

	if opts.Environment == "" {
		stage := os.Getenv("STAGE")
		if stage != "" {
			logger.Info("Using environment from STAGE environment variable: " + stage)
			opts.Environment = stage
		} else {
			opts.Environment = "development"
		}
	}
	logger.Info("Environment: " + opts.Environment)
	lock.Lock()
	baseOptions = *opts
	lock.Unlock()
}

func IsDevelopment() bool {
	lock.RLock()
	environment := strings.ToLower(baseOptions.Environment)
	lock.RUnlock()
	return environment == "development" || environment == "dev"
}

func IsTesting() bool {
	lock.RLock()
	environment := strings.ToLower(baseOptions.Environment)
	lock.RUnlock()
	return environment == "testing" || environment == "test"
}

func IsStaging() bool {
	lock.RLock()
	environment := strings.ToLower(baseOptions.Environment)
	lock.RUnlock()
	return environment == "staging" || environment == "stage"
}

func IsProduction() bool {
	lock.RLock()
	environment := strings.ToLower(baseOptions.Environment)
	lock.RUnlock()
	return environment == "production" || environment == "prod" || environment == "live"
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

type LazyHandler struct {
	Delegate func() slog.Handler
}

func (v *LazyHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (v *LazyHandler) Handle(ctx context.Context, record slog.Record) error {
	return v.Delegate().Handle(ctx, record)
}

func (v *LazyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &LazyHandler{
		Delegate: func() slog.Handler {
			return v.Delegate().WithAttrs(attrs)
		},
	}
}

func (v *LazyHandler) WithGroup(name string) slog.Handler {
	return &LazyHandler{
		Delegate: func() slog.Handler {
			return v.Delegate().WithGroup(name)
		},
	}
}
