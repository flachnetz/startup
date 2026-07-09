package startup_base

import (
	"fmt"
	"log/slog"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"github.com/flachnetz/startup/v2/lib/clock"
	"github.com/flachnetz/startup/v2/startup_base/tint"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/mattn/go-isatty"
)

var (
	BuildPackage       string
	BuildGitHash       string
	BuildVersion       string
	BuildUnixTimestamp string
)

var LogLevel slog.LevelVar

var handlerVar slog.Handler = slog.NewTextHandler(
	os.Stderr,
	&slog.HandlerOptions{AddSource: true},
)

var baseOptions atomic.Pointer[BaseOptions]

func init() {
	baseOptions.Store(new(BaseOptions))

	lazy := &lazyHandler{
		Delegate: func() slog.Handler {
			return handlerVar
		},
	}

	LogLevel.Set(slog.LevelInfo)
	slog.SetDefault(slog.New(lazy))
}

type BaseOptions struct {
	Logfile                string `long:"log-file" env:"LOG_FILE" description:"Write logs to a different file. Defaults to stdout."`
	ForceColor             bool   `long:"log-color" env:"LOG_COLOR" description:"Forces colored output even on non TTYs."`
	JSONFormatter          bool   `long:"log-json" env:"LOG_JSON" description:"Log using the logrus json formatter."`
	JSONFormatterLogSource bool   `long:"log-json-source" env:"LOG_JSON_SOURCE" description:"When doing json logging, log source code file and position as well."`

	Verbose     bool   `long:"verbose" env:"VERBOSE" description:"Show verbose logging output."`
	Version     bool   `long:"version" env:"VERSION" description:"Prints the build information about this application if available."`
	Environment string `long:"environment" env:"ENVIRONMENT" description:"The environment this application is running in."`
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
				AddSource: opts.JSONFormatterLogSource,
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
		handler = nilHandler{}
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
	baseOptions.Store(opts)
}

func IsVerboseLoggingEnabled() bool {
	return baseOptions.Load().Verbose
}

func GetEnvironment() string {
	return baseOptions.Load().Environment
}

func IsDevelopment() bool {
	environment := strings.ToLower(baseOptions.Load().Environment)
	return environment == "development" || environment == "dev"
}

func IsTesting() bool {
	environment := strings.ToLower(baseOptions.Load().Environment)
	return environment == "testing" || environment == "test"
}

func IsStaging() bool {
	environment := strings.ToLower(baseOptions.Load().Environment)
	return environment == "staging" || environment == "stage"
}

func IsProduction() bool {
	environment := strings.ToLower(baseOptions.Load().Environment)
	return environment == "production" || environment == "prod" || environment == "live"
}
