package startup_metrics

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/flachnetz/startup/v2/startup_http"
	"github.com/gorilla/handlers"

	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type PrometheusConfig struct {
	Disabled bool   `long:"prometheus-disabled" env:"PROMETHEUS_DISABLED" description:"Disable Prometheus metrics endpoint"`
	Path     string `long:"prometheus-path" env:"PROMETHEUS_PATH" default:"/metrics" description:"Path for Prometheus metrics endpoint"`
	Port     string `long:"prometheus-port" env:"PROMETHEUS_PORT" default:":9090" description:"Port for Prometheus metrics endpoint"`

	httpServer *http.Server
}

func startPrometheusMetrics(opts PrometheusConfig) *http.Server {
	mux := http.NewServeMux()
	logger := slog.With(slog.String("prefix", "prometheus"))

	handler := promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
		ErrorLog: &slogErrorLogger{logger: logger},
	})

	// don't let a panic crash the server.
	handler = handlers.RecoveryHandler(handlers.PrintRecoveryStack(true),
		handlers.RecoveryLogger(startup_http.NewSlogRecoveryHandlerLogger()))(handler)
	mux.Handle(opts.Path, handler)

	opts.httpServer = &http.Server{
		Addr:    opts.Port,
		Handler: mux,
	}

	go func() {
		// Start Prometheus HTTP server
		logger.Info("Starting Prometheus metrics endpoint", slog.String("port", opts.Port), slog.String("path", opts.Path))
		if err := opts.httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Prometheus HTTP server failed", slog.String("error", err.Error()))
		}
	}()

	return opts.httpServer
}

// slogErrorLogger adapts *slog.Logger for use as promhttp.Logger (which expects Println).
type slogErrorLogger struct {
	logger *slog.Logger
}

func (l *slogErrorLogger) Println(v ...any) {
	l.logger.Error(strings.TrimSpace(fmt.Sprint(v...)))
}
