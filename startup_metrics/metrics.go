package startup_metrics

import (
	"context"
	"sync"
	"time"

	"log/slog"

	"github.com/flachnetz/startup/v2/startup_base"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

var log = slog.With(slog.String("prefix", "main"))

type MetricsPrefix string

type MetricsOptions struct {
	PrometheusConfig PrometheusConfig

	Inputs struct {
		// Prefix to apply to all metrics. This must not be empty.
		MetricsPrefix string `validate:"required"`
	}

	once sync.Once
	mp   *sdkmetric.MeterProvider
}

func (opts *MetricsOptions) Initialize() {
	opts.once.Do(func() {
		prefix := opts.Inputs.MetricsPrefix
		if prefix == "" {
			startup_base.Panicf("Metrics prefix must be set")
			return
		}

		log.Debug("Initializing metrics with OTel", slog.String("prefix", prefix))

		res, err := opts.createResource(prefix)
		startup_base.PanicOnError(err, "Failed to create OTel resource")

		promExporter, err := prometheus.New()
		startup_base.PanicOnError(err, "Failed to create Prometheus exporter")

		opts.mp = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(promExporter),
		)

		otel.SetMeterProvider(opts.mp)

		opts.captureRuntimeMetrics()

		if !opts.PrometheusConfig.Disabled {
			opts.PrometheusConfig.httpServer = startPrometheusMetrics(opts.PrometheusConfig)
		}
	})
}

func (opts *MetricsOptions) createResource(serviceName string) (*resource.Resource, error) {
	if serviceName == "" {
		return nil, errors.New("service name must be set")
	}

	attributes := []attribute.KeyValue{
		semconv.ServiceName(serviceName),
	}

	return resource.New(context.Background(),
		resource.WithAttributes(attributes...),
		resource.WithFromEnv(),
		resource.WithHost(),
		resource.WithContainer(),
		resource.WithOS(),
	)
}

func (opts *MetricsOptions) captureRuntimeMetrics() {
	log.Debug("Start capturing of golang runtime metrics")
	err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(5 * time.Second))
	startup_base.PanicOnError(err, "Failed to start runtime metrics collection")
}

func (opts *MetricsOptions) Shutdown() error {
	ctx := context.Background()

	if opts.PrometheusConfig.httpServer != nil {
		if err := opts.PrometheusConfig.httpServer.Shutdown(ctx); err != nil {
			sl.LoggerOf(ctx).ErrorContext(ctx, "Failed to shutdown Prometheus HTTP server", sl.Error(err))
		}
	}

	if opts.mp != nil {
		return opts.mp.Shutdown(ctx)
	}

	return nil
}
