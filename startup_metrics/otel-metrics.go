package startup_metrics

import (
	"context"
	"sync"
	"time"

	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

type OTELMetricsOptions struct {
	// Prometheus configuration
	PrometheusConfig PrometheusConfig

	Inputs struct {
		ServiceName string `long:"service-name" description:"Service name for metrics"`
	}

	once sync.Once
	mp   *sdkmetric.MeterProvider
}

func (opts *OTELMetricsOptions) Initialize() {
	opts.once.Do(func() {
		// Create resource with service information and base attributes
		res, err := opts.createResource(opts.Inputs.ServiceName)
		startup_base.PanicOnError(err, "Failed to create resource")

		// Create readers slice for multiple exporters
		var readers []sdkmetric.Option

		promExporter, err := prometheus.New()
		startup_base.PanicOnError(err, "Failed to create Prometheus exporter")
		readers = append(readers, sdkmetric.WithReader(promExporter))

		opts.mp = sdkmetric.NewMeterProvider(
			append([]sdkmetric.Option{
				sdkmetric.WithResource(res),
			}, readers...)...,
		)

		// Set as global meter provider
		otel.SetMeterProvider(opts.mp)
		opts.captureRuntimeMetrics()

		if !opts.PrometheusConfig.Disabled {
			// Start Prometheus HTTP server
			opts.PrometheusConfig.httpServer = startPrometheusMetrics(opts.PrometheusConfig)
		}
	})
}

func (opts *OTELMetricsOptions) createResource(serviceName string) (*resource.Resource, error) {
	if serviceName == "" {
		return nil, errors.New("service name must be set")
	}

	// Create base attributes using OpenTelemetry semantic conventions
	attributes := []attribute.KeyValue{
		semconv.ServiceName(serviceName),
	}

	return resource.New(context.Background(),
		resource.WithAttributes(attributes...),
		resource.WithFromEnv(),
		resource.WithHost(),
		resource.WithContainer(),
		resource.WithOS(),
		// resource.WithProcess(),
	)
}

func (opts *OTELMetricsOptions) captureRuntimeMetrics() {
	err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(5 * time.Second))
	startup_base.PanicOnError(err, "Failed to start runtime metrics collection")
}

func (opts *OTELMetricsOptions) Shutdown() error {
	// Shutdown Prometheus HTTP server if it exists
	ctx := context.Background()
	if opts.PrometheusConfig.httpServer != nil {
		if err := opts.PrometheusConfig.httpServer.Shutdown(ctx); err != nil {
			startup_logrus.LoggerOf(ctx).WithError(err).Error("Failed to shutdown Prometheus HTTP server")
		}
	}

	// Shutdown meter provider
	if opts.mp != nil {
		return opts.mp.Shutdown(ctx)
	}
	return nil
}
