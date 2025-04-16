package startup_metrics

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	Prometheus struct {
		Path string `long:"prometheus-path" default:"/metrics" description:"Path for Prometheus metrics endpoint"`
		Port string `long:"prometheus-port" default:":9090" description:"Port for Prometheus metrics endpoint"`
	}

	Inputs struct {
		ServiceName string `long:"service-name" description:"Service name for metrics"`
	}

	once       sync.Once
	mp         *sdkmetric.MeterProvider
	httpServer *http.Server
}

func (opts *OTELMetricsOptions) Initialize() {
	opts.once.Do(func() {
		// Create resource with service information and base attributes
		res, err := opts.createResource(opts.Inputs.ServiceName)
		startup_base.PanicOnError(err, "Failed to create resource")

		// Create readers slice for multiple exporters
		var readers []sdkmetric.Option
		logger := startup_logrus.LoggerOf(context.Background())

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

		go func() {
			// Start Prometheus HTTP server
			mux := http.NewServeMux()
			mux.Handle(opts.Prometheus.Path, promhttp.Handler())
			opts.httpServer = &http.Server{
				Addr:    opts.Prometheus.Port,
				Handler: mux,
			}

			logger.Infof("Starting Prometheus metrics endpoint on %s%s", opts.Prometheus.Port, opts.Prometheus.Path)
			if err := opts.httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				logger.WithError(err).Error("Prometheus HTTP server failed")
			}
		}()
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
	if opts.httpServer != nil {
		if err := opts.httpServer.Shutdown(ctx); err != nil {
			startup_logrus.LoggerOf(ctx).WithError(err).Error("Failed to shutdown Prometheus HTTP server")
		}
	}

	// Shutdown meter provider
	if opts.mp != nil {
		return opts.mp.Shutdown(ctx)
	}
	return nil
}
