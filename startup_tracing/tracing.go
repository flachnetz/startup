package startup_tracing

import (
	"context"
	"log/slog"
	"sync"

	"github.com/flachnetz/startup/v2/startup_base"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var log = slog.With(slog.String("prefix", "tracing"))

type TracingOptions struct {
	HttpEndpoint string `long:"otlp-trace-endpoint-http" validate:"omitempty" description:"OTLP HTTP endpoint for traces, e.g. localhost:4318"`

	Inputs struct {
		// The service name of your application
		ServiceName string `validate:"required"`
	}

	once sync.Once
}

func (opts *TracingOptions) IsActive() bool {
	return opts.HttpEndpoint != ""
}

func (opts *TracingOptions) Initialize() {
	if !opts.IsActive() {
		return
	}

	opts.once.Do(func() {
		ctx := context.Background()

		res, err := resource.New(ctx,
			resource.WithAttributes(
				semconv.ServiceName(opts.Inputs.ServiceName),
			),
		)
		startup_base.PanicOnError(err, "Unable to create otel resource")

		var exporter sdktrace.SpanExporter

		if opts.HttpEndpoint != "" {
			log.Info("Sending traces via OTLP HTTP", slog.String("endpoint", opts.HttpEndpoint))
			exporter, err = otlptracehttp.New(ctx,
				otlptracehttp.WithEndpoint(opts.HttpEndpoint),
				otlptracehttp.WithInsecure(),
			)
			startup_base.PanicOnError(err, "Unable to create OTLP HTTP trace exporter")
		}

		tp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
		)

		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
	})
}

type slogWriter struct {
	logger *slog.Logger
}

func (w slogWriter) Write(p []byte) (int, error) {
	w.logger.Info(string(p))
	return len(p), nil
}
