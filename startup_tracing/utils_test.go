package startup_tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})

	return recorder
}

func TestTrace_SpanKindDefaultsToClient(t *testing.T) {
	recorder := recordSpans(t)

	require.NoError(t, Trace(t.Context(), "op", func(ctx context.Context, span trace.Span) error {
		return nil
	}))

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, trace.SpanKindClient, spans[0].SpanKind())
}

func TestTrace_SpanKindOverride(t *testing.T) {
	recorder := recordSpans(t)

	require.NoError(t, Trace(t.Context(), "op", func(ctx context.Context, span trace.Span) error {
		return nil
	}, trace.WithSpanKind(trace.SpanKindProducer)))

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, trace.SpanKindProducer, spans[0].SpanKind())
}
