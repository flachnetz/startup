package sl

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

func WithTraceId(ctx context.Context, record slog.Record) (slog.Record, bool, error) {
	span := trace.SpanFromContext(ctx)

	if span != nil && span.SpanContext().IsValid() {
		traceId := span.SpanContext().TraceID().String()
		record.AddAttrs(slog.String("traceId", traceId))
	}

	return record, true, nil
}
