package sl

import (
	"context"
	"log/slog"

	"github.com/flachnetz/startup/v2/lib/tls"
	"go.opentelemetry.io/otel/trace"
)

type ThreadLocalTraceID trace.TraceID

func WithTraceId(ctx context.Context, record slog.Record) (slog.Record, bool, error) {
	span := trace.SpanFromContext(ctx)

	if span != nil && span.SpanContext().IsValid() {
		traceId := span.SpanContext().TraceID().String()
		record.AddAttrs(slog.String("traceId", traceId))
	} else {
		// check thread local storage, maybe it is in there
		if traceId, ok := tls.Get[ThreadLocalTraceID](); ok {
			traceIdStr := trace.TraceID(traceId).String()
			record.AddAttrs(slog.String("traceId", traceIdStr))
		}
	}

	return record, true, nil
}
