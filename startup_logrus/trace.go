package startup_logrus

import (
	"context"
	"log/slog"

	"github.com/opentracing/opentracing-go"
	zipkintracer "github.com/openzipkin-contrib/zipkin-go-opentracing"
)

func WithTraceId(ctx context.Context, record slog.Record) (slog.Record, bool, error) {
	// get the span from the entries context
	spanContext := spanContextOf(ctx)

	if spanContext != nil {
		if traceId := traceIdOf(spanContext); traceId != "" {
			// add traceId to log record
			record.AddAttrs(slog.String("traceId", traceId))
		}
	}

	return record, true, nil
}

func spanContextOf(ctx context.Context) opentracing.SpanContext {
	if ctx == nil {
		return nil
	}

	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return nil
	}

	return span.Context()
}

func traceIdOf(spanContext interface{}) string {
	if spanContext, ok := spanContext.(zipkintracer.SpanContext); ok {
		return spanContext.TraceID.String()
	}

	return ""
}
