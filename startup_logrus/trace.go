package startup_logrus

import (
	"context"
	"log/slog"

	"github.com/labstack/echo/v4"
	"github.com/opentracing/opentracing-go"
	zipkintracer "github.com/openzipkin-contrib/zipkin-go-opentracing"
)

func EchoTraceLoggerMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			traceId := c.Request().Header.Get("X-Trace-Id")
			ctx := ContextLoggerWithFields(c.Request().Context(), "traceId", traceId)
			c.SetRequest(c.Request().Clone(ctx))
			return next(c)
		}
	}
}

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
