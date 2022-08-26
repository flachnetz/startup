package startup_tracing

import (
	"context"
	"database/sql"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/pkg/errors"
)

// a span that does nothing
var noopSpan = opentracing.NoopTracer{}.StartSpan("")

type activeSpan struct{}

var activeSpanKey = activeSpan{}

// Trace a child call while propagating the span using the context.
func TraceChildContext(ctx context.Context, op string, fn func(ctx context.Context, span opentracing.Span) error) (err error) {
	var parentContext opentracing.SpanContext

	parentSpan := CurrentSpanFromContext(ctx)
	if parentSpan != nil {
		parentContext = parentSpan.Context()
	}

	span := opentracing.GlobalTracer().StartSpan(op,
		ext.SpanKindRPCClient,
		opentracing.ChildOf(parentContext))

	defer func() {
		if err != nil && isNotErrNoRows(err) {
			span.SetTag("error", true)
			span.SetTag("error_message", err.Error())
		}

		span.Finish()
	}()

	err = fn(opentracing.ContextWithSpan(ctx, span), span)
	return
}

// Returns the current span, or nil.
func CurrentSpanFromContext(ctx context.Context) opentracing.Span {
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		return span
	}

	return nil
}

func isNotErrNoRows(err error) bool {
	return errors.Cause(err) != sql.ErrNoRows
}
