package startup_tracing

import (
	"context"
	"database/sql"
	"github.com/modern-go/gls"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/pkg/errors"
)

// Use the legacy GLS way to forward spans using goroutine-local storage.
var UseGLS = true

// a span that does nothing
var noopSpan = opentracing.NoopTracer{}.StartSpan("")

type activeSpan struct{}

var activeSpanKey = activeSpan{}

// Returns the current span, or nil, if no span is currently set
// in local storage.
//
// Deprecated: Start using the version with an explicit context parameter.
//
func CurrentSpan() opentracing.Span {
	if UseGLS {
		if g := gls.GetGls(gls.GoID()); g != nil {
			if span, ok := g[activeSpanKey].(opentracing.Span); ok {
				return span
			}
		}
	}

	return nil
}

// Runs the given function with the provided span
// set in local storage for the duration of function call.
// This method will not call 'Finish()' on the span
//
// Deprecated: Please propagate spans using contexts.
//
func WithSpan(span opentracing.Span, fn func()) {
	if UseGLS {
		if g := gls.GetGls(gls.GoID()); g != nil {
			previousSpan := g[activeSpanKey]
			g[activeSpanKey] = span

			// restore previous span later
			defer func() {
				g[activeSpanKey] = previousSpan
			}()
		}
	}

	fn()
}

// Runs an operation and traces it with the given name. This will create a
// new child span if some span is currently active.
//
// Deprecated: propagate spans using context.
//
func TraceChild(op string, fn func(span opentracing.Span) error, spanOpts ...opentracing.StartSpanOption) (err error) {
	return trace(op, false, fn, spanOpts...)
}

// Runs an operation and traces it with the given name. This will create a
// new span if no span is currently active.
//
// Deprecated: propagate spans using context.
//
func TraceOrCreate(op string, fn func(span opentracing.Span) error, spanOpts ...opentracing.StartSpanOption) (err error) {
	return trace(op, true, fn, spanOpts...)
}

func trace(op string, always bool, fn func(span opentracing.Span) error, spanOpts ...opentracing.StartSpanOption) (err error) {
	span := noopSpan

	if UseGLS {
		if g := gls.GetGls(gls.GoID()); g != nil {
			previousSpan, ok := g[activeSpanKey].(opentracing.Span)

			if ok && previousSpan != nil {
				// build a child span
				span = previousSpan.Tracer().StartSpan(op, append(spanOpts, ext.SpanKindRPCClient, opentracing.ChildOf(previousSpan.Context()))...)
			} else if always {
				// start a new one
				span = opentracing.StartSpan(op, append(spanOpts, ext.SpanKindRPCClient)...)
			}

			g[activeSpanKey] = span

			defer func() {
				g[activeSpanKey] = previousSpan

				if err != nil && isNotErrNoRows(err) {
					span.SetTag("error", true)
					span.SetTag("error_message", err.Error())
				}

				span.Finish()
			}()
		}
	}

	err = fn(span)
	return
}

// Trace a child call while propagating the span using the context.
func TraceChildContext(ctx context.Context, op string, fn func(ctx context.Context, span opentracing.Span) error) (err error) {
	var parentContext opentracing.SpanContext

	parentSpan := CurrentSpanFromContextOrGLS(ctx)
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

// Returns the current span, or nil, if no span is currently set
// in local storage.
//
func CurrentSpanFromContextOrGLS(ctx context.Context) opentracing.Span {
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		return span
	}

	if UseGLS {
		if g := gls.GetGls(gls.GoID()); g != nil {
			if span, ok := g[activeSpanKey].(opentracing.Span); ok {
				return span
			}
		}
	}

	return nil
}

func isNotErrNoRows(err error) bool {
	return errors.Cause(err) != sql.ErrNoRows
}
