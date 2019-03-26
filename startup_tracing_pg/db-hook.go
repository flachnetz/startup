package startup_tracing_pg

import (
	"context"
	"github.com/flachnetz/startup/startup_tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"strings"
)

type dbHook struct {
	ServiceName string
}

func (h *dbHook) Before(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	// lookup if we have a parent span
	parent := startup_tracing.CurrentSpanFromContextOrGLS(ctx)

	// if we dont have a parent span, we don't do tracing
	if parent == nil {
		return ctx, nil
	}

	span := opentracing.StartSpan(h.ServiceName,
		opentracing.ChildOf(parent.Context()),
		ext.SpanKindRPCClient)

	// set extra tags for our datadog proxy
	span.SetTag("dd.service", h.ServiceName)
	span.SetTag("dd.resource", strings.TrimSpace(query))

	return opentracing.ContextWithSpan(ctx, span), nil
}

func (h *dbHook) After(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		span.Finish()
	}

	return ctx, nil
}

func (h *dbHook) OnError(ctx context.Context, err error, query string, args ...interface{}) error {
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		span.SetTag("error", true)
		span.SetTag("err", err.Error())
		span.Finish()
	}

	return err
}
