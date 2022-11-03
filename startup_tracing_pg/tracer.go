package startup_tracing_pg

import (
	"context"

	st "github.com/flachnetz/startup/v2/startup_tracing"
	"github.com/jackc/pgx/v5"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

type tracer struct {
	ServiceName         string
	SkipFrameworkMethod SkipFunc
}

func (t *tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return t.startSpan(ctx, "TraceQuery")
}

func (t *tracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	t.endSpan(ctx)
}

func (t *tracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	return t.startSpan(ctx, "TracePrepare")
}

func (t *tracer) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	t.endSpan(ctx)
}

func (t *tracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	return t.startSpan(ctx, "TraceConnect")
}

func (t *tracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	t.endSpan(ctx)
}

func (t *tracer) TransactionStart(ctx context.Context) context.Context {
	return t.startSpan(ctx, "Transaction")
}

func (t *tracer) TransactionEnd(ctx context.Context) {
	t.endSpan(ctx)
}

func (t *tracer) startSpan(ctx context.Context, op string) context.Context {
	var parentContext opentracing.SpanContext

	parentSpan := st.CurrentSpanFromContext(ctx)
	if parentSpan != nil {
		parentContext = parentSpan.Context()
	}

	span := opentracing.GlobalTracer().StartSpan(op,
		ext.SpanKindRPCClient,
		opentracing.ChildOf(parentContext),
	)

	return opentracing.ContextWithSpan(ctx, span)
}

func (t *tracer) endSpan(ctx context.Context) {
	st.CurrentSpanFromContext(ctx).Finish()
}
