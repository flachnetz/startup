package startup_tracing_pg

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"

	"github.com/flachnetz/startup/v2/lib/pg_trace"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var reSpace = regexp.MustCompile(`\s+`)

type tracer struct {
	ServiceName         string
	SkipFrameworkMethod SkipFunc
}

func (t *tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return ctx
	}
	cleanQuery := cleanQuery(data.SQL)
	ctx, span := t.startSpan(ctx, cleanQuery)
	span.SetAttributes(attribute.String("sql.query", cleanQuery))
	return ctx
}

func (t *tracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return
	}
	span := trace.SpanFromContext(ctx)

	if data.Err != nil && !errors.Is(data.Err, sql.ErrNoRows) {
		span.SetStatus(codes.Error, data.Err.Error())
		span.SetAttributes(attribute.String("error", data.Err.Error()))
	}

	span.End()
}

func (t *tracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return ctx
	}
	cleanQuery := cleanQuery(data.SQL)
	ctx, span := t.startSpan(ctx, cleanQuery)
	span.SetAttributes(
		attribute.String("sql.query", cleanQuery),
		attribute.Bool("sql.prepare", true),
	)
	return ctx
}

func (t *tracer) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return
	}
	trace.SpanFromContext(ctx).End()
}

func (t *tracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return ctx
	}
	ctx, _ = t.startSpan(ctx, "CONNECT")
	return ctx
}

func (t *tracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return
	}
	trace.SpanFromContext(ctx).End()
}

func (t *tracer) TransactionStart(ctx context.Context) context.Context {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return ctx
	}
	tag := findOutsideCaller(t.SkipFrameworkMethod)
	if tag == "" {
		tag = "transaction"
	}

	ctx, _ = t.startSpan(ctx, "tx:"+tag)
	return ctx
}

func (t *tracer) TransactionEnd(ctx context.Context) {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return
	}
	trace.SpanFromContext(ctx).End()
}

func (t *tracer) AcquireConnectionStart(ctx context.Context) context.Context {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return ctx
	}
	ctx, _ = t.startSpan(ctx, "tx:acquire-connection")
	return ctx
}

func (t *tracer) AcquireConnectionEnd(ctx context.Context) {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return
	}
	trace.SpanFromContext(ctx).End()
}

func (t *tracer) startSpan(ctx context.Context, res string) (context.Context, trace.Span) {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return ctx, trace.SpanFromContext(ctx)
	}

	ctx, span := otel.Tracer("").Start(ctx, t.ServiceName,
		trace.WithSpanKind(trace.SpanKindClient),
	)

	span.SetAttributes(
		attribute.String("peer.service", t.ServiceName),
		attribute.String("dd.resource", res),
		attribute.String("resource.name", res),
	)

	return ctx, span
}

var cleanQueryCache *lru.Cache

func init() {
	cleanQueryCache, _ = lru.New(10_000)
}

func cleanQuery(query string) string {
	cached, ok := cleanQueryCache.Get(query)
	if ok {
		return cached.(string)
	}

	cleaned := strings.TrimSpace(reSpace.ReplaceAllString(query, " "))
	cleanQueryCache.Add(query, cleaned)
	return cleaned
}
