package startup_tracing_pg

import (
	"context"
	"database/sql"
	"errors"
	"github.com/flachnetz/startup/v2/lib/pg"
	"regexp"
	"strings"

	st "github.com/flachnetz/startup/v2/startup_tracing"
	"github.com/hashicorp/golang-lru"
	"github.com/jackc/pgx/v5"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

var reSpace = regexp.MustCompile(`\s+`)

type tracer struct {
	ServiceName         string
	SkipFrameworkMethod SkipFunc
}

func (t *tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if ctx.Value(pg.DisableTracingKey) != nil {
		return ctx
	}
	cleanQuery := cleanQuery(data.SQL)
	span, ctx := t.startSpan(ctx, cleanQuery)
	span.SetTag("sql.query", cleanQuery)
	return ctx
}

func (t *tracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if ctx.Value(pg.DisableTracingKey) != nil {
		return
	}
	span := t.spanOf(ctx)

	if data.Err != nil && !errors.Is(data.Err, sql.ErrNoRows) {
		span.SetTag("error", data.Err.Error())
	}

	span.Finish()
}

func (t *tracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	if ctx.Value(pg.DisableTracingKey) != nil {
		return ctx
	}
	cleanQuery := cleanQuery(data.SQL)
	span, ctx := t.startSpan(ctx, cleanQuery)
	span.SetTag("sql.query", cleanQuery)
	span.SetTag("sql.prepare", true)
	return ctx
}

func (t *tracer) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	if ctx.Value(pg.DisableTracingKey) != nil {
		return
	}
	t.spanOf(ctx).Finish()
}

func (t *tracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	if ctx.Value(pg.DisableTracingKey) != nil {
		return ctx
	}
	_, ctx = t.startSpan(ctx, "CONNECT")
	return ctx
}

func (t *tracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	if ctx.Value(pg.DisableTracingKey) != nil {
		return
	}
	t.spanOf(ctx).Finish()
}

func (t *tracer) TransactionStart(ctx context.Context) context.Context {
	if ctx.Value(pg.DisableTracingKey) != nil {
		return ctx
	}
	tag := findOutsideCaller(t.SkipFrameworkMethod)
	if tag == "" {
		tag = "transaction"
	}

	_, ctx = t.startSpan(ctx, "tx:"+tag)
	return ctx
}

func (t *tracer) TransactionEnd(ctx context.Context) {
	if ctx.Value(pg.DisableTracingKey) != nil {
		return
	}
	t.spanOf(ctx).Finish()
}

func (t *tracer) AcquireConnectionStart(ctx context.Context) context.Context {
	if ctx.Value(pg.DisableTracingKey) != nil {
		return ctx
	}
	_, ctx = t.startSpan(ctx, "tx:acquire-connection")
	return ctx
}

func (t *tracer) AcquireConnectionEnd(ctx context.Context) {
	if ctx.Value(pg.DisableTracingKey) != nil {
		return
	}
	t.spanOf(ctx).Finish()
}

func (t *tracer) startSpan(ctx context.Context, res string) (opentracing.Span, context.Context) {
	if ctx.Value(pg.DisableTracingKey) != nil {
		return nil, ctx
	}
	var parentContext opentracing.SpanContext

	parentSpan := st.CurrentSpanFromContext(ctx)
	if parentSpan != nil {
		parentContext = parentSpan.Context()
	}

	span := opentracing.GlobalTracer().StartSpan(t.ServiceName,
		ext.SpanKindRPCClient,
		opentracing.ChildOf(parentContext),
	)

	span.SetTag("dd.service", t.ServiceName)
	span.SetTag("dd.resource", res)

	return span, opentracing.ContextWithSpan(ctx, span)
}

func (t *tracer) spanOf(ctx context.Context) opentracing.Span {
	return st.CurrentSpanFromContext(ctx)
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
