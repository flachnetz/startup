package startup_postgres

import (
	"context"
	"log/slog"

	"github.com/flachnetz/startup/v2/lib/pg_trace"

	"github.com/jackc/pgx/v5"
)

type tracerWrapper struct {
	logger *slog.Logger
}

var (
	_ = (pgx.QueryTracer)(tracerWrapper{})
	_ = (pgx.PrepareTracer)(tracerWrapper{})
	_ = (pgx.ConnectTracer)(tracerWrapper{})
)

func (m tracerWrapper) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return ctx
	}
	if m.logger != nil {
		m.logger.DebugContext(ctx, "Query start",
			slog.String("query", data.SQL),
			slog.Any("args", data.Args),
		)
	}
	tracer := globalTracer.Load()
	if tracer == nil {
		return ctx
	}

	return (*tracer).TraceQueryStart(ctx, conn, data)
}

func (m tracerWrapper) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return
	}
	tracer := globalTracer.Load()
	if tracer == nil {
		return
	}

	(*tracer).TraceQueryEnd(ctx, conn, data)
}

func (m tracerWrapper) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return ctx
	}
	if m.logger != nil {
		m.logger.DebugContext(ctx, "Prepare start",
			slog.String("name", data.Name),
			slog.String("sql", data.SQL),
		)
	}
	tracer := globalTracer.Load()
	if tracer == nil {
		return ctx
	}

	return (*tracer).TracePrepareStart(ctx, conn, data)
}

func (m tracerWrapper) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return
	}
	tracer := globalTracer.Load()
	if tracer == nil {
		return
	}

	(*tracer).TracePrepareEnd(ctx, conn, data)
}

func (m tracerWrapper) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return ctx
	}
	tracer := globalTracer.Load()
	if tracer == nil {
		return ctx
	}

	return (*tracer).TraceConnectStart(ctx, data)
}

func (m tracerWrapper) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	if ctx.Value(pg_trace.DisableTracingKey) != nil {
		return
	}
	tracer := globalTracer.Load()
	if tracer == nil {
		return
	}

	(*tracer).TraceConnectEnd(ctx, data)
}
