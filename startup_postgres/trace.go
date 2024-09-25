package startup_postgres

import (
	"context"
	"github.com/sirupsen/logrus"

	"github.com/jackc/pgx/v5"
)

type tracerWrapper struct {
	logger *logrus.Entry
}

var (
	_ = (pgx.QueryTracer)(tracerWrapper{})
	_ = (pgx.PrepareTracer)(tracerWrapper{})
	_ = (pgx.ConnectTracer)(tracerWrapper{})
)

func (m tracerWrapper) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if m.logger != nil {
		m.logger.WithFields(logrus.Fields{
			"query": data.SQL,
			"args":  data.Args,
		}).Debug("Query start")
	}
	tracer := globalTracer.Load()
	if tracer == nil {
		return ctx
	}

	return (*tracer).TraceQueryStart(ctx, conn, data)
}

func (m tracerWrapper) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	tracer := globalTracer.Load()
	if tracer == nil {
		return
	}

	(*tracer).TraceQueryEnd(ctx, conn, data)
}

func (m tracerWrapper) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	if m.logger != nil {
		m.logger.WithFields(logrus.Fields{
			"name": data.Name,
			"sql":  data.SQL,
		}).Debug("Prepare start")
	}
	tracer := globalTracer.Load()
	if tracer == nil {
		return ctx
	}

	return (*tracer).TracePrepareStart(ctx, conn, data)
}

func (m tracerWrapper) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	tracer := globalTracer.Load()
	if tracer == nil {
		return
	}

	(*tracer).TracePrepareEnd(ctx, conn, data)
}

func (m tracerWrapper) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	tracer := globalTracer.Load()
	if tracer == nil {
		return ctx
	}

	return (*tracer).TraceConnectStart(ctx, data)
}

func (m tracerWrapper) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	tracer := globalTracer.Load()
	if tracer == nil {
		return
	}

	(*tracer).TraceConnectEnd(ctx, data)
}
