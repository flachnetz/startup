package startup_tracing_pg

import (
	"context"
	"github.com/flachnetz/startup/startup_postgres"
	"github.com/flachnetz/startup/startup_tracing"
	"github.com/jmoiron/sqlx"
	"github.com/opentracing/opentracing-go"
)

type TracedHelper struct {
	*sqlx.DB
	tracingServiceName string
}

func New(db *sqlx.DB, serviceName string) TracedHelper {
	return TracedHelper{
		DB:                 db,
		tracingServiceName: serviceName,
	}
}

func (p *TracedHelper) WithTransaction(tag string, fn func(tx *sqlx.Tx) error) error {
	ctx := context.Background()

	span := startup_tracing.CurrentSpan()
	if span == nil {
		ctx = opentracing.ContextWithSpan(ctx, span)
	}

	return startup_postgres.NewTransactionContext(ctx, p.DB,
		func(ctx context.Context, tx *sqlx.Tx) error { return fn(tx) })
}

func (p *TracedHelper) Traced(ctx context.Context, tag string, fn func(ctx context.Context) error) error {
	return startup_tracing.TraceChildContext(ctx, p.tracingServiceName+"-db", func(ctx context.Context, span opentracing.Span) error {
		span.SetTag("dd.service", p.tracingServiceName)
		span.SetTag("dd.resource", "tx:"+tag)
		return fn(ctx)
	})
}
