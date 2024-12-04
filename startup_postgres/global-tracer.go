package startup_postgres

import (
	"context"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
)

type Tracer interface {
	pgx.QueryTracer
	pgx.PrepareTracer
	pgx.ConnectTracer

	TransactionStart(ctx context.Context) context.Context
	TransactionEnd(ctx context.Context)

	AcquireConnectionStart(ctx context.Context) context.Context
	AcquireConnectionEnd(ctx context.Context)
}

var globalTracer atomic.Pointer[Tracer]

func InstallTracer(tracer Tracer) {
	globalTracer.Store(&tracer)
}

func GetTracer() Tracer {
	if tracer := globalTracer.Load(); tracer != nil {
		return *tracer
	}

	return nil
}
