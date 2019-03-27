package startup_tracing_pg

import (
	"context"
	"database/sql"
	"github.com/flachnetz/startup/startup_base"
	. "github.com/flachnetz/startup/startup_postgres"
	"github.com/flachnetz/startup/startup_tracing"
	"github.com/gchaincl/sqlhooks"
	"github.com/lib/pq"
	"github.com/opentracing/opentracing-go"
	"runtime"
	"strings"
	"sync"
)

type PostgresTracingOptions struct {
	once sync.Once
}

func (opts *PostgresTracingOptions) Initialize(tops *startup_tracing.TracingOptions) {
	opts.once.Do(func() {
		if tops.IsActive() {
			for _, driver := range sql.Drivers() {
				if driver == "pgx" {
					startup_base.Panicf("Cannot setup tracing: 'pgx' driver already registered.")
					return
				}
			}

			// Register a driver with hooks.
			// We need to use the pgx name here so that sqlx will use the right binding syntax.
			sql.Register("pgx", sqlhooks.Wrap(&pq.Driver{}, &dbHook{tops.Inputs.ServiceName + "-db"}))

			// replace the new transaction function with a new hook
			installTransactionTracingHook(tops.Inputs.ServiceName + "-db")
		}
	})
}

func installTransactionTracingHook(serviceName string) {
	withTransactionContext := WithTransactionContext

	WithTransactionContext = func(ctx context.Context, db TxStarter, operation TransactionFn) (err error) {
		var tag string

		// get the first method in the stack outside of the startup package
		pcSlice := [10]uintptr{}
		n := runtime.Callers(1, pcSlice[:])
		if n > 0 {
			frames := runtime.CallersFrames(pcSlice[:])
			for {
				frame, more := frames.Next()

				// take first one out of startup
				if !strings.Contains(frame.Function, "flachnetz/startup/") {
					tag = frame.Function
					break
				}

				if !more {
					break
				}
			}
		}

		if tag == "" {
			tag = "transaction"
		}

		return startup_tracing.TraceChildContext(ctx, tag, func(ctx context.Context, span opentracing.Span) error {
			span.SetTag("dd.service", serviceName)
			span.SetTag("dd.resource", "tx:"+tag)
			return withTransactionContext(ctx, db, operation)
		})
	}
}
