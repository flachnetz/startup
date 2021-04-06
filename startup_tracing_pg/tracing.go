package startup_tracing_pg

import (
	"context"
	"database/sql/driver"
	"github.com/gchaincl/sqlhooks/v2"
	"github.com/opentracing/opentracing-go"
	"runtime"
	"strings"
	"sync"

	pt "github.com/flachnetz/startup/v2/startup_postgres"
	"github.com/flachnetz/startup/v2/startup_tracing"
)

type PostgresTracingOptions struct {
	Inputs struct {
		// Extra skip function that skips methods (by name) when guessing the
		// transaction span tags.
		SkipFrameworkMethod func(name string) bool
	}

	once sync.Once
}

func (opts *PostgresTracingOptions) Initialize(tops *startup_tracing.TracingOptions) {
	opts.once.Do(func() {
		if tops.IsActive() {
			pt.Use(func(driver driver.Driver) driver.Driver {
				return sqlhooks.Wrap(driver, &dbHook{tops.Inputs.ServiceName + "-db"})
			})

			// replace the new transaction function with a new hook
			opts.installTransactionTracingHook(tops.Inputs.ServiceName + "-db")
		}
	})
}

func (opts *PostgresTracingOptions) installTransactionTracingHook(serviceName string) {
	withTransactionContext := pt.WithTransactionContext

	skipFunction := opts.Inputs.SkipFrameworkMethod
	if skipFunction == nil {
		skipFunction = func(name string) bool {
			return false
		}
	}

	pt.WithTransactionContext = func(ctx context.Context, db pt.TxStarter, operation pt.TransactionCommitFn) (err error) {
		var tag string

		// get the first method in the stack outside of the startup package
		pcSlice := [10]uintptr{}
		n := runtime.Callers(1, pcSlice[:])
		if n > 0 {
			frames := runtime.CallersFrames(pcSlice[:])
			for {
				frame, more := frames.Next()

				// take first one out of startup
				if !strings.Contains(frame.Function, "flachnetz/startup/") && !skipFunction(frame.Function) {
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
