package startup_tracing_pg

import (
	"context"
	"database/sql/driver"
	"runtime"
	"strings"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/qustavo/sqlhooks/v2"

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
				hooks := &dbHook{tops.Inputs.ServiceName + "-db"}
				return wrap(driver, hooks)
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

		_, err = startup_tracing.Trace(ctx, tag, func(ctx context.Context, span opentracing.Span) (any, error) {
			span.SetTag("dd.service", serviceName)
			span.SetTag("dd.resource", "tx:"+tag)
			return nil, withTransactionContext(ctx, db, operation)
		})

		return err
	}
}

type extraWrapDriver struct {
	delegate *sqlhooks.Driver
}

func wrap(driver driver.Driver, hooks sqlhooks.Hooks) driver.Driver {
	wrapped := sqlhooks.Wrap(driver, hooks).(*sqlhooks.Driver)
	return &extraWrapDriver{wrapped}
}

func (e extraWrapDriver) Open(name string) (driver.Conn, error) {
	conn, err := e.delegate.Open(name)
	if err != nil {
		return nil, err
	}

	// A pgx connection implements 'driver.NamedValueChecker' ... The sqlhooks packages does not.
	// Because of that we forward the implementation of the actual connection wrapped by sqlhooks.
	// We dont do this completely generic, we only do it for the common case, as we pretty much
	// expect the given implementation here.
	if conn, ok := conn.(*sqlhooks.ExecerQueryerContextWithSessionResetter); ok {
		if checker, ok := conn.Conn.Conn.(driver.NamedValueChecker); ok {
			return &connWithNamedValueChecker{conn, checker}, nil
		}
	} else {
		_ = conn.Close()

		// this is unexpected and should be fixed in the implementation.
		panic(errors.Errorf("unexpected return value by sqlhooks, please check implementation"))
	}

	return conn, nil
}

type connWithNamedValueChecker struct {
	*sqlhooks.ExecerQueryerContextWithSessionResetter
	driver.NamedValueChecker
}
