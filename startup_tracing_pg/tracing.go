package startup_tracing_pg

import (
	"sync"

	pt "github.com/flachnetz/startup/v2/startup_postgres"
	"github.com/flachnetz/startup/v2/startup_tracing"
)

type PostgresTracingOptions struct {
	Inputs struct {
		// Extra skip function that skips methods (by name) when guessing the
		// transaction span tags.
		SkipFrameworkMethod SkipFunc
	}

	once sync.Once
}

func (opts *PostgresTracingOptions) Initialize(tops *startup_tracing.TracingOptions) {
	opts.once.Do(func() {
		if tops.IsActive() {
			skipFunction := opts.Inputs.SkipFrameworkMethod
			if skipFunction == nil {
				skipFunction = func(name string) bool { return false }
			}

			pt.InstallTracer(&tracer{
				ServiceName:         tops.Inputs.ServiceName + "-db",
				SkipFrameworkMethod: skipFunction,
			})
		}
	})
}

//func (opts *PostgresTracingOptions) installTransactionTracingHook(serviceName string) {
//	withTransactionContext := pt.WithTransactionContext
//
//	skipFunction := opts.Inputs.SkipFrameworkMethod
//	if skipFunction == nil {
//		skipFunction = func(name string) bool {
//			return false
//		}
//	}
//
//	pt.WithTransactionContext = func(ctx context.Context, db pt.TxStarter, operation pt.TransactionCommitFn) (err error) {
//		tag := findOutsideCaller(skipFunction)
//
//		if tag == "" {
//			tag = "transaction"
//		}
//
//		_, err = startup_tracing.Trace(ctx, tag, func(ctx context.Context, span opentracing.Span) (any, error) {
//			span.SetTag("dd.service", serviceName)
//			span.SetTag("dd.resource", "tx:"+tag)
//			return nil, withTransactionContext(ctx, db, operation)
//		})
//
//		return err
//	}
//}
