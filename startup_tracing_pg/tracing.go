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
