package startup_tracing_pg

import (
	"database/sql"
	"github.com/flachnetz/startup/startup_base"
	"github.com/flachnetz/startup/startup_tracing"
	"github.com/gchaincl/sqlhooks"
	"github.com/lib/pq"
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
		}
	})
}
