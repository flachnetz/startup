package startup_postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"

	"github.com/benbjohnson/clock"
	pgxstd "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

type Initializer func(db *sqlx.DB) error

type PostgresOptions struct {
	URL                string `long:"postgres" default:"postgres://postgres:postgres@localhost:5432?sslmode=disable" description:"Postgres server url."`
	PoolSize           int    `long:"postgres-pool" validate:"min=1" default:"8" description:"Maximum number of (idle) connections in the postgres connection pool."`
	EnableQueryLogging bool   `long:"enable-query-logging" description:"Enable query logging."`

	ConnectionLifetime time.Duration `long:"postgres-lifetime" default:"10m" description:"Maximum time a connection in the pool can be used."`

	Inputs struct {
		// An optional initializer. This might be used to do
		// database migration or stuff.
		Initializer Initializer
	}

	connectionOnce sync.Once
	connection     *sqlx.DB
}

func (opts *PostgresOptions) Connection() *sqlx.DB {
	opts.connectionOnce.Do(func() {
		ctx, cancelTimeout := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelTimeout()

		logger := slog.With(slog.String("prefix", "postgres"))

		conf, err := pgx.ParseConfig(opts.URL)
		startup_base.PanicOnError(err, "Failed to parse database connection")

		if err == nil {
			logger.Info("Connecting to postgres database",
				slog.String("user", conf.User),
				slog.String("host", conf.Host),
				slog.Int("port", int(conf.Port)),
				slog.String("database", conf.Database),
			)
		}

		conf.Tracer = tracerWrapper{}
		if opts.EnableQueryLogging {
			conf.Tracer = tracerWrapper{logger: logger}
		}

		// create the new database connection
		db := sqlx.NewDb(pgxstd.OpenDB(*conf), "pgx")

		// configure pool
		db.SetMaxOpenConns(opts.PoolSize)
		db.SetMaxIdleConns(opts.PoolSize)
		db.SetConnMaxLifetime(opts.ConnectionLifetime)

		// check the connection
		err = db.PingContext(ctx)
		startup_base.PanicOnError(err, "Failed to ping database")

		// create schema if needed
		if schema := conf.RuntimeParams["search_path"]; schema != "" {
			logger.Info("Ensure default schema exists", slog.String("schema", schema))
			_, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS ` + quoteIdentifier(schema))
			startup_base.PanicOnError(err, "Failed to create schema %q in database", schema)
		}

		if opts.Inputs.Initializer != nil {
			logger.Info("Running database initializer")

			if err := opts.Inputs.Initializer(db); err != nil {
				// close database on error
				defer startup_base.Close(db, "Close database after error")
				startup_base.PanicOnError(err, "Database initialization failed")
			}
		}

		observeStats(db)

		opts.connection = db
	})

	return opts.connection
}

func observeStats(db *sqlx.DB) {
	m := otel.Meter("db.pool")

	idle, _ := m.Int64ObservableGauge("db.pool.idle")
	inuse, _ := m.Int64ObservableGauge("db.pool.inuse")
	open, _ := m.Int64ObservableGauge("db.pool.open")

	waitCount, _ := m.Int64ObservableCounter("db.pool.wait_count")
	waitDuration, _ := m.Int64ObservableGauge("db.pool.wait_duration_ms")

	closedLifetime, _ := m.Int64ObservableCounter("db.pool.closed.lifetime")
	closedIdletime, _ := m.Int64ObservableCounter("db.pool.closed.idletime")
	closedIdle, _ := m.Int64ObservableCounter("db.pool.closed.idle")

	_, _ = m.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			stats := db.Stats()

			o.ObserveInt64(idle, int64(stats.Idle))
			o.ObserveInt64(inuse, int64(stats.InUse))
			o.ObserveInt64(open, int64(stats.OpenConnections))

			o.ObserveInt64(waitCount, stats.WaitCount)
			o.ObserveInt64(waitDuration, stats.WaitDuration.Milliseconds())

			o.ObserveInt64(closedLifetime, stats.MaxLifetimeClosed)
			o.ObserveInt64(closedIdletime, stats.MaxIdleTimeClosed)
			o.ObserveInt64(closedIdle, stats.MaxIdleClosed)

			return nil
		},
		idle, inuse, open, waitCount, waitDuration, closedLifetime, closedIdletime, closedIdle,
	)
}

func (opts *PostgresOptions) StartVacuumTask(db *sqlx.DB, table string, interval time.Duration, clock clock.Clock) io.Closer {
	if interval < 1*time.Second {
		interval = 1 * time.Second
	}

	closeCh := make(chan bool)

	go func() {
		l := slog.With(slog.String("prefix", "vacuum"))

		for {
			select {
			case <-closeCh:
				return

			case <-clock.After(interval):
				l.Info("Running periodic vacuum on table now", slog.String("table", table))

				if _, err := db.Exec(fmt.Sprintf(`VACUUM "%s"`, table)); err != nil {
					l.Warn("Maintenance task failed", slog.String("error", err.Error()))
				}
			}
		}
	}()

	return channelCloser(closeCh)
}

//lint:ignore U1000
func (opts *PostgresOptions) mustConnect(connector driver.Connector) *sqlx.DB {
	ctx, cancelTimeout := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTimeout()

	db := sqlx.NewDb(sql.OpenDB(connector), "pgx")
	db.SetMaxOpenConns(opts.PoolSize)
	db.SetMaxIdleConns(opts.PoolSize)
	db.SetConnMaxLifetime(opts.ConnectionLifetime)

	if err := db.PingContext(ctx); err != nil {
		startup_base.FatalOnError(err, "Cannot connect to database.")
	}

	return db
}

type channelCloser chan bool

func (ch channelCloser) Close() error {
	close(ch)
	return nil
}

func ErrIsForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23503"
	}

	return false
}

func ErrIsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}

	return false
}

// from pgx
func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
