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

	"github.com/rcrowley/go-metrics"

	"github.com/sirupsen/logrus"

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

		logger := logrus.WithField("prefix", "postgres")

		conf, err := pgx.ParseConfig(opts.URL)
		startup_base.PanicOnError(err, "Failed to parse database connection")

		if err == nil {
			logger.Infof(
				"Connecting to postgres database at %s@%s:%d/%s",
				conf.User, conf.Host, conf.Port, conf.Database,
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
			logger.Infof("Ensure default schema %q exists", schema)
			_, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS ` + quoteIdentifier(schema))
			startup_base.PanicOnError(err, "Failed to create schema %q in database", schema)
		}

		if opts.Inputs.Initializer != nil {
			logger.Infof("Running database initializer")

			if err := opts.Inputs.Initializer(db); err != nil {
				// close database on error
				defer startup_base.Close(db, "Close database after error")
				startup_base.PanicOnError(err, "Database initialization failed")
			}
		}

		go observeStats(db)

		opts.connection = db
	})

	return opts.connection
}

func observeStats(db *sqlx.DB) {
	go func() {
		prevStats := db.Stats()

		for {
			time.Sleep(1 * time.Second)

			stats := db.Stats()

			reg := metrics.DefaultRegistry
			metrics.GetOrRegisterGauge("db.pool.idle", reg).Update(int64(stats.Idle))
			metrics.GetOrRegisterGauge("db.pool.inuse", reg).Update(int64(stats.InUse))
			metrics.GetOrRegisterGauge("db.pool.open", reg).Update(int64(stats.OpenConnections))

			metrics.GetOrRegisterGauge("db.pool.gauge.wait-count", reg).Update(stats.WaitCount)
			metrics.GetOrRegisterGauge("db.pool.gauge.wait-duration", reg).Update(stats.WaitDuration.Milliseconds())

			metrics.GetOrRegisterMeter("db.pool.meter.wait-count", reg).Mark(stats.WaitCount - prevStats.WaitCount)
			metrics.GetOrRegisterTimer("db.pool.meter.wait-duration", reg).Update(stats.WaitDuration - prevStats.WaitDuration)

			metrics.GetOrRegisterGauge("db.pool.closed.lifetime", reg).Update(stats.MaxLifetimeClosed)
			metrics.GetOrRegisterGauge("db.pool.closed.idletime", reg).Update(stats.MaxIdleTimeClosed)
			metrics.GetOrRegisterGauge("db.pool.closed.idle", reg).Update(stats.MaxIdleClosed)

			prevStats = stats
		}
	}()
}

func (opts *PostgresOptions) StartVacuumTask(db *sqlx.DB, table string, interval time.Duration, clock clock.Clock) io.Closer {
	if interval < 1*time.Second {
		interval = 1 * time.Second
	}

	closeCh := make(chan bool)

	go func() {
		l := logrus.WithField("prefix", "vacuum")

		for {
			select {
			case <-closeCh:
				return

			case <-clock.After(interval):
				l.Infof("Running periodic vacuum on table %s now", table)

				if _, err := db.Exec(fmt.Sprintf(`VACUUM "%s"`, table)); err != nil {
					l.Warnf("Maintenance task failed: %s", err)
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
