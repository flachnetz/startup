package startup_postgres

import (
	"github.com/jackc/pgconn"
	"net/url"
	"time"

	"github.com/flachnetz/startup/v2/startup_base"

	"database/sql"
	"fmt"
	"github.com/benbjohnson/clock"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"io"
	"sync"
)

type Initializer func(db *sqlx.DB) error

type PostgresOptions struct {
	URL      string `long:"postgres" default:"postgres://postgres:postgres@localhost:5432?sslmode=disable" description:"Postgres server url."`
	PoolSize int    `long:"postgres-pool" validate:"min=1" default:"8" description:"Maximum number of (idle) connections in the postgres connection pool."`

	ConnectionLifetime time.Duration `long:"postgres-lifetime" default:"10m" description:"Maximum time a connection in the pool can be used."`

	Inputs struct {
		// the driver name to use. If this is empty,
		// we select the default of 'postgres' or 'pgx',
		// depending on availability
		DriverName string

		// An optional initializer. This might be used to do
		// database migration or stuff.
		Initializer Initializer
	}

	connectionOnce sync.Once
	connection     *sqlx.DB
}

func (opts *PostgresOptions) Connection() *sqlx.DB {
	opts.connectionOnce.Do(func() {
		logger := logrus.WithField("prefix", "postgres")

		if pgUrl, err := url.Parse(opts.URL); err == nil {
			pgUrl.User = nil
			logger.Infof("Connecting to postgres database (username and password removed) at %s", pgUrl.String())

		}

		// check the driver name to use. We normally use the 'postgres' driver.
		// BUT: If tracing is enabled, we'll switch over to the 'pgx' driver, which is
		// the same postgres driver but with registered hooks.
		driverName := opts.Inputs.DriverName
		if driverName == "" {
			driverName = GuessDriverName()
		}

		logger.Debugf("Opening database using driver %s", driverName)

		db, err := sqlx.Connect(driverName, opts.URL)
		startup_base.PanicOnError(err, "Cannot connect to postgres")

		db.SetMaxOpenConns(opts.PoolSize)
		db.SetMaxIdleConns(opts.PoolSize)
		db.SetConnMaxLifetime(opts.ConnectionLifetime)

		if opts.Inputs.Initializer != nil {
			logger.Infof("Running database initializer")

			if err := opts.Inputs.Initializer(db); err != nil {
				// close database on error
				defer db.Close()
				startup_base.PanicOnError(err, "Database initialization failed")
			}
		}

		opts.connection = db
	})

	return opts.connection
}

func GuessDriverName() string {
	var pgx, postgres bool
	for _, driver := range sql.Drivers() {
		pgx = pgx || driver == "pgx"
		postgres = postgres || driver == "postgres"
	}

	if postgres {
		return "postgres"
	}

	if pgx {
		return "pgx"
	}

	panic(startup_base.Errorf("No postgres database driver found"))
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

type channelCloser chan bool

func (ch channelCloser) Close() error {
	close(ch)
	return nil
}

func ErrIsForeignKeyViolation(err error) bool {
	if err, ok := err.(*pgconn.PgError); ok {
		return err.Code == "23503"
	}

	return false
}

func ErrIsUniqueViolation(err error) bool {
	if err, ok := err.(*pgconn.PgError); ok {
		return err.Code == "23505"
	}

	return false
}
