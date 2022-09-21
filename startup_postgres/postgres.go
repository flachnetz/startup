package startup_postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/jackc/pgconn"
	"net/url"
	"strings"
	"time"

	"github.com/flachnetz/startup/v2/startup_base"

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

		pgUrl, err := url.Parse(opts.URL)
		if err == nil {
			pgUrl.User = nil
			logger.Infof("Connecting to postgres database (username and password removed) at %s", pgUrl.String())
		}

		connector, err := openConnector(opts.URL)
		startup_base.PanicOnError(err, "Failed to create a database connector")

		db := opts.mustConnect(connector)

		if pgUrl != nil {
			if schema := pgUrl.Query().Get("search_path"); schema != "" {
				logger.Infof("Ensure default schema %q exists", schema)
				_, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS ` + quoteIdentifier(schema))
				startup_base.PanicOnError(err, "Failed to create schema %q in database", schema)
			}
		}

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

// from pgx
func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
