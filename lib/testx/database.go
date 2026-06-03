package testx

import (
	"testing"

	"github.com/flachnetz/pgtest/v2"
	"github.com/flachnetz/startup/v2/startup_postgres"
	"github.com/jmoiron/sqlx"
)

// NewConnection creates a temporary postgres database and applies schema files from
// the ./sql directory.
func NewConnection(t *testing.T, schemaTable string) *sqlx.DB {
	t.Helper()

	// connect to a temporary postgres database
	db := sqlx.NewDb(pgtest.Connect(t), "pgx")

	// run the migration scripts
	err := startup_postgres.DefaultMigration(schemaTable)(db)
	if err != nil {
		t.Fatal(err)
	}

	return db
}
