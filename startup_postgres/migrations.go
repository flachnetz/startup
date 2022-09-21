package startup_postgres

import (
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/rubenv/sql-migrate"
	"github.com/sirupsen/logrus"
	"os"
)

// Migration Runs a migration with the sql files from the given directory.
// The directory must exist. The migration library will use the given table
// name to store the migration progress
func Migration(table, directory string) Initializer {
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		startup_base.PanicOnError(err, "No database migration files found")
	}

	return func(db *sqlx.DB) error {
		migrate.SetTable(table)

		migrations := &migrate.FileMigrationSource{Dir: directory}
		n, err := migrate.Exec(db.DB, "postgres", migrations, migrate.Up)
		if err != nil {
			return errors.WithMessage(err, "applying database migration")
		}

		logrus.WithField("prefix", "database").
			Infof("%d migrations executed", n)

		return nil
	}
}

// Creates an Initializer that performs a database migration by looking for
// sql files in the default directories.
func DefaultMigration(table string) Initializer {
	return Migration(table, guessMigrationDirectory())
}

func guessMigrationDirectory() string {
	names := []string{"sql", "../sql", "../../sql", "../../../sql", "../../../../sql"}
	for _, name := range names {
		if _, err := os.Stat(name); os.IsNotExist(err) {
			continue
		}

		return name
	}

	panic(startup_base.Errorf("No sql directory found not found"))
}
