package startup_postgres

import (
	"context"
	"database/sql"
	"github.com/jmoiron/sqlx"
)

type BeginTxer interface {
	Beginx() (*sqlx.Tx, error)
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
}

type Helper struct {
	*sqlx.DB
}

func New(db *sqlx.DB) Helper {
	return Helper{
		DB: db,
	}
}

func (h *Helper) WithTransaction(fn func(tx *sqlx.Tx) error) (err error) {
	return WithTransaction(h.DB, fn)
}

// Ends the given transaction. This method will either commit the transaction if
// the given recoverValue is nil, or rollback the transaction if it is non nil.
func WithTransaction(db BeginTxer, fn func(tx *sqlx.Tx) error) (err error) {
	return NewTransactionContext(context.Background(), db, func(ctx context.Context, tx *sqlx.Tx) error {
		return fn(tx)
	})
}
