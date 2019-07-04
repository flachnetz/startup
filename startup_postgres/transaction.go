package startup_postgres

import (
	"context"
	"database/sql"
	"github.com/jmoiron/sqlx"
)

type TxStarter interface {
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
}

type TxHelper struct {
	*sqlx.DB
}

func NewTxHelper(db *sqlx.DB) TxHelper {
	return TxHelper{
		DB: db,
	}
}

// Deprecated: Stop using this and start using the WithTransaction
// function that includes the context argument
func (h *TxHelper) WithTransaction(fn func(tx *sqlx.Tx) error) (err error) {
	return WithTransaction(h.DB, fn)
}

func (h *TxHelper) WithTransactionContext(ctx context.Context, operation TransactionCommitFn) error {
	return WithTransactionContext(ctx, h.DB, operation)
}

func (h *TxHelper) WithTransactionContextAutoCommit(ctx context.Context, operation TransactionFn) error {
	return WithTransactionAutoCommitContext(ctx, h.DB, operation)
}

// Ends the given transaction. This method will either commit the transaction if
// the given recoverValue is nil, or rollback the transaction if it is non nil.
//
// Deprecated: Use a variant of this function that includes a context argument.
//
func WithTransaction(db TxStarter, fn func(tx *sqlx.Tx) error) (err error) {
	return WithTransactionAutoCommitContext(context.Background(), db, func(ctx context.Context, tx *sqlx.Tx) error {
		return fn(tx)
	})
}
