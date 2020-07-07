package startup_postgres

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var ErrNoTransaction = errors.New("no transaction in context")

type TransactionCommitFn func(ctx context.Context, tx *sqlx.Tx) (commit bool, err error)
type TransactionFn func(ctx context.Context, tx *sqlx.Tx) error

type transactionKey struct{}

// Gets the current transaction from the context or nil, if the context
// does not contain a transaction.
func TransactionFromContext(ctx context.Context) *sqlx.Tx {
	txValue := ctx.Value(transactionKey{})
	if txValue == nil {
		return nil
	}

	return txValue.(*sqlx.Tx)
}

// Puts a transaction into a context and returns
// the new context with the transaction
func ContextWithTransaction(ctx context.Context, tx *sqlx.Tx) context.Context {
	return context.WithValue(ctx, transactionKey{}, tx)
}

// Calls the given operation with the transaction that is
// currently stored in the context. Will fail with ErrNoTransaction if there
// is no transaction stored in the context.
func WithTransactionFromContext(ctx context.Context, operation func(tx *sqlx.Tx) error) error {
	tx := TransactionFromContext(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	return operation(tx)
}

func WithTransactionAutoCommitContext(ctx context.Context, db TxStarter, operation TransactionFn) (err error) {
	return WithTransactionContext(ctx, db, func(ctx context.Context, tx *sqlx.Tx) (commit bool, err error) {
		return true, operation(ctx, tx)
	})
}

// Creates a new transaction, puts it into the context and runs the given operation
// with the context and the transaction. This is a var so that you are able to replace
// it with your own function to enable tracing during initialization of your application.
var WithTransactionContext = func(ctx context.Context, db TxStarter, operation TransactionCommitFn) (err error) {
	var tx *sqlx.Tx
	var commit bool

	// begin a new transaction
	tx, err = db.BeginTxx(ctx, nil)
	if err != nil {
		return errors.WithMessage(err, "begin transaction")
	}

	defer func() {
		r := recover()

		if r == nil && err == nil && commit {
			// commit the transaction
			if err = tx.Commit(); err != nil {
				err = errors.WithMessage(err, "commit")
			}

		} else {
			if err := tx.Rollback(); err != nil {
				logrus.Warnf("Could not rollback transaction: %s", err)
			}

			// convert recovered value into an error instance
			if r != nil {
				var ok bool
				if err, ok = r.(error); !ok {
					err = fmt.Errorf("panic(%#v)", r)
				}
			}

			// and give context to the error
			err = errors.WithMessage(err, "transaction")
		}
	}()

	commit, err = operation(ContextWithTransaction(ctx, tx), tx)

	return
}
