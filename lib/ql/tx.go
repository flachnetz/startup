package ql

import (
	"context"
	"database/sql"

	sl "github.com/flachnetz/startup/v2/startup_logrus"
	pt "github.com/flachnetz/startup/v2/startup_postgres"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

var ErrNoTransaction = errors.New("no transaction in context")
var ErrTransactionExistInContext = errors.New("transaction exists in context")

type noRollbackTxError struct {
	wrapped error
}

func (n noRollbackTxError) Error() string {
	return n.wrapped.Error()
}

func (n noRollbackTxError) Unwrap() error {
	return n.wrapped
}

func NoRollback(err error) error {
	return noRollbackTxError{wrapped: err}
}

// InNewTransaction creates a new transaction and executes the given function within that transaction.
// The method will automatically roll back the transaction if an error is returned or otherwise commit it.
// This excludes the 'ErrNoRows' error. This error never triggers a rollback.
//
// This behaviour can be overwritten by wrapping the error into NoRollback. The transaction will
// be committed in spite of the error present.
//
// The caller can also trigger a rollback with no error present by simply calling Rollback on the transaction.
//
// If the context already contains a transaction then ErrTransactionExistInContext will be returned as
// error and the actual operation will not be executed.
func InNewTransaction[R any](ctx context.Context, db TxStarter, fun func(ctx TxContext) (R, error)) (R, error) {
	if tx := txContextFromContext(ctx); tx != nil {
		// must not have an existing transaction in context
		var defaultValue R
		return defaultValue, ErrTransactionExistInContext
	}

	var res resultWithErr[R]

	var hooks hooks
	var doCommit bool

	// TODO do transaction handling ourself (plus tracing)
	err := pt.WithTransactionContext(ctx, db, func(ctx context.Context, tx *sqlx.Tx) (bool, error) {
		// call the transaction operation
		result_, err := fun(newTxContext(ctx, tx, &hooks))
		res = resultWithErr[R]{result_, err}

		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// the error `ErrNoRows` will not trigger a rollback.
				return true, nil
			}

			var nrtxerr noRollbackTxError
			if errors.As(err, &nrtxerr) {
				// user does not want to rollback but still wants to return an error to the caller
				res.error = nrtxerr.wrapped
				doCommit = true
				return true, nil
			}
		}

		// default is rollback on error, otherwise commit.
		doCommit = err == nil

		return doCommit, nil
	})

	if doCommit && err == nil {
		// if we committed with no errors, we can run the commit hooks
		hooks.RunOnCommit()
	}

	return consolidateTransactionErrors(res, err)
}

func consolidateTransactionErrors[T any](res resultWithErr[T], err error) (T, error) {
	if err == nil {
		// we dont have an extra error, just return the original
		// application result as is.
		return res.result, res.error
	}

	if res.error != nil {
		// we also have a transactional level error, wrap the error
		return res.result, errors.WithMessagef(res.error, "transaction error (%s) with application error", err)
	}

	// if we have no application level error, just return the error
	// that occurred during commit or rollback.
	return res.result, err
}

type resultWithErr[R any] struct {
	result R
	error  error
}

// InExistingTransaction runs the given operation in the transaction that is hidden in the
// provided Context instance. If the context does not contain any transaction, ErrNoTransaction
// will be returned. The context must contain a transaction created by InNewTransaction.
//
// This function will rollback the transaction on error. See InNewTransaction regarding error handling.
func InExistingTransaction[R any](ctx context.Context, fun func(ctx TxContext) (R, error)) (R, error) {
	tx := txContextFromContext(ctx)
	if tx == nil {
		var defaultValue R
		return defaultValue, ErrNoTransaction
	}

	result, err := fun(tx)

	if errors.Is(err, sql.ErrNoRows) {
		// the error `ErrNoRows` will not trigger a rollback.
		return result, err
	}

	var noRollbackTxError noRollbackTxError
	if errors.As(err, &noRollbackTxError) {
		// user does not want to rollback but still wants to return an error to the caller
		return result, noRollbackTxError.wrapped
	}

	if err != nil {
		// execute rollback and update error
		if err := tx.Tx.Rollback(); err != nil {
			log := sl.GetLogger(ctx, InExistingTransaction[R])
			log.Warnf("Error during rollback: %s", err)
		}
	}

	return result, err
}

// InAnyTransaction checks the context for an existing transaction created by InNewTransaction.
// If a transaction exists it will run the given operation in the transaction context.
// If no transaction exists, a new transaction will be created.
//
// See InNewTransaction regarding error handling.
func InAnyTransaction[R any](ctx context.Context, db TxStarter, fun func(ctx TxContext) (R, error)) (R, error) {
	tx := txContextFromContext(ctx)
	if tx != nil {
		return InExistingTransaction[R](ctx, fun)
	} else {
		return InNewTransaction[R](ctx, db, fun)
	}
}

type TxStarter interface {
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
}
