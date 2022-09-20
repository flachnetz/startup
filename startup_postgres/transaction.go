package startup_postgres

import (
	"context"
	"database/sql"
	sl "github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
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

// WithTransaction Ends the given transaction. This method will either commit the transaction if
// the given recoverValue is nil, or rollback the transaction if it is non nil.
//
// Deprecated: Use a variant of this function that includes a context argument.
func WithTransaction(db TxStarter, fn func(tx *sqlx.Tx) error) (err error) {
	return WithTransactionAutoCommitContext(context.Background(), db, func(ctx context.Context, tx *sqlx.Tx) error {
		return fn(tx)
	})
}

type noRollbackTxError struct {
	wrapped error
}

func (n noRollbackTxError) Error() string {
	return n.wrapped.Error()
}

func NoRollback(err error) error {
	return noRollbackTxError{wrapped: err}
}

var ErrTransactionExistInContext = errors.New("transaction already exists in context")

// ExecuteInNewTransaction creates a new transaction and executes the given function within that transaction.
// The method will automatically rollback the transaction if no error is returned or otherwise commit it.
// This excludes the 'ErrNoRows' error. This error never triggers a rollback.
//
// This behaviour can be overwritten by wrapping the error into NoRollback. The transaction will
// be committed in spite of the error present.
//
// The caller can also trigger a rollback with no error present by simply calling Rollback on the transaction.
//
// If the context already contains a transaction then ErrTransactionExistInContext will be returned as
// error and the actual operation will not be executed
//
func ExecuteInNewTransaction[R any](ctx context.Context, db TxStarter, fun func(context.Context, *sqlx.Tx) (R, error)) (R, error) {
	if tx := TransactionFromContext(ctx); tx != nil {
		// must not have an existing transaction in context
		var defaultValue R
		return defaultValue, ErrTransactionExistInContext
	}

	var res resultWithErr[R]

	err := WithTransactionContext(ctx, db, func(ctx context.Context, tx *sqlx.Tx) (bool, error) {
		// call the transaction operation
		_result, err := fun(ctx, tx)
		res = resultWithErr[R]{_result, err}

		if err != nil {
			if errors.Cause(err) == sql.ErrNoRows {
				// the error `ErrNoRows` will not trigger a rollback.
				return true, nil
			}

			if err, ok := err.(noRollbackTxError); ok {
				// user does not want to rollback but still wants to return an error to the caller
				res.error = err.wrapped
				return true, nil
			}
		}

		// default is rollback on error, otherwise commit.
		return err == nil, nil
	})

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

// ExecuteInExistingTransaction runs the given operation in the transaction that is hidden in the
// provided Context instance. If the context does not contain any transaction, ErrNoTransaction
// will be returned.
//
// See ExecuteInNewTransaction regarding error handling.
//
func ExecuteInExistingTransaction[R any](ctx context.Context, fun func(context.Context, *sqlx.Tx) (R, error)) (R, error) {
	tx := TransactionFromContext(ctx)
	if tx != nil {
		result, err := fun(ctx, tx)

		if errors.Cause(err) == sql.ErrNoRows {
			// the error `ErrNoRows` will not trigger a rollback.
			return result, err
		}

		if err, ok := err.(noRollbackTxError); ok {
			// user does not want to rollback but still wants to return an error to the caller
			return result, err.wrapped
		}

		if err != nil {
			// execute rollback and update error
			if err := tx.Rollback(); err != nil {
				log := sl.GetLogger(ctx, ExecuteInExistingTransaction[R])
				log.Warnf("Error during rollback: %s", err)
			}
		}

		return result, err

	} else {
		var defaultValue R
		return defaultValue, ErrNoTransaction
	}
}

// ExecuteInAnyTransaction checks the context for an existing transaction. If a transaction
// exists it will run the given operation in the transaction context. If no transaction exists,
// a new transaction will be created.
//
// See ExecuteInNewTransaction regarding error handling.
//
func ExecuteInAnyTransaction[R any](ctx context.Context, db TxStarter, fun func(context.Context, *sqlx.Tx) (R, error)) (R, error) {
	tx := TransactionFromContext(ctx)
	if tx != nil {
		return ExecuteInExistingTransaction[R](ctx, fun)
	} else {
		return ExecuteInNewTransaction[R](ctx, db, fun)
	}
}

// GetContext runs the given query and parses the result into an object of type T.
// If not object can be found the method will return sql.ErrNoRows and a default value
//
func GetContext[T any](ctx context.Context, tx sqlx.QueryerContext, query string, args ...interface{}) (T, error) {
	var resultValue T

	err := sqlx.GetContext(ctx, tx, &resultValue, query, args...)
	return resultValue, err
}

// GetContextOrNull runs the given query and parses the result into an object of type T.
// If not object can be found the method will return sql.ErrNoRows and a value of nil.
//
func GetContextOrNull[T any](ctx context.Context, tx sqlx.QueryerContext, query string, args ...interface{}) (*T, error) {
	resultValue, err := GetContext[T](ctx, tx, query, args...)
	if err != nil {
		return nil, err
	}

	return &resultValue, nil
}
