package ql

import (
	"context"
	"database/sql"
	"github.com/hashicorp/go-multierror"

	sl "github.com/flachnetz/startup/v2/startup_logrus"
	pt "github.com/flachnetz/startup/v2/startup_postgres"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

var (
	ErrNoTransaction             = errors.New("no transaction in context")
	ErrTransactionExistInContext = errors.New("transaction exists in context")
)

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

// InNewTransaction calls InNewTransactionWithResult without returning a result
func InNewTransaction(ctx context.Context, db TxStarter, fun func(ctx TxContext) error) error {
	_, err := InNewTransactionWithResult(ctx, db, func(ctx TxContext) (any, error) {
		return nil, fun(ctx)
	})
	return err
}

// InNewTransactionWithResult creates a new transaction and executes the given function within that transaction.
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
func InNewTransactionWithResult[R any](ctx context.Context, db TxStarter, fun func(ctx TxContext) (R, error)) (R, error) {
	if tx := TxContextFromContext(ctx); tx != nil {
		// must not have an existing transaction in context
		var defaultValue R
		return defaultValue, ErrTransactionExistInContext
	}

	// tracing this transaction
	ctx = startTraceTransaction(ctx)
	defer endTraceTransaction(ctx)

	var hooks hooks

	// begin the transaction
	tx, closeConn, err := beginTx(ctx, db)
	if err != nil {
		var defaultValue R
		return defaultValue, err
	}

	defer closeConn()

	// set to true once the users code ran
	var userCodeOk bool
	defer func() {
		if !userCodeOk {
			// There was a panic in the users code.
			// We need to rollback the transaction now.
			if err := tx.Rollback(); err != nil {
				// If the rollback failed, there isnt much we can do except logging
				// the issue.
				sl.GetLogger(ctx, InNewTransactionWithResult[R]).Warnf("Rollback during panic failed: %s", err)
			}
		}
	}()

	// run the users transaction code
	res, err := fun(newTxContext(ctx, tx, &hooks))

	// if we panic now, we dont do anything
	userCodeOk = true

	// check if the user wants to rollback
	err, rollback := requiresTxRollback(err)

	if rollback {
		// we need to perform a rollback
		rerr := tx.Rollback()
		if rerr != nil && !errors.Is(rerr, sql.ErrTxDone) {
			err = rollbackError{err: err, rerr: rerr}
		}

		return res, err
	}

	// everything is fine, customer wants to commit
	cerr := tx.Commit()
	if cerr != nil && !errors.Is(cerr, sql.ErrTxDone) {
		err = commitError{err: err, cerr: cerr}
	}

	// if we committed with no errors, we can run the commit hooks
	if cerr == nil {
		hooks.RunOnCommit()
	}

	return res, err
}

func noop() {}

func acquireConnection(ctx context.Context, db *sqlx.DB) (*sqlx.Conn, error) {
	ctx = startTraceAcquireConnection(ctx)
	defer endTraceAcquireConnection(ctx)

	return db.Connx(ctx)
}

func beginTx(ctx context.Context, txStarter TxStarter) (*sqlx.Tx, func(), error) {
	switch pool := txStarter.(type) {
	case *sqlx.DB:
		// get the connection from the pool
		conn, err := acquireConnection(ctx, pool)
		if err != nil {
			return nil, nil, errors.WithMessage(err, "get connection from pool")
		}

		// and begin a connection on this trace
		tx, err := conn.BeginTxx(ctx, nil)
		if err != nil {
			// we still own the connection, so we need to close it
			if errClose := conn.Close(); errClose != nil {
				err = multierror.Append(err, errClose)
			}

			return nil, nil, errors.WithMessage(err, "start transaction")
		}

		closeConn := func() { _ = conn.Close() }
		return tx, closeConn, nil

	default:
		// probably already a sqlx.Conn, so we can just use it
		tx, err := txStarter.BeginTxx(ctx, nil)
		return tx, noop, err
	}
}

func requiresTxRollback(err error) (error, bool) {
	// no rollback required if we just didnt find anything
	if errors.Is(err, sql.ErrNoRows) {
		return err, false
	}

	// user does not want to rollback but still wants
	// to return an error to the caller
	var nrtxerr noRollbackTxError
	if errors.As(err, &nrtxerr) {
		return nrtxerr.wrapped, false
	}

	return err, err != nil
}

func startTraceTransaction(ctx context.Context) context.Context {
	tracer := pt.GetTracer()
	if tracer == nil {
		return ctx
	}

	return tracer.TransactionStart(ctx)
}

func endTraceTransaction(ctx context.Context) {
	tracer := pt.GetTracer()
	if tracer == nil {
		return
	}

	tracer.TransactionEnd(ctx)
}

func startTraceAcquireConnection(ctx context.Context) context.Context {
	tracer := pt.GetTracer()
	if tracer == nil {
		return ctx
	}

	return tracer.AcquireConnectionStart(ctx)
}

func endTraceAcquireConnection(ctx context.Context) {
	tracer := pt.GetTracer()
	if tracer == nil {
		return
	}

	tracer.AcquireConnectionEnd(ctx)
}

// InExistingTransaction calls InExistingTransactionWithResult without returning the error.
func InExistingTransaction(ctx context.Context, fun func(ctx TxContext) error) error {
	_, err := InExistingTransactionWithResult(ctx, func(ctx TxContext) (any, error) {
		return nil, fun(ctx)
	})
	return err
}

// InExistingTransactionWithResult runs the given operation in the transaction that is hidden in the
// provided Context instance. If the context does not contain any transaction, ErrNoTransaction
// will be returned. The context must contain a transaction created by InNewTransactionWithResult.
//
// This function will not rollback the transaction on error.
func InExistingTransactionWithResult[R any](ctx context.Context, fun func(ctx TxContext) (R, error)) (R, error) {
	tx := TxContextFromContext(ctx)
	if tx == nil {
		var defaultValue R
		return defaultValue, ErrNoTransaction
	}

	return fun(tx)
}

// InAnyTransaction calls InAnyTransactionWithResult without returning a result.
func InAnyTransaction(ctx context.Context, db TxStarter, fun func(ctx TxContext) error) error {
	_, err := InAnyTransactionWithResult(ctx, db, func(ctx TxContext) (any, error) {
		return nil, fun(ctx)
	})
	return err
}

// InAnyTransactionWithResult checks the context for an existing transaction created by InNewTransactionWithResult.
// If a transaction exists it will run the given operation in the transaction context.
// If no transaction exists, a new transaction will be created.
//
// See InNewTransactionWithResult regarding error handling.
func InAnyTransactionWithResult[R any](ctx context.Context, db TxStarter, fun func(ctx TxContext) (R, error)) (R, error) {
	tx := TxContextFromContext(ctx)
	if tx != nil {
		return InExistingTransactionWithResult[R](ctx, fun)
	} else {
		return InNewTransactionWithResult[R](ctx, db, fun)
	}
}

type TxStarter interface {
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
}
