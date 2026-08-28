package ql

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

// An Action can be schedule to run after a commit or after a rollback of a transaction
// to execute some (infallible) side effects.
type Action func()

// A FallibleAction runs while the transaction is still open, so it may touch the
// database and may fail. Returning an error rolls the transaction back.
type FallibleAction func(ctx TxContext) error

// Tx describes a simple transaction
type Tx interface {
	sqlx.ExtContext
}

type Hooks interface {
	// OnCommit schedules some side effect that is only run if the transaction
	// commits successfully. The Action is run after the transaction is committed and must
	// not access the database again.
	OnCommit(action Action)

	// BeforeCommit schedules work that runs inside the transaction just before it
	// is committed, so it may still write to the database. Use it to flush
	// buffered writes as one statement instead of one per call. A failing hook
	// rolls the transaction back. Hooks run in registration order, and a hook may
	// register further hooks.
	BeforeCommit(action FallibleAction)

	// OnDone schedules cleanup that runs once the transaction is finished, no
	// matter whether it committed, rolled back or panicked. Use it to release
	// state a BeforeCommit hook would otherwise leak on a rollback. The Action
	// must not access the database.
	OnDone(action Action)
}

type TxContext interface {
	context.Context
	Tx
	Hooks
	sqlx.PreparerContext

	// WithContext returns a new TxContext with the given "real" context.
	WithContext(ctx context.Context) TxContext

	// CommitAndChain performs a commit, runs all OnCommit hooks and creates a new transaction
	// using the postgres `COMMIT AND CHAIN` command.
	CommitAndChain() error
}

// WithTimeout is a wrapper around context.WithTimeout
func WithTimeout(parent TxContext, timeout time.Duration) (TxContext, context.CancelFunc) {
	newCtx, cancel := context.WithTimeout(parent, timeout)
	return parent.WithContext(newCtx), cancel
}

// WithDeadline is a wrapper around context.WithDeadline
func WithDeadline(parent TxContext, deadline time.Time) (TxContext, context.CancelFunc) {
	newCtx, cancel := context.WithDeadline(parent, deadline)
	return parent.WithContext(newCtx), cancel
}

// WithValue is a wrapper around context.WithValue
func WithValue(parent TxContext, key any, value any) TxContext {
	newCtx := context.WithValue(parent, key, value)
	return parent.WithContext(newCtx)
}

// WithCancel is a wrapper around context.WithCancel
func WithCancel(parent TxContext) (TxContext, context.CancelFunc) {
	newCtx, cancel := context.WithCancel(parent)
	return parent.WithContext(newCtx), cancel
}
