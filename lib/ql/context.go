package ql

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

// An Action can be schedule to run after a commit or after a rollback of a transaction
// to execute some (infallible) side effects.
type Action func()

// Tx describes a simple transaction
type Tx interface {
	sqlx.ExtContext
}

type Hooks interface {
	// OnCommit schedules some side effect that is only run if the transaction
	// commits successfully. The Action is run after the transaction is committed and must
	// not access the database again.
	OnCommit(action Action)
}

type TxContext interface {
	context.Context
	Tx
	Hooks

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
