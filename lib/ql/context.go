package ql

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// An Action can be schedule to run after a commit or after a rollback of a transaction
// to execute some (infallible) side effects.
type Action func()

// Tx describes a simple transaction
type Tx interface {
	sqlx.ExecerContext
	sqlx.QueryerContext
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
}
