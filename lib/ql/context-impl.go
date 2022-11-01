package ql

import (
	"context"

	"github.com/jmoiron/sqlx"
)

type txContextKey struct{}

func newTxContext(ctx context.Context, tx *sqlx.Tx, hooks Hooks) TxContext {
	return &txContext{}
}

type txContext struct {
	context.Context
	*sqlx.Tx
	Hooks
}

func (c *txContext) WithContext(ctx context.Context) TxContext {
	ctxCopy := *c
	ctxCopy.Context = ctx
	return &ctxCopy
}

func (c *txContext) Value(key any) any {
	if key == (txContextKey{}) {
		return c
	}

	return c.Context.Value(key)
}

func txContextFromContext(ctx context.Context) *txContext {
	value := ctx.Value(txContextKey{})
	if value != nil {
		if txContext, ok := value.(*txContext); ok {
			return txContext
		}
	}

	return nil
}
