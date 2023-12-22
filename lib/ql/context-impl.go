package ql

import (
	"context"

	"github.com/jmoiron/sqlx"
)

type txContextKey struct{}

func newTxContext(ctx context.Context, tx *sqlx.Tx, hooks *hooks) TxContext {
	return &txContext{ctx, tx, hooks}
}

type txContext struct {
	context.Context
	*sqlx.Tx
	*hooks
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

func (c *txContext) CommitAndChain() error {
	if err := Exec(c, "COMMIT AND CHAIN"); err != nil {
		return err
	}

	c.hooks.RunOnCommit()

	return nil
}

func TxContextFromContext(ctx context.Context) TxContext {
	value := ctx.Value(txContextKey{})
	if value != nil {
		if txContext, ok := value.(*txContext); ok {
			return txContext
		}
	}

	return nil
}

func TryTxFromContext(ctx TxContext) *sqlx.Tx {
	if ctx, ok := ctx.(*txContext); ok {
		return ctx.Tx
	}

	return nil
}
