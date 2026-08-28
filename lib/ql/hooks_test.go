package ql_test

import (
	"errors"
	"testing"

	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/flachnetz/startup/v2/lib/testx"
	"github.com/stretchr/testify/require"
)

func TestBeforeCommitWritesWithinTheTransaction(t *testing.T) {
	db := testx.NewConnection(t, "ql_hooks_migrations")

	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		return ql.Exec(ctx, `CREATE TABLE IF NOT EXISTS hooks_probe (value TEXT NOT NULL)`)
	})

	var order []string

	testx.MustTransact(t, db, func(ctx ql.TxContext) {
		ctx.OnCommit(func() { order = append(order, "commit") })
		ctx.OnDone(func() { order = append(order, "done") })
		ctx.BeforeCommit(func(ctx ql.TxContext) error {
			order = append(order, "before")
			return ql.Exec(ctx, `INSERT INTO hooks_probe (value) VALUES ($1)`, "flushed")
		})
	})

	require.Equal(t, []string{"before", "commit", "done"}, order)

	values := testx.MustTransactWithResult(t, db, func(ctx ql.TxContext) ([]string, error) {
		return ql.Select[string](ctx, `SELECT value FROM hooks_probe`)
	})
	require.Equal(t, []string{"flushed"}, values)
}

func TestBeforeCommitErrorRollsBackAndStillRunsOnDone(t *testing.T) {
	db := testx.NewConnection(t, "ql_hooks_migrations")

	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		return ql.Exec(ctx, `CREATE TABLE IF NOT EXISTS hooks_probe (value TEXT NOT NULL)`)
	})

	errFlush := errors.New("flush failed")
	committed, done := false, false

	err := ql.InNewTransaction(t.Context(), db, func(ctx ql.TxContext) error {
		ctx.OnCommit(func() { committed = true })
		ctx.OnDone(func() { done = true })
		ctx.BeforeCommit(func(ctx ql.TxContext) error {
			if err := ql.Exec(ctx, `INSERT INTO hooks_probe (value) VALUES ($1)`, "rolled-back"); err != nil {
				return err
			}
			return errFlush
		})

		return nil
	})

	require.ErrorIs(t, err, errFlush)
	require.False(t, committed, "commit hooks must not run when the flush failed")
	require.True(t, done, "cleanup hooks run on every outcome")

	values := testx.MustTransactWithResult(t, db, func(ctx ql.TxContext) ([]string, error) {
		return ql.Select[string](ctx, `SELECT value FROM hooks_probe`)
	})
	require.Empty(t, values)
}

// A rollback requested by the users code must skip the flush entirely.
func TestBeforeCommitSkippedOnRollback(t *testing.T) {
	db := testx.NewConnection(t, "ql_hooks_migrations")

	errBoom := errors.New("boom")
	flushed, done := false, false

	err := ql.InNewTransaction(t.Context(), db, func(ctx ql.TxContext) error {
		ctx.OnDone(func() { done = true })
		ctx.BeforeCommit(func(ql.TxContext) error {
			flushed = true
			return nil
		})

		return errBoom
	})

	require.ErrorIs(t, err, errBoom)
	require.False(t, flushed)
	require.True(t, done)
}
