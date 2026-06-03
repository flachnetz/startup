package testx

import (
	"testing"

	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func MustTransact(t *testing.T, db *sqlx.DB, fn func(ctx ql.TxContext)) {
	t.Helper()

	err := ql.InNewTransaction(t.Context(), db, func(ctx ql.TxContext) error {
		t.Helper()
		fn(ctx)
		return nil
	})

	require.NoError(t, err)
}

func MustTransactErr(t *testing.T, db *sqlx.DB, fn func(ctx ql.TxContext) error) {
	t.Helper()

	err := ql.InNewTransaction(t.Context(), db, func(ctx ql.TxContext) error {
		t.Helper()

		err := fn(ctx)
		require.NoError(t, err)

		return nil
	})

	require.NoError(t, err)
}
