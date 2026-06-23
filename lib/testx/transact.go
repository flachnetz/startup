package testx

import (
	"testing"

	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// MustTransact runs fn inside a new transaction bound to the test context and commits
// it. The test fails if the transaction cannot be opened or committed.
func MustTransact(t *testing.T, db *sqlx.DB, fn func(ctx ql.TxContext)) {
	t.Helper()

	err := ql.InNewTransaction(t.Context(), db, func(ctx ql.TxContext) error {
		t.Helper()
		fn(ctx)
		return nil
	})

	require.NoError(t, err)
}

// MustTransactErr runs fn inside a new transaction bound to the test context and fails
// the test if fn returns an error. On success the transaction is committed.
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
