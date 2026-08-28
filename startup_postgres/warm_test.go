package startup_postgres

import (
	"context"
	"log/slog"
	"testing"

	"github.com/flachnetz/pgtest/v2"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestWarmPoolOpensTheConfiguredNumberOfConnections(t *testing.T) {
	db := sqlx.NewDb(pgtest.Connect(t), "pgx")
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	warmPool(t.Context(), db, 4, slog.Default())

	stats := db.Stats()
	require.Equal(t, 4, stats.OpenConnections)
	require.Equal(t, 4, stats.Idle, "warmed connections are returned to the pool")
}

// A pool that cannot be warmed must not block startup: the loop gives up on the
// first failure and leaves the pool usable.
func TestWarmPoolGivesUpOnError(t *testing.T) {
	db := sqlx.NewDb(pgtest.Connect(t), "pgx")
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)

	before := db.Stats().OpenConnections

	ctx, cancel := contextCancelled(t)
	defer cancel()

	warmPool(ctx, db, 2, slog.Default())

	require.Equal(t, before, db.Stats().OpenConnections, "no connection opened after the failure")
	require.NoError(t, db.PingContext(t.Context()))
}

func contextCancelled(t *testing.T) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	return ctx, cancel
}
