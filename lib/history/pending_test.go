package history

import (
	"context"
	"errors"
	"testing"

	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/flachnetz/startup/v2/lib/testx"
	"github.com/jackc/pgx/v5"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// pendingService returns a service on a fresh history table.
func pendingService(t *testing.T) (*Service, *sqlx.DB) {
	t.Helper()

	db := testx.NewConnection(t, "history_migrations")
	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		return CreateTable(ctx, "history")
	})

	return New(db, pgx.Identifier{"history"}, nil), db
}

func recordsOf(t *testing.T, service *Service, db *sqlx.DB, group GroupId) []Record {
	t.Helper()

	var records []Record
	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		var err error
		records, err = service.Records(ctx, group)
		return err
	})

	return records
}

// A pending record rides the transaction that tracks the work it triggered: one
// transaction writes both rows, so no separate commit is paid for the arrival.
func TestPendingRecordIsWrittenByTheTrackingTransaction(t *testing.T) {
	service, db := pendingService(t)
	group := GroupId{"order", "group-1"}

	ctx := Pending(t.Context(), item{Value: "received"}, group)

	testx.MustTransact(t, db, func(txCtx ql.TxContext) {
		service.Track(txCtx.WithContext(ctx), item{Value: "handled"}, group)
	})
	service.FlushPending(ctx)

	records := recordsOf(t, service, db, group)
	require.Len(t, records, 2, "the arrival record must be written exactly once")
	require.JSONEq(t, `{"value":"received"}`, string(records[0].Payload))
	require.JSONEq(t, `{"value":"handled"}`, string(records[1].Payload))

	var writingTransactions int
	// xid has no ordering operator, so DISTINCT needs the text form.
	require.NoError(t, db.Get(&writingTransactions, `SELECT count(DISTINCT xmin::text) FROM history`))
	require.Equal(t, 1, writingTransactions, "both records must come from one transaction")
}

// The arrival record survives the rollback of the work it describes: "the event
// reached us" stays true when handling it failed.
func TestPendingRecordSurvivesARolledBackTransaction(t *testing.T) {
	service, db := pendingService(t)
	group := GroupId{"order", "group-1"}

	ctx := Pending(t.Context(), item{Value: "received"}, group)

	err := ql.InNewTransaction(ctx, db, func(txCtx ql.TxContext) error {
		service.Track(txCtx, item{Value: "handled"}, group)
		return errors.New("handling failed")
	})
	require.Error(t, err)
	service.FlushPending(ctx)

	records := recordsOf(t, service, db, group)
	require.Len(t, records, 1, "the arrival record must be written exactly once")
	require.JSONEq(t, `{"value":"received"}`, string(records[0].Payload))
}

// Without a transaction to ride, the deferred flush is what writes the record.
func TestPendingRecordIsWrittenByFlushPending(t *testing.T) {
	service, db := pendingService(t)
	group := GroupId{"order", "group-1"}

	ctx := Pending(t.Context(), item{Value: "received"}, group)
	service.FlushPending(ctx)
	service.FlushPending(ctx)

	records := recordsOf(t, service, db, group)
	require.Len(t, records, 1, "a second flush must not duplicate the record")
}

// A context without a pending record is not special: nothing to flush, no panic.
func TestFlushPendingWithoutPendingRecordDoesNothing(t *testing.T) {
	service, db := pendingService(t)

	service.FlushPending(context.Background())

	require.Empty(t, recordsOf(t, service, db, GroupId{"order", "group-1"}))
}
