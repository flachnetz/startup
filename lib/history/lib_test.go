package history

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/flachnetz/startup/v2/lib/testx"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type item struct {
	Value string `json:"value"`
}

func (i item) HistoryString() string { return "trace string" }

// dummyEvent is a minimal events.Event used to satisfy EventCreator in tests.
// It records everything the EventCreator received so tests can assert on it.
type dummyEvent struct {
	serviceId      string
	serviceVersion string
	rec            RecordToSend
}

func (e dummyEvent) Schema() string            { return "" }
func (e dummyEvent) Serialize(io.Writer) error { return nil }

func dummyEventCreator(serviceId, serviceVersion string, rec RecordToSend) events.Event {
	return dummyEvent{serviceId: serviceId, serviceVersion: serviceVersion, rec: rec}
}

func TestTrackWritesToHistoryTable(t *testing.T) {
	db := testx.NewConnection(t, "history_migrations")

	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		return CreateTable(ctx, "history")
	})

	service := New(db, pgx.Identifier{"history"}, nil)

	testx.MustTransact(t, db, func(ctx ql.TxContext) {
		service.Track(ctx, GroupId("group-1"), item{Value: "hello"})
	})

	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		records, err := service.Records(ctx, GroupId("group-1"))
		require.NoError(t, err)
		require.Len(t, records, 1)

		rec := records[0]
		require.Equal(t, "item", rec.Step)
		require.Equal(t, "trace string", rec.Description)
		require.JSONEq(t, `{"value":"hello"}`, string(rec.Payload))

		return nil
	})
}

func TestTrackCreatesAndSendsEvent(t *testing.T) {
	db := testx.NewConnection(t, "history_migrations")

	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		return CreateTable(ctx, "history")
	})

	captured := testx.CaptureEvents(t)

	service := New(db, pgx.Identifier{"history"}, &EventSending{
		EventSender:    captured,
		EventCreator:   dummyEventCreator,
		ServiceId:      "test-service",
		ServiceVersion: "1.2.3",
		WriteToOutbox:  true,
	})

	testx.MustTransact(t, db, func(ctx ql.TxContext) {
		service.Track(ctx, GroupId("group-1"), item{Value: "hello"})
	})

	// the EventCreator should have been used to build exactly one event, and it
	// should have been sent out via the EventSender once the transaction committed.
	event := testx.MockEventsGetSingle[dummyEvent](t, captured)

	require.Equal(t, "test-service", event.serviceId)
	require.Equal(t, "1.2.3", event.serviceVersion)
	require.Equal(t, GroupId("group-1"), event.rec.GroupId)
	require.Equal(t, "item", event.rec.Step)
	require.Equal(t, "trace string", event.rec.Description)
	require.JSONEq(t, `{"value":"hello"}`, string(event.rec.Payload))
}

func TestTrackSendsEventAsyncOnCommit(t *testing.T) {
	db := testx.NewConnection(t, "history_migrations")

	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		return CreateTable(ctx, "history")
	})

	captured := testx.CaptureEvents(t)

	// no outbox: the event is sent asynchronously once the transaction commits.
	service := New(db, pgx.Identifier{"history"}, &EventSending{
		EventSender:    captured,
		EventCreator:   dummyEventCreator,
		ServiceId:      "test-service",
		ServiceVersion: "1.2.3",
		WriteToOutbox:  false,
	})

	testx.MustTransact(t, db, func(ctx ql.TxContext) {
		service.Track(ctx, GroupId("group-1"), item{Value: "hello"})
	})

	// the history row is written as part of the transaction.
	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		records, err := service.Records(ctx, GroupId("group-1"))
		require.NoError(t, err)
		require.Len(t, records, 1)
		return nil
	})

	// and the event is sent out on commit.
	event := testx.MockEventsGetSingle[dummyEvent](t, captured)
	require.Equal(t, GroupId("group-1"), event.rec.GroupId)
	require.Equal(t, "test-service", event.serviceId)
}

func TestTrackWithoutTableOnlySendsEvent(t *testing.T) {
	db := testx.NewConnection(t, "history_migrations")

	captured := testx.CaptureEvents(t)

	// no table configured: tracking is done purely via events.
	service := New(db, nil, &EventSending{
		EventSender:    captured,
		EventCreator:   dummyEventCreator,
		ServiceId:      "test-service",
		ServiceVersion: "1.2.3",
		WriteToOutbox:  false,
	})

	testx.MustTransact(t, db, func(ctx ql.TxContext) {
		service.Track(ctx, GroupId("group-1"), item{Value: "hello"})
	})

	event := testx.MockEventsGetSingle[dummyEvent](t, captured)
	require.Equal(t, GroupId("group-1"), event.rec.GroupId)

	// without a table there is nothing to read back.
	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		_, err := service.Records(ctx, GroupId("group-1"))
		require.ErrorIs(t, err, ErrNoTable)
		return nil
	})
}

func TestTrackAsyncFlushesQueuedRecords(t *testing.T) {
	db := testx.NewConnection(t, "history_migrations")

	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		return CreateTable(ctx, "history")
	})

	captured := testx.CaptureEvents(t)

	service := New(db, pgx.Identifier{"history"}, &EventSending{
		EventSender:    captured,
		EventCreator:   dummyEventCreator,
		ServiceId:      "test-service",
		ServiceVersion: "1.2.3",
		WriteToOutbox:  true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// start the background flush task.
	service.SendAsync(ctx)

	// tracking outside of a transaction enqueues the record to be flushed later.
	service.Track(context.Background(), GroupId("group-1"), item{Value: "hello"})

	// the background task flushes the queued record in its own transaction and
	// sends the event out.
	hasEvent := func() bool { return len(testx.MockEventsGetAll[dummyEvent](t, captured)) == 1 }
	require.Eventually(t, hasEvent, 3*time.Second, 50*time.Millisecond)

	event := testx.MockEventsGetSingle[dummyEvent](t, captured)
	require.Equal(t, GroupId("group-1"), event.rec.GroupId)

	// the flushed record is also persisted to the history table.
	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		records, err := service.Records(ctx, GroupId("group-1"))
		require.NoError(t, err)
		require.Len(t, records, 1)
		return nil
	})
}
