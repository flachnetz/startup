package history

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/flachnetz/startup/v2/lib/testx"
	"github.com/jackc/pgx/v5"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
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
		service.Track(ctx, item{Value: "hello"}, GroupId{"order", "group-1"})
	})

	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		records, err := service.Records(ctx, GroupId{"order", "group-1"})
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
		service.Track(ctx, item{Value: "hello"}, GroupId{"order", "group-1"})
	})

	// the EventCreator should have been used to build exactly one event, and it
	// should have been sent out via the EventSender once the transaction committed.
	event := testx.MockEventsGetSingle[dummyEvent](t, captured)

	require.Equal(t, "test-service", event.serviceId)
	require.Equal(t, "1.2.3", event.serviceVersion)
	require.Equal(t, GroupIds{{"order", "group-1"}}, event.rec.GroupIds)
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
		service.Track(ctx, item{Value: "hello"}, GroupId{"order", "group-1"})
	})

	// the history row is written as part of the transaction.
	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		records, err := service.Records(ctx, GroupId{"order", "group-1"})
		require.NoError(t, err)
		require.Len(t, records, 1)
		return nil
	})

	// and the event is sent out on commit.
	event := testx.MockEventsGetSingle[dummyEvent](t, captured)
	require.Equal(t, GroupIds{{"order", "group-1"}}, event.rec.GroupIds)
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
		service.Track(ctx, item{Value: "hello"}, GroupId{"order", "group-1"})
	})

	event := testx.MockEventsGetSingle[dummyEvent](t, captured)
	require.Equal(t, GroupIds{{"order", "group-1"}}, event.rec.GroupIds)

	// without a table there is nothing to read back.
	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		_, err := service.Records(ctx, GroupId{"order", "group-1"})
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

	// tracking outside a transaction enqueues the record to be flushed later.
	service.Track(context.Background(), item{Value: "hello"}, GroupId{"order", "group-1"})

	// the background task flushes the queued record in its own transaction and
	// sends the event out.
	hasEvent := func() bool { return len(testx.MockEventsGetAll[dummyEvent](t, captured)) == 1 }
	require.Eventually(t, hasEvent, 3*time.Second, 50*time.Millisecond)

	event := testx.MockEventsGetSingle[dummyEvent](t, captured)
	require.Equal(t, GroupIds{{"order", "group-1"}}, event.rec.GroupIds)

	// the flushed record is also persisted to the history table.
	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		records, err := service.Records(ctx, GroupId{"order", "group-1"})
		require.NoError(t, err)
		require.Len(t, records, 1)
		return nil
	})
}

// TestRecordsAtRoutesToLocal covers the RecordsAt paths that resolve from the
// local table without touching Athena: no Athena config, a zero createdTime with
// data still present locally, and a createdTime newer than the lookup threshold.
// The Athena-hitting paths (known-old, and empty-local fallback) are not
// exercised here because they require a live AWS Athena endpoint.
func TestRecordsAtRoutesToLocal(t *testing.T) {
	db := testx.NewConnection(t, "history_migrations")

	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		return CreateTable(ctx, "history")
	})

	testx.MustTransact(t, db, func(ctx ql.TxContext) {
		New(db, pgx.Identifier{"history"}, nil).
			Track(ctx, item{Value: "hello"}, GroupId{"order", "group-1"})
	})

	athenaCfg := AthenaConfig{Database: "db", Table: "t", WorkGroup: "wg", OutputLocation: "s3://bucket/out/"}

	cases := []struct {
		name        string
		opts        []Option
		createdTime time.Time
	}{
		{"no athena config", nil, time.Now().Add(-72 * time.Hour)},
		{"zero created time", []Option{WithAthena(athenaCfg)}, time.Time{}},
		{"newer than threshold", []Option{WithAthena(athenaCfg)}, time.Now()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service := New(db, pgx.Identifier{"history"}, nil, tc.opts...)
			testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
				records, err := service.RecordsAt(ctx, GroupId{"order", "group-1"}, tc.createdTime)
				require.NoError(t, err)
				require.Len(t, records, 1)
				require.Equal(t, "hello", func() string {
					var i item
					_ = json.Unmarshal(records[0].Payload, &i)
					return i.Value
				}())
				return nil
			})
		})
	}
}

func TestTrackMultipleGroupIds(t *testing.T) {
	db := testx.NewConnection(t, "history_migrations")

	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		return CreateTable(ctx, "history")
	})

	service := New(db, pgx.Identifier{"history"}, nil)

	order := GroupId{"order", "ord-1"}
	player := GroupId{"player", "pl-42"}

	testx.MustTransact(t, db, func(ctx ql.TxContext) {
		service.Track(ctx, item{Value: "multi"}, order, player)
	})

	// the record is found when searching by either group id
	for _, gid := range (GroupIds{order, player}) {
		t.Run("lookup by "+gid.String(), func(t *testing.T) {
			testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
				records, err := service.Records(ctx, gid)
				require.NoError(t, err)
				require.Len(t, records, 1)
				require.Equal(t, "trace string", records[0].Description)
				return nil
			})
		})
	}

	// a group id that was not tracked returns no results
	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		records, err := service.Records(ctx, GroupId{"order", "unknown"})
		require.NoError(t, err)
		require.Empty(t, records)
		return nil
	})
}

// Several records tracked in one transaction are written with a single INSERT.
// The batching must not lose records, reorder them, or drop the ones tracked
// after the first flush was scheduled.
func TestTrackBatchesRowsWithinOneTransaction(t *testing.T) {
	db := testx.NewConnection(t, "history_migrations")
	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		return CreateTable(ctx, "history")
	})

	service := New(db, pgx.Identifier{"history"}, nil)
	group := GroupId{"order", "batch-1"}

	testx.MustTransact(t, db, func(ctx ql.TxContext) {
		for _, v := range []string{"one", "two", "three"} {
			service.Track(ctx, item{Value: v}, group)
		}
	})

	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		records, err := service.Records(ctx, group)
		require.NoError(t, err)
		require.Len(t, records, 3)

		values := make([]string, 0, len(records))
		for _, rec := range records {
			var i item
			require.NoError(t, json.Unmarshal(rec.Payload, &i))
			values = append(values, i.Value)
		}
		require.ElementsMatch(t, []string{"one", "two", "three"}, values)

		return nil
	})
}

// A rolled back transaction must not leave buffered rows behind that a later
// transaction would flush.
func TestTrackBufferIsDiscardedOnRollback(t *testing.T) {
	db := testx.NewConnection(t, "history_migrations")
	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		return CreateTable(ctx, "history")
	})

	service := New(db, pgx.Identifier{"history"}, nil)
	group := GroupId{"order", "rollback-1"}

	errBoom := errors.New("boom")
	err := ql.InNewTransaction(t.Context(), db, func(ctx ql.TxContext) error {
		service.Track(ctx, item{Value: "gone"}, group)
		return errBoom
	})
	require.ErrorIs(t, err, errBoom)

	testx.MustTransact(t, db, func(ctx ql.TxContext) {
		service.Track(ctx, item{Value: "kept"}, group)
	})

	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		records, err := service.Records(ctx, group)
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.JSONEq(t, `{"value":"kept"}`, string(records[0].Payload))
		return nil
	})
}

// spanSender records the span the sender was handed for every event.
type spanSender struct {
	t     *testing.T
	spans []trace.SpanContext
}

func (s *spanSender) record(ctx context.Context) {
	s.spans = append(s.spans, trace.SpanContextFromContext(ctx))
}

func (s *spanSender) SendAsync(ctx context.Context, _ events.Event) { s.record(ctx) }

func (s *spanSender) SendInTx(ctx context.Context, _ sqlx.ExecerContext, _ events.Event) error {
	// the outbox write must still happen inside the transaction
	require.NotNil(s.t, ql.TxContextFromContext(ctx))
	s.record(ctx)
	return nil
}

func (s *spanSender) Close() error { return nil }

// All events of one transaction hang under a single "HistoryEvents" parent span,
// so they collapse into one group in the trace view.
func TestTrackGroupsEventsInOneSpan(t *testing.T) {
	db := testx.NewConnection(t, "history_migrations")
	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		return CreateTable(ctx, "history")
	})

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})

	for _, outbox := range []bool{true, false} {
		t.Run(fmt.Sprintf("outbox=%v", outbox), func(t *testing.T) {
			sender := &spanSender{t: t}
			service := New(db, pgx.Identifier{"history"}, &EventSending{
				EventSender:   sender,
				EventCreator:  dummyEventCreator,
				WriteToOutbox: outbox,
			})

			testx.MustTransact(t, db, func(ctx ql.TxContext) {
				for _, v := range []string{"one", "two", "three"} {
					service.Track(ctx, item{Value: v}, GroupId{"order", "span-1"})
				}

				// nothing is handed to the sender while the transaction still runs:
				// history events are flushed at the end of it.
				require.Empty(t, sender.spans)
			})

			var parents []sdktrace.ReadOnlySpan
			for _, span := range recorder.Ended() {
				if span.Name() == "HistoryEvents" {
					parents = append(parents, span)
				}
			}
			require.Len(t, parents, 1, "exactly one parent span per transaction")

			// every event was sent with that parent span as its context
			require.Len(t, sender.spans, 3)
			for _, sc := range sender.spans {
				require.Equal(t, parents[0].SpanContext().SpanID(), sc.SpanID())
			}

			recorder.Reset()
		})
	}
}

// Records already batched in sendAsyncTask must still be written when the
// background task's context is cancelled - shutdown must not lose them.
func TestSendAsyncFlushesBatchedRecordsOnShutdown(t *testing.T) {
	db := testx.NewConnection(t, "history_migrations")
	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		return CreateTable(ctx, "history")
	})

	service := New(db, pgx.Identifier{"history"}, nil)
	group := GroupId{"order", "shutdown-1"}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// the task returns right away because the context is already cancelled, and
	// flushes what it holds on the way out.
	service.sendAsyncTask(ctx, []RecordToSend{recordOf(ctx, item{Value: "kept"}, []GroupId{group})})

	testx.MustTransactErr(t, db, func(ctx ql.TxContext) error {
		records, err := service.Records(ctx, group)
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.JSONEq(t, `{"value":"kept"}`, string(records[0].Payload))
		return nil
	})
}
