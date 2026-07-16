package history

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"time"

	"github.com/flachnetz/startup/v2/lib/clock"
	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/lib/ql"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/jackc/pgx/v5"
)

var ErrNoTable = errors.New("no trace table configured")

// GroupId groups multiple history.Record instances into one history trace.
type GroupId string

func (g GroupId) LogValue() slog.Value {
	return slog.StringValue(string(g))
}

func (g GroupId) String() string {
	return string(g)
}

// Record describes one tracing record.
type Record struct {
	Timestamp      time.Time       `db:"timestamp"`
	RequestTraceId RequestTraceId  `db:"request_trace_id"`
	Step           string          `db:"step"`
	Description    string          `db:"description"`
	Payload        json.RawMessage `db:"payload"`
	Trigger        Trigger         `db:"trigger"`

	// Optional field that might indicate the sender of an event. This is useful if the
	// event comes from an external source, e.g. athena.
	// For local data, it is set to either the serviceId or to `local`, if no serviceId
	// is specified.
	EventSender        string `db:"-"`
	EventSenderVersion string `db:"-"`
}

// Item must be implemented by every event that is to be traced.
// Every Item should be json serializable.
type Item interface {
	// HistoryString returns a human-readable description of this trace.Item.
	HistoryString() string
}

// EventSender is re-exported from the events package for convenience.
type EventSender = events.EventSender

// EventSending groups options required for sending events to an EventSender.
type EventSending struct {
	// EventSender sends the events created by EventCreator.
	EventSender EventSender
	// EventCreator builds the event that is sent for a tracked record.
	EventCreator EventCreator

	// ServiceId and ServiceVersion identify the sender of the events. If left
	// empty, they are loaded from the SERVICE_ID and SERVICE_VERSION environment
	// variables in New.
	ServiceId      string
	ServiceVersion string

	// Write events to the kafka outbox first. If false, events are sent async.
	WriteToOutbox bool
}

// Service traces events by writing them to a history table and/or sending them
// out as events. Create an instance with New.
type Service struct {
	txStarter    ql.TxStarter
	table        pgx.Identifier
	eventSending *EventSending
	athena       *AthenaConfig

	// record to send out async
	queue chan RecordToSend
}

// Option customizes a Service created with New.
type Option func(*Service)

// WithAthena enables the Athena fallback for reads: RecordsAt loads records
// from Athena instead of the local table when the tracked object is older than
// AthenaConfig.LookupThreshold. See RecordsAt.
func WithAthena(cfg AthenaConfig) Option {
	return func(s *Service) { s.athena = &cfg }
}

// New creates a new history.Service instance to trace events. By default the service writes
// records to the history table given by table.
//
// If you specify the optional eventSending parameter, every tracked record is also turned
// into an event. With EventSending.WriteToOutbox the event is written to the kafka outbox as
// part of the same transaction (via EventSender.SendInTx); otherwise it is sent asynchronously
// once the transaction commits (via EventSender.SendAsync).
//
// If Service.SendAsync was called prior, trace events that are not tracked within a transaction
// are put into a channel (without blocking) and are sent later.
//
// You can specify parameter table as nil to not write to the history table. If you specify an
// EventSending config, history entries are then only sent out as events.
func New(txStarter ql.TxStarter, table pgx.Identifier, eventSending *EventSending, opts ...Option) *Service {
	if eventSending != nil {
		if eventSending.EventSender == nil || eventSending.EventCreator == nil {
			panic(errors.New("history: EventSending requires both EventSender and EventCreator"))
		}

		if eventSending.ServiceId == "" {
			// try to load from environment
			if id, ok := os.LookupEnv("SERVICE_ID"); ok {
				eventSending.ServiceId = id
			}
		}
		if eventSending.ServiceVersion == "" {
			if version, ok := os.LookupEnv("SERVICE_VERSION"); ok {
				eventSending.ServiceVersion = version
			}
		}
	}
	s := &Service{
		txStarter:    txStarter,
		table:        table,
		eventSending: eventSending,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Track records the given item under groupId. Depending on the Service configuration the
// record is written to the history table and/or sent out as an event. Tracking never returns
// an error: any failure is only logged.
//
// If the context carries a transaction, the record is written within it. Otherwise a new
// transaction is opened, unless async sending was enabled via Service.SendAsync, in which case
// the record is queued without blocking.
func (h *Service) Track(ctx context.Context, groupId GroupId, item Item) {
	ctx = context.WithoutCancel(ctx)

	encoded, _ := json.Marshal(item)

	rec := RecordToSend{
		GroupId:        groupId,
		Timestamp:      clock.GlobalClock.Now(),
		RequestTraceId: requestTraceIdOf(ctx),
		Trigger:        triggerOf(ctx),
		Step:           reflect.Indirect(reflect.ValueOf(item)).Type().Name(),
		Description:    item.HistoryString(),
		Payload:        encoded,
	}

	logger := sl.LoggerOf(ctx)
	logger.DebugContext(ctx, "Track history entry", slog.String("step", rec.Step), slog.String("entry", item.HistoryString()))

	sendInTx := func(ctx ql.TxContext) error { return h.trackInTx(ctx, rec) }

	var err error

	if h.requireTransaction() {
		if tx := ql.TxContextFromContext(ctx); tx != nil || h.queue == nil {
			// send in the existing or in a new transaction
			err = ql.InAnyTransaction(ctx, h.txStarter, sendInTx)
		} else {
			// there is no transaction but there is a queue
			err = h.sendToChannel(ctx, rec)
		}
	} else if h.eventSending != nil {
		// we do not require a transaction to send,
		// but if we do have a transaction, we send the trace
		// on successful commit, otherwise we send it directly
		if txCtx := ql.TxContextFromContext(ctx); txCtx != nil {
			txCtx.OnCommit(func() { h.trackAsyncEvent(txCtx, rec) })
		} else {
			// no transaction: send the event directly
			h.trackAsyncEvent(ctx, rec)
		}
	}

	if err != nil {
		logger.WarnContext(
			ctx, "Failed to create trace item",
			slog.Any("groupId", groupId),
			slog.String("entry", item.HistoryString()),
			sl.Error(err),
		)
	}
}

// Records returns all local events for the given groupId.
// This method does not guarantee any ordering between the records returned.
func (h *Service) Records(ctx ql.TxContext, groupId GroupId) ([]Record, error) {
	if h.table == nil {
		// no table is configured, tracing is probably done only via events.
		return nil, ErrNoTable
	}

	query := fmt.Sprintf(
		"SELECT timestamp, COALESCE(request_trace_id, '00') as request_trace_id, step, description, payload, \"trigger\" FROM %s WHERE group_id=$1",
		h.table.Sanitize(),
	)

	records, err := ql.Select[Record](ctx, query, groupId)
	if err != nil {
		return nil, fmt.Errorf("query records: %w", err)
	}

	// initialize a value for the event sender field.
	eventSender := "local"
	eventVersion := "unknown"
	if h.eventSending != nil {
		eventSender = h.eventSending.ServiceId
		eventVersion = h.eventSending.ServiceVersion
	}

	// and update the field on all records
	for idx := range records {
		records[idx].EventSender = eventSender
		records[idx].EventSenderVersion = eventVersion
	}

	return records, nil
}

// Cleanup deletes all events that happened before the given timestamp.
func (h *Service) Cleanup(ctx context.Context, txStarter ql.TxStarter, before time.Time) error {
	if h.table == nil {
		// no table configured, so no cleanup is needed.
		return nil
	}

	// delete chunk-wise, only the rows that are actually older than the cutoff.
	sql := `
		DELETE FROM %s
		WHERE ctid IN (
			SELECT ctid FROM %s WHERE timestamp < $1
			ORDER BY timestamp LIMIT 1000
		)
	`

	stmt := fmt.Sprintf(sql, h.table.Sanitize(), h.table.Sanitize())

	for {
		more, err := ql.InNewTransactionWithResult(ctx, txStarter, func(ctx ql.TxContext) (bool, error) {
			affected, err := ql.ExecAffected(ctx, stmt, before)
			if err != nil {
				return false, err
			}

			return affected >= 1000, nil
		})
		if err != nil {
			return fmt.Errorf("cleanup chunk: %w", err)
		}

		if !more {
			return nil
		}
	}
}

// requireTransaction checks if a transaction is needed to send an event.
func (h *Service) requireTransaction() bool {
	return h.table != nil || (h.eventSending != nil && h.eventSending.WriteToOutbox)
}

func (h *Service) trackAsyncEvent(ctx context.Context, rec RecordToSend) {
	event := h.eventSending.EventCreator(h.eventSending.ServiceId, h.eventSending.ServiceVersion, rec)
	h.eventSending.EventSender.SendAsync(ctx, event)
}

func (h *Service) trackInTx(ctx ql.TxContext, rec RecordToSend) error {
	stmt := fmt.Sprintf(
		`INSERT INTO %s ("timestamp", group_id, request_trace_id, step, description, payload, "trigger") VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		h.table.Sanitize(),
	)

	var result error

	// store in local table
	err := ql.Exec(ctx, stmt, rec.Timestamp,
		rec.GroupId.String(), rec.RequestTraceId, rec.Step, rec.Description,
		[]byte(rec.Payload), rec.Trigger)

	result = errors.Join(result, err)

	if h.eventSending != nil {
		// convert to an event
		event := h.eventSending.EventCreator(h.eventSending.ServiceId, h.eventSending.ServiceVersion, rec)

		if h.eventSending.WriteToOutbox {
			// write the event to the outbox table as part of this transaction
			if err := h.eventSending.EventSender.SendInTx(ctx, ctx, event); err != nil {
				result = errors.Join(result, err)
			}
		} else {
			// enqueue a commit action to send the event to kafka if this transaction is committed
			ctx.OnCommit(func() { h.eventSending.EventSender.SendAsync(ctx, event) })
		}
	}

	return result
}

// EventCreator builds the event that is sent out for a tracked record. It receives the
// ServiceId and ServiceVersion from the EventSending config.
type EventCreator func(serviceId, serviceVersion string, rec RecordToSend) events.Event

// RecordToSend is a single tracked record, ready to be written to the history table
// and/or converted into an event.
type RecordToSend struct {
	GroupId        GroupId
	Timestamp      time.Time
	Step           string
	Description    string
	Payload        json.RawMessage
	RequestTraceId RequestTraceId
	Trigger        Trigger
}

// CreateTable creates the history table with the given name together with the indexes
// used by Records and Cleanup, unless they already exist.
func CreateTable(ctx ql.TxContext, name string) error {
	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
		    timestamp        TIMESTAMP NOT NULL,
		    group_id         TEXT      NOT NULL,
		    step             TEXT      NOT NULL,
		    description      TEXT      NOT NULL,
		    request_trace_id TEXT      NULL,
		    payload          JSON      NOT NULL,
		    "trigger"        TEXT      NULL
		)
	`, name)

	if err := ql.Exec(ctx, sql); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// self-migrate tables created before the trigger column existed.
	sql = fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS "trigger" TEXT NULL`, name)
	if err := ql.Exec(ctx, sql); err != nil {
		return fmt.Errorf("add trigger column: %w", err)
	}

	sql = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_group_id_idx ON %s (group_id)`, name, name)
	if err := ql.Exec(ctx, sql); err != nil {
		return fmt.Errorf("create index: %w", err)
	}

	sql = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_request_trace_id_idx ON %s (request_trace_id)`, name, name)
	if err := ql.Exec(ctx, sql); err != nil {
		return fmt.Errorf("create index: %w", err)
	}

	sql = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_timestamp_idx ON %s (timestamp)`, name, name)
	if err := ql.Exec(ctx, sql); err != nil {
		return fmt.Errorf("create index: %w", err)
	}

	return nil
}
