package history

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/jackc/pgx/v5"
	"github.com/jmoiron/sqlx"
)

// Options configures the global history Service set up by InitializeGlobal.
type Options struct {
	// DB starts the transactions used to create the table and to write records.
	DB ql.TxStarter

	// ServiceId identifies this service as the sender of the emitted events.
	ServiceId string
	// HistoryTable is the name of the table that history records are written to.
	HistoryTable string
	// EventCreator builds the event that is sent out for every tracked record.
	EventCreator EventCreator
	// Athena, when set, enables the Athena read fallback for old records
	// (see WithAthena and Service.RecordsAt).
	Athena *AthenaConfig
}

// instance holds the global history Service initialized by InitializeGlobal.
var instance *Service

// InitializeGlobal sets up the global history Service used by Track. It creates
// the history table (if it does not exist yet) and starts the background task
// that sends records tracked outside of a transaction.
func InitializeGlobal(ctx context.Context, opts Options) error {
	err := ql.InNewTransaction(ctx, opts.DB, func(ctx ql.TxContext) error {
		return CreateTable(ctx, opts.HistoryTable, opts.ServiceId)
	})
	if err != nil {
		return fmt.Errorf("create history table: %w", err)
	}

	var histOpts []Option
	if opts.Athena != nil {
		histOpts = append(histOpts, WithAthena(*opts.Athena))
	}

	instance = New(opts.DB, pgx.Identifier{opts.HistoryTable}, &EventSending{
		EventSender:    forwardSender{},
		EventCreator:   opts.EventCreator,
		ServiceId:      opts.ServiceId,
		ServiceVersion: startup_base.BuildVersion,
		WriteToOutbox:  true,
	}, histOpts...)

	// start the background task that flushes records tracked outside of a
	// transaction. Cancel ctx to stop it.
	instance.SendAsync(ctx)

	return nil
}

// Track uses the global history singleton.
// You need to initialize it using InitializeGlobal first.
func Track[T ~string](ctx context.Context, groupType GroupType, groupId T, item Item) {
	if instance == nil {
		// not initialized: just log the event instead of tracking it.
		slog.WarnContext(ctx, "Tracking event", slog.String("event", item.HistoryString()))
		return
	}

	instance.Track(ctx, groupType, GroupId(groupId), item)
}

// RenderPage uses the global history singleton to render the history page for
// groupId. You need to initialize it using InitializeGlobal first.
func RenderPage[T ~string](ctx context.Context, w io.Writer, groupType GroupType, groupId T, title string) error {
	if instance == nil {
		return errors.New("history: global instance not initialized")
	}

	return instance.RenderPage(ctx, w, groupType, GroupId(groupId), title)
}

// RenderPageAt is RenderPage with the Athena fallback: createdTime decides
// whether records are read from the local table or from Athena.
func RenderPageAt[T ~string](ctx context.Context, w io.Writer, groupType GroupType, groupId T, title string, createdTime time.Time) error {
	if instance == nil {
		return errors.New("history: global instance not initialized")
	}

	return instance.RenderPageAt(ctx, w, groupType, GroupId(groupId), title, createdTime)
}

// RenderPageSummary is RenderPage with a current-state summary above the ledger.
func RenderPageSummary[T ~string](ctx context.Context, w io.Writer, groupType GroupType, groupId T, title string, summary []SummaryItem) error {
	if instance == nil {
		return errors.New("history: global instance not initialized")
	}

	return instance.RenderPageSummary(ctx, w, groupType, GroupId(groupId), title, summary)
}

// RenderPageSummaryAt is RenderPageSummary with the Athena fallback (see RenderPageAt).
func RenderPageSummaryAt[T ~string](ctx context.Context, w io.Writer, groupType GroupType, groupId T, title string, summary []SummaryItem, createdTime time.Time) error {
	if instance == nil {
		return errors.New("history: global instance not initialized")
	}

	return instance.RenderPageSummaryAt(ctx, w, groupType, GroupId(groupId), title, summary, createdTime)
}

// RecordsAt uses the global history singleton to load records for groupId with
// the Athena fallback (see Service.RecordsAt).
func RecordsAt[T ~string](ctx ql.TxContext, groupType GroupType, groupId T, createdTime time.Time) ([]Record, error) {
	if instance == nil {
		return nil, errors.New("history: global instance not initialized")
	}

	return instance.RecordsAt(ctx, groupType, GroupId(groupId), createdTime)
}

type forwardSender struct{}

func (f forwardSender) SendAsync(ctx context.Context, event events.Event) {
	events.Sender.SendAsync(ctx, event)
}

func (f forwardSender) SendInTx(ctx context.Context, tx sqlx.ExecerContext, event events.Event) error {
	return events.Sender.SendInTx(ctx, tx, event)
}

func (f forwardSender) Close() error {
	return nil
}
