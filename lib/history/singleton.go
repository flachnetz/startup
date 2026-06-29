package history

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/jackc/pgx/v5"
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
}

// instance holds the global history Service initialized by InitializeGlobal.
var instance *Service

// InitializeGlobal sets up the global history Service used by Track. It creates
// the history table (if it does not exist yet) and starts the background task
// that sends records tracked outside of a transaction.
func InitializeGlobal(ctx context.Context, opts Options) error {
	err := ql.InNewTransaction(ctx, opts.DB, func(ctx ql.TxContext) error {
		return CreateTable(ctx, opts.HistoryTable)
	})
	if err != nil {
		return fmt.Errorf("create history table: %w", err)
	}

	instance = New(opts.DB, pgx.Identifier{opts.HistoryTable}, &EventSending{
		EventSender:    events.Sender,
		EventCreator:   opts.EventCreator,
		ServiceId:      opts.ServiceId,
		ServiceVersion: startup_base.BuildVersion,
		WriteToOutbox:  true,
	})

	// start the background task that flushes records tracked outside of a
	// transaction. Cancel ctx to stop it.
	instance.SendAsync(ctx)

	return nil
}

// Track uses the global history singleton.
// You need to initialize it using InitializeGlobal first.
func Track[T ~string](ctx context.Context, groupId T, item Item) {
	if instance == nil {
		// not initialized: just log the event instead of tracking it.
		slog.WarnContext(ctx, "Tracking event", slog.String("event", item.HistoryString()))
		return
	}

	instance.Track(ctx, GroupId(groupId), item)
}

// RenderPage uses the global history singleton to render the history page for
// groupId. You need to initialize it using InitializeGlobal first.
func RenderPage[T ~string](ctx context.Context, w io.Writer, groupId T, title string) error {
	if instance == nil {
		return errors.New("history: global instance not initialized")
	}

	return instance.RenderPage(ctx, w, GroupId(groupId), title)
}

// RenderPageSummary is RenderPage with a current-state summary above the ledger.
func RenderPageSummary[T ~string](ctx context.Context, w io.Writer, groupId T, title string, summary []SummaryItem) error {
	if instance == nil {
		return errors.New("history: global instance not initialized")
	}

	return instance.RenderPageSummary(ctx, w, GroupId(groupId), title, summary)
}
