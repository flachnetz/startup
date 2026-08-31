package history

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/flachnetz/startup/v2/lib/ql"
	sl "github.com/flachnetz/startup/v2/startup_logging"
)

// pendingRow is a record waiting for a transaction to ride along with. claimed is
// set while a transaction carries it, written once that transaction committed: a
// rollback releases the claim, so the record is not lost with the work it
// describes.
type pendingRow struct {
	rec     RecordToSend
	claimed atomic.Bool
	written atomic.Bool
}

type pendingKey struct{}

// Pending stashes a record on ctx instead of writing it right away. The first
// Track that runs in a transaction under the returned context writes it there,
// first; FlushPending writes it if no transaction did.
//
// This is for the entry that records "we received X" right before the work X
// triggers: the record belongs in that work's transaction, and the call site (a
// kafka consumer, a webhook) does not own that transaction. A transaction of its
// own would cost a connection acquire and a commit on the hot path.
//
// Ordering on the history page is unaffected: timestamp and provenance are taken
// here, at the Pending call.
func Pending(ctx context.Context, item Item, groupId GroupId, groupIds ...GroupId) context.Context {
	rec := recordOf(ctx, item, append([]GroupId{groupId}, groupIds...))

	return context.WithValue(ctx, pendingKey{}, &pendingRow{rec: rec})
}

// FlushPending writes a record stashed by Pending that no transaction picked up,
// in a transaction of its own. Deferred by the call site that stashed it, so a
// handler that opened no transaction - or whose transaction rolled back - still
// leaves its entry.
func (h *Service) FlushPending(ctx context.Context) {
	row, ok := ctx.Value(pendingKey{}).(*pendingRow)
	if !ok || row.claimed.Swap(true) {
		return
	}

	if !h.requireTransaction() {
		h.send(ctx, row.rec)
		return
	}

	// Deliberately not the async queue: the caller is on its way out, and a
	// record still in a channel is a record lost to the next pod restart.
	err := ql.InAnyTransaction(context.WithoutCancel(ctx), h.txStarter, func(txCtx ql.TxContext) error {
		return h.trackInTx(txCtx, row.rec)
	})
	if err != nil {
		sl.LoggerOf(ctx).WarnContext(ctx, "Failed to flush pending trace item",
			slog.Any("groupIds", row.rec.GroupIds), slog.String("entry", row.rec.Description), sl.Error(err))
		return
	}

	row.written.Store(true)
}

// writePending writes the stashed record into txCtx, once. A rollback releases
// the claim again, so FlushPending still writes it afterwards: the entry says the
// event arrived, which stays true when handling it failed.
func (h *Service) writePending(txCtx ql.TxContext) {
	row, ok := txCtx.Value(pendingKey{}).(*pendingRow)
	if !ok || row.claimed.Swap(true) {
		return
	}

	if err := h.trackInTx(txCtx, row.rec); err != nil {
		sl.LoggerOf(txCtx).WarnContext(txCtx, "Failed to create pending trace item",
			slog.Any("groupIds", row.rec.GroupIds), slog.String("entry", row.rec.Description), sl.Error(err))
	}

	txCtx.OnCommit(func() { row.written.Store(true) })
	txCtx.OnDone(func() {
		if !row.written.Load() {
			row.claimed.Store(false)
		}
	})
}
