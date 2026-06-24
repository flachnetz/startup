package history

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/flachnetz/startup/v2/lib/batcher"
	"github.com/flachnetz/startup/v2/lib/ql"
	sl "github.com/flachnetz/startup/v2/startup_logging"
)

var ErrChannelFull = errors.New("channel is full")

// SendAsync starts the async process. This will start a background task to
// send out records that are not traced within a transaction. You can stop
// the background task by canceling the Context ctx.
func (h *Service) SendAsync(ctx context.Context) {
	var records []RecordToSend

	if h.queue != nil {
		panic(errors.New("already initialized"))
	}

	// setup the queue
	h.queue = make(chan RecordToSend, 512)

	go h.sendAsyncTask(ctx, records)
}

func (h *Service) sendAsyncTask(ctx context.Context, records []RecordToSend) {
	b := batcher.New(256, 100*time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			// hard exit, we can not even send the pending messages
			// if the context is done
			return

		case <-b.Await():
			b.Reset()
			h.flush(ctx, records)
			records = nil

		case record, ok := <-h.queue:
			if !ok {
				// shutting down, flushing all pending messages
				h.flush(ctx, records)
				return
			}

			b.Increment()
			records = append(records, record)
		}
	}
}

func (h *Service) flush(ctx context.Context, records []RecordToSend) {
	if len(records) == 0 {
		return
	}

	err := ql.InNewTransaction(ctx, h.txStarter, func(ctx ql.TxContext) error {
		for _, rec := range records {
			if err := h.trackInTx(ctx, rec); err != nil {
				return fmt.Errorf("send record: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		logger := sl.LoggerOf(ctx)
		logger.Warn("Failed to flush records", slog.Int("recordCount", len(records)), sl.Error(err))
	}
}

func (h *Service) sendToChannel(ctx context.Context, rec RecordToSend) error {
	select {
	case h.queue <- rec:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrChannelFull
	}
}
