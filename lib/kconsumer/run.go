package kconsumer

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	sl "github.com/flachnetz/startup/v2/startup_logging"
)

// RunConsumer runs partitionConsumer in a supervision loop that keeps it alive
// across failures. If Consume returns an error or panics, the panic is
// recovered, its stack is printed, and the consumer is restarted after a short
// delay. The loop only stops when ctx is canceled or the underlying kafka
// consumer is closed.
func RunConsumer(ctx context.Context, partitionConsumer *PartitionConsumer, handler HandleMessage) {
	log := sl.LoggerOf(ctx)

	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.ErrorContext(ctx, "Consumer panicked, restarting", slog.Any("panic", r))

					// printing the stack makes debugging easier
					debug.PrintStack()
				}
			}()

			slog.InfoContext(ctx, "Starting kafka consumer", slog.Any("handler", handler))
			err := partitionConsumer.Consume(ctx, handler)
			if err != nil {
				log.ErrorContext(ctx, "Consumer stopped with error, restarting", slog.String("error", err.Error()))
			}
		}()

		if err := ctx.Err(); err != nil {
			log.InfoContext(ctx, "Stopping consumer, Context is close", sl.Error(err))
			return
		}

		if partitionConsumer.Consumer.IsClosed() {
			log.InfoContext(ctx, "Stopping consumer, underlying kafka consumer is close")
			return
		}

		time.Sleep(5 * time.Second)
	}
}
