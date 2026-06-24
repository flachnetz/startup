package history

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	"github.com/flachnetz/startup/v2/lib/clock"
	sl "github.com/flachnetz/startup/v2/startup_logging"
)

type Cleanup struct {
	Interval time.Duration
	Jitter   time.Duration
	MaxAge   time.Duration
}

// Run executes cleanup on the given service in a loop waiting for Cleanup.Interval + rand(Cleanup.Jitter)
// before each Service.Cleanup. It will delete all items that are older than Cleanup.MaxAge.
func (c Cleanup) Run(ctx context.Context, service *Service) {
	logger := sl.LoggerOf(ctx)

	for ctx.Err() == nil {
		// wait until we start the next cleanup run
		delay := c.Interval
		if c.Jitter > 0 {
			delay += time.Duration(rand.Intn(int(c.Jitter)))
		}

		select {
		case <-ctx.Done():
			return

		case <-clock.GlobalClock.After(delay):
			startTime := time.Now()
			logger.Info("Starting history cleanup")

			err := service.Cleanup(ctx, service.txStarter, clock.GlobalClock.Now().Add(-c.MaxAge))

			duration := time.Since(startTime)

			if err != nil {
				logger.ErrorContext(ctx, "History cleanup failed", slog.Duration("duration", duration), sl.Error(err))
			} else {
				logger.InfoContext(ctx, "History cleanup finished", slog.Duration("duration", duration))
			}
		}
	}
}
