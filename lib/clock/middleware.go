package clock

import (
	"context"
	"log/slog"
)

// AdjustTimeInLog is a slog middleware to adjust timestamps in logs
func AdjustTimeInLog(ctx context.Context, record slog.Record) (slog.Record, bool, error) {
	c := global.get()
	if _, ok := c.(realtimeClock); !ok {
		// update only if clock is not set to the "realtime clock"
		record.Time = c.Now()
	}

	return record, true, nil
}
