package clock

import (
	"context"
	"github.com/benbjohnson/clock"
	"github.com/oklog/ulid"
	"log/slog"
	"math/rand"
	"sync"
)

var GlobalClock clock.Clock = realtimeClock{clock.New()}

// the monotonic instance is not thread safe
var (
	monotonicLock = sync.Mutex{}
	monotonic     = ulid.Monotonic(rand.New(rand.NewSource(GlobalClock.Now().UnixNano())), 0)
)

func GenerateId() string {
	monotonicLock.Lock()
	defer monotonicLock.Unlock()

	id := ulid.MustNew(ulid.Timestamp(GlobalClock.Now()), monotonic)
	return id.String()
}

func AdjustTimeInLog(ctx context.Context, record slog.Record) (slog.Record, bool, error) {
	if _, ok := GlobalClock.(realtimeClock); !ok {
		// update only if clock is not set to the "realtime clock"
		record.Time = GlobalClock.Now()
	}

	return record, true, nil
}

type realtimeClock struct {
	clock.Clock
}
