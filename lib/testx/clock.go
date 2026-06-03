package testx

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	cl "github.com/flachnetz/startup/v2/lib/clock"
	"github.com/flachnetz/startup/v2/lib/tz"
)

func MockClock(t *testing.T) *clock.Mock {
	if _, ok := cl.GlobalClock.(*clock.Mock); ok {
		panic("MockClock is not supported in concurrent tests")
	}

	prevClock := cl.GlobalClock
	t.Cleanup(func() { cl.GlobalClock = prevClock })

	mockClock := clock.NewMock()
	mockClock.Set(time.Date(2026, 1, 1, 9, 0, 0, 0, tz.UTC))
	cl.GlobalClock = mockClock

	return mockClock
}
