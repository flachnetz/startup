package testx

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	cl "github.com/flachnetz/startup/v2/lib/clock"
	"github.com/flachnetz/startup/v2/lib/tz"
)

// MockClock replaces the global clock with a mock fixed at 2026-01-01 09:00:00 UTC
// and returns it for advancing time during the test. The previous clock is restored
// on test cleanup. It panics if a mock clock is already installed, since the global
// clock cannot be shared across concurrent tests.
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
