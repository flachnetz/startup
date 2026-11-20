package clock

import (
	"sync"
	"testing"
	"time"

	bclock "github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"
)

// Run with -race: a background reader of GlobalClock must not race with Set.
func TestSetSwapsClockWhileReadersRun(t *testing.T) {
	mock := bclock.NewMock()
	mockTime := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	mock.Set(mockTime)

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
				_ = GlobalClock.Now()
			}
		}
	})

	prev := Set(mock)
	t.Cleanup(func() { Set(prev) })
	require.Equal(t, mockTime, GlobalClock.Now())
	for range 100 {
		Set(Set(prev))
	}
	require.Same(t, mock, Set(prev))

	close(done)
	wg.Wait()
	_, realtime := global.get().(realtimeClock)
	require.True(t, realtime)
}
