package clock

import (
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jonboulle/clockwork"
)

var _ clockwork.Clock = adapter{}

func ToClockworkClock(clock clock.Clock) clockwork.Clock {
	return adapter{clock}
}

type adapter struct {
	clock.Clock
}

func (a adapter) NewTicker(d time.Duration) clockwork.Ticker {
	ticker := a.Clock.Ticker(d)
	return tickerAdapter{Ticker: ticker}
}

func (a adapter) NewTimer(d time.Duration) clockwork.Timer {
	timer := a.Clock.Timer(d)
	return timerAdapter{timer}
}

func (a adapter) AfterFunc(d time.Duration, f func()) clockwork.Timer {
	timer := a.Clock.AfterFunc(d, f)
	return timerAdapter{timer}
}

type tickerAdapter struct {
	*clock.Ticker
}

func (t tickerAdapter) Chan() <-chan time.Time {
	return t.Ticker.C
}

type timerAdapter struct {
	*clock.Timer
}

func (t timerAdapter) Chan() <-chan time.Time {
	return t.Timer.C
}
