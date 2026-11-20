package clock

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
)

type Clock = clock.Clock

// global is the clock GlobalClock points to; Set swaps the clock it delegates to.
var global = newGlobalClock(realtimeClock{clock.New()})

// GlobalClock is the process-wide clock. It delegates every call to the clock installed by Set,
// so goroutines reading it while a test or timejump swaps the clock do not race.
// Assigning GlobalClock directly bypasses Set: it is a data race with every background reader,
// and later Set calls (testx.MockClock, timejump) no longer reach readers of GlobalClock.
var GlobalClock Clock = global

// Set installs c as the clock behind GlobalClock and returns the clock it replaces.
func Set(c Clock) Clock {
	return *global.current.Swap(&c)
}

type globalClock struct {
	current atomic.Pointer[Clock]
}

func newGlobalClock(c Clock) *globalClock {
	g := &globalClock{}
	g.current.Store(&c)
	return g
}

func (g *globalClock) get() Clock { return *g.current.Load() }

func (g *globalClock) After(d time.Duration) <-chan time.Time { return g.get().After(d) }

func (g *globalClock) AfterFunc(d time.Duration, f func()) *clock.Timer {
	return g.get().AfterFunc(d, f)
}

func (g *globalClock) Now() time.Time                        { return g.get().Now() }
func (g *globalClock) Since(t time.Time) time.Duration       { return g.get().Since(t) }
func (g *globalClock) Until(t time.Time) time.Duration       { return g.get().Until(t) }
func (g *globalClock) Sleep(d time.Duration)                 { g.get().Sleep(d) }
func (g *globalClock) Tick(d time.Duration) <-chan time.Time { return g.get().Tick(d) }
func (g *globalClock) Ticker(d time.Duration) *clock.Ticker  { return g.get().Ticker(d) }
func (g *globalClock) Timer(d time.Duration) *clock.Timer    { return g.get().Timer(d) }

func (g *globalClock) WithDeadline(parent context.Context, d time.Time) (context.Context, context.CancelFunc) {
	return g.get().WithDeadline(parent, d)
}

func (g *globalClock) WithTimeout(parent context.Context, t time.Duration) (context.Context, context.CancelFunc) {
	return g.get().WithTimeout(parent, t)
}

type realtimeClock struct {
	clock.Clock
}

// Now returns the current time in UTC, this is different to the original clock implementation which returned the local time.
func (receiver realtimeClock) Now() time.Time {
	return receiver.Clock.Now().UTC()
}

// We always want to use UTC time.
func init() {
	time.Local = time.UTC
}
