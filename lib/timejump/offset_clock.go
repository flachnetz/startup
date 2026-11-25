package timejump

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clock2 "github.com/benbjohnson/clock"
	"github.com/flachnetz/startup/v2/lib/clock"
)

var _ clock.Clock = (*offsetClock)(nil)

type trackedTimer struct {
	timer    *clock2.Timer
	deadline time.Time
}

// offsetClock implements clock.Clock by adding a forward-only offset to wall time.
// Unlike benbjohnson/clock.Mock.Set, jumping forward updates the offset first and
// fires each due timer once at the new time instead of replaying intermediate ticks.
type offsetClock struct {
	real   clock2.Clock
	offset *atomic.Pointer[time.Duration]

	mu     sync.Mutex
	nextID uint64
	timers map[uint64]trackedTimer
}

func newOffsetClock(offset *atomic.Pointer[time.Duration]) *offsetClock {
	return &offsetClock{
		real:   clock2.New(),
		offset: offset,
		timers: make(map[uint64]trackedTimer),
	}
}

func (c *offsetClock) currentOffset() time.Duration {
	if c.offset == nil {
		return 0
	}
	if off := c.offset.Load(); off != nil {
		return *off
	}
	return 0
}

func (c *offsetClock) Now() time.Time {
	return time.Now().Add(c.currentOffset()).UTC()
}

func (c *offsetClock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

func (c *offsetClock) Until(t time.Time) time.Duration {
	return t.Sub(c.Now())
}

func (c *offsetClock) AfterFunc(d time.Duration, f func()) *clock2.Timer {
	if d <= 0 {
		return c.real.AfterFunc(d, f)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.Now()
	id := c.nextID
	c.nextID++

	t := c.real.AfterFunc(d, func() {
		c.mu.Lock()
		delete(c.timers, id)
		c.mu.Unlock()
		f()
	})
	c.timers[id] = trackedTimer{
		timer:    t,
		deadline: now.Add(d),
	}
	return t
}

func (c *offsetClock) Timer(d time.Duration) *clock2.Timer {
	if d <= 0 {
		return c.real.Timer(d)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.Now()
	if len(c.timers) > 64 {
		c.pruneExpiredLocked(now)
	}

	id := c.nextID
	c.nextID++

	t := c.real.Timer(d)
	c.timers[id] = trackedTimer{
		timer:    t,
		deadline: now.Add(d),
	}
	return t
}

func (c *offsetClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.AfterFunc(d, func() {
		ch <- c.Now()
	})
	return ch
}

func (c *offsetClock) Sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	<-c.After(d)
}

// shortcut: Ticker runs on real intervals because benbjohnson/clock.Ticker has unexported fields;
// upgrade if a Ticker consumer needs immediate ticks on timejumps.
func (c *offsetClock) Ticker(d time.Duration) *clock2.Ticker {
	return c.real.Ticker(d)
}

func (c *offsetClock) Tick(d time.Duration) <-chan time.Time {
	return c.real.Tick(d)
}

func (c *offsetClock) WithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return c.WithDeadline(parent, c.Now().Add(timeout))
}

func (c *offsetClock) WithDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	if cur, ok := parent.Deadline(); ok && cur.Before(deadline) {
		return context.WithCancel(parent)
	}

	ctx := &offsetTimerCtx{
		clock:    c,
		parent:   parent,
		deadline: deadline,
		done:     make(chan struct{}),
	}

	if parent.Done() != nil {
		go func() {
			select {
			case <-parent.Done():
				ctx.cancel(parent.Err())
			case <-ctx.done:
			}
		}()
	}

	dur := c.Until(deadline)
	if dur <= 0 {
		ctx.cancel(context.DeadlineExceeded)
		return ctx, func() {}
	}

	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.err == nil {
		ctx.timer = c.AfterFunc(dur, func() {
			ctx.cancel(context.DeadlineExceeded)
		})
	}
	return ctx, func() { ctx.cancel(context.Canceled) }
}

// onJump reconciles all tracked timers after the offset has moved forward.
// Callers must store the new offset before calling onJump so every awakened
// timer immediately observes the jumped Now().
func (c *offsetClock) onJump() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.Now()
	for id, entry := range c.timers {
		if !entry.deadline.After(now) {
			delete(c.timers, id)
			if entry.timer.Stop() {
				entry.timer.Reset(0)
			}
			continue
		}

		if entry.timer.Stop() {
			entry.timer.Reset(entry.deadline.Sub(now))
		} else {
			delete(c.timers, id)
		}
	}
}

func (c *offsetClock) pruneExpiredLocked(now time.Time) {
	for id, entry := range c.timers {
		if !entry.deadline.After(now) {
			delete(c.timers, id)
		}
	}
}

type offsetTimerCtx struct {
	mu       sync.Mutex
	clock    clock.Clock
	parent   context.Context
	deadline time.Time
	done     chan struct{}
	err      error
	timer    *clock2.Timer
}

func (c *offsetTimerCtx) cancel(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return
	}
	c.err = err
	close(c.done)
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
}

func (c *offsetTimerCtx) Deadline() (time.Time, bool) { return c.deadline, true }
func (c *offsetTimerCtx) Done() <-chan struct{}       { return c.done }
func (c *offsetTimerCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}
func (c *offsetTimerCtx) Value(key any) any { return c.parent.Value(key) }
func (c *offsetTimerCtx) String() string {
	return fmt.Sprintf("timejump.WithDeadline(%s [%s])", c.deadline, c.deadline.Sub(c.clock.Now()))
}
