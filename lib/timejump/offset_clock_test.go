package timejump

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flachnetz/startup/v2/lib/clock"
	"github.com/go-co-op/gocron/v2"
	"github.com/stretchr/testify/require"
)

func jumpOffset(offset *atomic.Pointer[time.Duration], oc *offsetClock, by time.Duration) {
	next := *offset.Load() + by
	offset.Store(new(next))
	oc.onJump()
}

func TestOffsetClock_JumpFiresDueTimersOnceAtNewTime(t *testing.T) {
	var offset atomic.Pointer[time.Duration]
	offset.Store(new(0 * time.Second))
	oc := newOffsetClock(&offset)

	seen := make(chan time.Time, 4)
	oc.AfterFunc(15*time.Minute, func() {
		seen <- oc.Now()
	})

	var stoppedFired atomic.Bool
	stopped := oc.AfterFunc(15*time.Minute, func() {
		stoppedFired.Store(true)
	})
	require.True(t, stopped.Stop())

	jump := 2 * time.Hour
	expectedMin := oc.Now().Add(jump - time.Second)
	jumpOffset(&offset, oc, jump)

	select {
	case got := <-seen:
		require.True(t, !got.Before(expectedMin), "expected callback to see jumped time >= %s, got %s", expectedMin, got)
	case <-time.After(time.Second):
		t.Fatal("AfterFunc timer did not fire after jump")
	}

	time.Sleep(30 * time.Millisecond)
	require.Empty(t, seen, "timer should fire only once")
	require.False(t, stoppedFired.Load(), "stopped timer must not be resurrected by a jump")
}

func TestOffsetClock_TimerFiresOnceAndDoesNotResurrectStopped(t *testing.T) {
	var offset atomic.Pointer[time.Duration]
	offset.Store(new(0 * time.Second))
	oc := newOffsetClock(&offset)

	active := oc.Timer(15 * time.Minute)
	stopped := oc.Timer(15 * time.Minute)
	require.True(t, stopped.Stop())

	jumpOffset(&offset, oc, 2*time.Hour)

	select {
	case <-active.C:
	case <-time.After(time.Second):
		t.Fatal("Timer did not fire after jump")
	}

	select {
	case <-stopped.C:
		t.Fatal("stopped Timer was resurrected by jump")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestOffsetClock_PartialJumpShortensRemainingWait(t *testing.T) {
	var offset atomic.Pointer[time.Duration]
	offset.Store(new(0 * time.Second))
	oc := newOffsetClock(&offset)

	ch := oc.After(1 * time.Hour)
	jumpOffset(&offset, oc, 1*time.Hour-50*time.Millisecond)

	select {
	case got := <-ch:
		require.True(t, got.After(time.Now().Add(50*time.Minute)), "After channel should carry jumped time")
	case <-time.After(time.Second):
		t.Fatal("After timer was not shortened by partial jump")
	}
}

func TestOffsetClock_WithTimeoutExpiresOnJump(t *testing.T) {
	var offset atomic.Pointer[time.Duration]
	offset.Store(new(0 * time.Second))
	oc := newOffsetClock(&offset)

	ctx, cancel := oc.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	jumpOffset(&offset, oc, 1*time.Hour)

	select {
	case <-ctx.Done():
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("WithTimeout context did not expire after jump")
	}
}

func TestOffsetClock_GocronBigJumpRunsOnceAtTargetTime(t *testing.T) {
	var offset atomic.Pointer[time.Duration]
	offset.Store(new(0 * time.Second))
	oc := newOffsetClock(&offset)

	scheduler, err := gocron.NewScheduler(gocron.WithClock(clock.ToClockworkClock(oc)))
	require.NoError(t, err)
	defer func() { _ = scheduler.Shutdown() }()

	runs := make(chan time.Time, 16)
	_, err = scheduler.NewJob(
		gocron.DurationJob(15*time.Minute),
		gocron.NewTask(func() {
			runs <- oc.Now()
			time.Sleep(30 * time.Millisecond)
		}),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
		gocron.WithStartAt(gocron.WithStartImmediately()),
	)
	require.NoError(t, err)
	scheduler.Start()

	// Wait for the initial immediate run to finish.
	select {
	case <-runs:
	case <-time.After(time.Second):
		t.Fatal("initial gocron run did not start")
	}
	time.Sleep(50 * time.Millisecond)

	// 15-day jump, then 7-year jump: each must complete immediately and run exactly once at the jumped time.
	for _, jump := range []time.Duration{15 * 24 * time.Hour, 7 * 365 * 24 * time.Hour} {
		expectedMin := oc.Now().Add(jump - time.Second)
		start := time.Now()
		jumpOffset(&offset, oc, jump)
		require.Less(t, time.Since(start), 100*time.Millisecond, "jump should not replay intermediate ticks")

		select {
		case got := <-runs:
			require.True(t, !got.Before(expectedMin), "gocron pass saw %s, want >= %s", got, expectedMin)
		case <-time.After(time.Second):
			t.Fatalf("gocron job did not run after %s jump", jump)
		}

		// Wait past the 30ms simulated pass and ensure no extra storm passes fired.
		time.Sleep(80 * time.Millisecond)
		require.Empty(t, runs, "jump should trigger exactly one pass, not an intermediate storm")
	}
}

func TestOffsetClock_GocronShortIntervalFromCompletionDoesNotSpin(t *testing.T) {
	var offset atomic.Pointer[time.Duration]
	offset.Store(new(0 * time.Second))
	oc := newOffsetClock(&offset)

	scheduler, err := gocron.NewScheduler(gocron.WithClock(clock.ToClockworkClock(oc)))
	require.NoError(t, err)
	defer func() { _ = scheduler.Shutdown() }()

	runs := make(chan time.Time, 16)
	job, err := scheduler.NewJob(
		gocron.DurationJob(100*time.Millisecond),
		gocron.NewTask(func() {
			runs <- oc.Now()
		}),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
		gocron.WithStartAt(gocron.WithStartImmediately()),
		gocron.WithIntervalFromCompletion(),
	)
	require.NoError(t, err)
	scheduler.Start()

	select {
	case <-runs:
	case <-time.After(time.Second):
		t.Fatal("initial gocron run did not start")
	}

	// Without WithIntervalFromCompletion, a 7-year jump on a 100ms DurationJob
	// would spin ~2.2 billion iterations in gocron's advancePastNow.
	jump := 7 * 365 * 24 * time.Hour
	expectedMin := oc.Now().Add(jump - time.Second)
	jumpOffset(&offset, oc, jump)

	select {
	case got := <-runs:
		require.True(t, !got.Before(expectedMin), "post-jump run saw %s, want >= %s", got, expectedMin)
	case <-time.After(time.Second):
		t.Fatal("gocron job did not run after 7-year jump")
	}

	// NextRun queries the scheduler goroutine; it times out if the scheduler is stuck in advancePastNow.
	require.Eventually(t, func() bool {
		next, err := job.NextRun()
		return err == nil && !next.Before(expectedMin)
	}, time.Second, 10*time.Millisecond, "next run should be scheduled at jumped time >= %s", expectedMin)
}
