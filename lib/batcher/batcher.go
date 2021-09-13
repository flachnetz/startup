package batcher

import (
	"time"
)

type Batcher struct {
	MaxSize   int
	BatchTime time.Duration

	timer *time.Timer
	count int
}

// NewBatcher returns a simple Batcher that can be used to buffer & batch
// values as simple as this:
//
//  b := batcher.New(1024, 5*time.Second)
//  for {
//    select {
//      case <- b.Await():
//        b.Reset()
//        flush(messages)
//        messages = nil
//
//      case msg <- kafka.Messages():
//        b.Increment()
//        messages = append(messages, msg)
//    }
//  }
//
func New(maxSize int, batchTime time.Duration) *Batcher {
	return &Batcher{
		MaxSize:   maxSize,
		BatchTime: batchTime,
	}
}

// Increment records a new message in the batch.
func (b *Batcher) Increment() {
	b.count += 1

	if b.timer == nil {
		// no timer yet? start a new one
		b.timer = time.NewTimer(b.BatchTime)
	}
}

// Reset the Batcher after flushing.
func (b *Batcher) Reset() {
	b.count = 0
	b.timer = nil
}

// Await the Batcher flush.
func (b *Batcher) Await() <-chan time.Time {
	if b.count >= b.MaxSize {
		// if we already have a timer, stop that one now
		if b.timer != nil {
			b.timer.Stop()
			b.timer = nil
		}

		// return a channel that immediately produces a value
		resultCh := make(chan time.Time, 1)
		resultCh <- time.Now()
		return resultCh
	}

	// if we have a timer, return the timers channel
	if b.timer != nil {
		return b.timer.C
	}

	// return nil so that the Await never succeeds
	return nil
}
