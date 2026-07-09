package clock

import (
	"time"

	"github.com/benbjohnson/clock"
)

type Clock = clock.Clock

var GlobalClock clock.Clock = realtimeClock{clock.New()}

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
