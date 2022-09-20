package clock

import (
	"github.com/benbjohnson/clock"
	"github.com/oklog/ulid"
	"math/rand"
	"sync"
)

var GlobalClock = clock.New()

// the monotonic instance is not thread safe
var monotonicLock = sync.Mutex{}
var monotonic = ulid.Monotonic(rand.New(rand.NewSource(GlobalClock.Now().UnixNano())), 0)

func GenerateId() string {
	monotonicLock.Lock()
	defer monotonicLock.Unlock()

	id := ulid.MustNew(ulid.Timestamp(GlobalClock.Now()), monotonic)
	return id.String()
}
