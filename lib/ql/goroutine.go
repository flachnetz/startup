package ql

import (
	"context"
	"log/slog"
	"runtime"
	"sync"

	"github.com/flachnetz/startup/v2/lib/goid"
	sl "github.com/flachnetz/startup/v2/startup_logging"
)

var goroutineIdCache sync.Map

func reentrantWarn(ctx context.Context) func() {
	gid := goid.Get()

	prev, _ := goroutineIdCache.Swap(gid, true)
	if prev != nil {
		buf := make([]byte, 16*1024)
		n := runtime.Stack(buf, false)

		trace := string(buf[:n])
		sl.LoggerOf(ctx).WarnContext(ctx,
			"Transaction in goroutine already exists",
			slog.Uint64("goroutineId", uint64(gid)),
			slog.String("trace", trace))
	}

	return func() {
		// unceremoniously remove the value for the current goroutine
		goroutineIdCache.Delete(gid)
	}
}
