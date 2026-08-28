package startup_tracing_pg

import (
	"runtime"
	"strings"
)

type SkipFunc func(name string) bool

// findOutsideCaller returns the first caller outside the startup packages,
// shortened to package plus function ("payment.(*Service).FulfilOrder"): the
// value ends up in a span name, where the full module path is unreadable.
// Returns "" when the whole stack is framework code, e.g. a transaction started
// from a goroutine spawned inside startup - the goroutine root (runtime.goexit)
// is not a caller and must not be reported as one.
func findOutsideCaller(skip SkipFunc) string {
	// 32: an app stack (http -> handler -> service -> repository -> lib/ql -> pgx)
	// easily exceeds 10 frames, and a truncated stack yields no caller at all.
	pcSlice := [32]uintptr{}
	n := runtime.Callers(1, pcSlice[:])

	frames := runtime.CallersFrames(pcSlice[:n])
	for {
		frame, more := frames.Next()

		// take first one out of startup
		if !strings.Contains(frame.Function, "flachnetz/startup/") &&
			!strings.HasPrefix(frame.Function, "runtime.") &&
			!skip(frame.Function) {
			return frame.Function[strings.LastIndexByte(frame.Function, '/')+1:]
		}

		if !more {
			return ""
		}
	}
}
