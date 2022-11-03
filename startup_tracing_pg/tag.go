package startup_tracing_pg

import (
	"runtime"
	"strings"
)

type SkipFunc func(name string) bool

func findOutsideCaller(skip SkipFunc) string {
	var tag string

	// get the first method in the stack outside of the startup package
	pcSlice := [10]uintptr{}
	n := runtime.Callers(1, pcSlice[:])
	if n > 0 {
		frames := runtime.CallersFrames(pcSlice[:])
		for {
			frame, more := frames.Next()

			// take first one out of startup
			if !strings.Contains(frame.Function, "flachnetz/startup/") && !skip(frame.Function) {
				tag = frame.Function
				break
			}

			if !more {
				break
			}
		}
	}

	return tag
}
