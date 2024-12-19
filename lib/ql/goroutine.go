package ql

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"unsafe"

	"github.com/flachnetz/startup/v2/startup_logrus"
)

var goroutineIdCache sync.Map

func reentrantWarn(ctx context.Context) func() {
	gid := currentGoroutineId()

	prev, _ := goroutineIdCache.Swap(gid, true)
	if prev != nil {
		buf := make([]byte, 16*1024)
		n := runtime.Stack(buf, false)

		trace := string(buf[:n])
		startup_logrus.LoggerOf(ctx).Warnf("Transaction in goroutine %d already exists:\n%s", gid, trace)
	}

	return func() {
		// unceremoniously remove the value for the current goroutine
		goroutineIdCache.Delete(gid)
	}
}

// Adjusted from code from the http2 module of the standard library. Thanks golang.
// https://github.com/golang/net/blob/master/http2/gotrack.go#L51
//
// See also:
// https://www.reddit.com/r/golang/comments/77mooi/why_golang_refuse_to_provide_goroutine_id_then/?rdt=63214
func currentGoroutineId() uint64 {
	var bp [64]byte

	b := bp[:]
	b = b[:runtime.Stack(b, false)]

	// Remove the prefix "goroutine " of "goroutine 4707 ["
	b = bytes.TrimPrefix(b, []byte("goroutine "))

	// Parse the 4707 out of "goroutine 4707 ["
	i := bytes.IndexByte(b, ' ')
	if i < 0 {
		panic(fmt.Sprintf("No space found in %q", b))
	}

	str := unsafe.String(&b[0], i)

	n, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse goroutine ID out of %q: %v", b, err))
	}

	return n
}

func init() {
	// validate that the method works and will not just crash sometime later
	// at runtime. fail fast!
	currentGoroutineId()
}
