package goid

import (
	"bytes"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"unsafe"
)

var pool = sync.Pool{
	New: func() any { return new([64]byte) },
}

// Adjusted from code from the http2 module of the standard library. Thanks golang.
// https://github.com/golang/net/blob/master/http2/gotrack.go#L51
//
// See also:
// https://www.reddit.com/r/golang/comments/77mooi/why_golang_refuse_to_provide_goroutine_id_then/?rdt=63214
func getViaStack() Id {
	bp := pool.Get().(*[64]byte)
	defer pool.Put(bp)

	b := (*bp)[:]
	b = b[:runtime.Stack(b, false)]

	// Remove the prefix "goroutine " of "goroutine 4707 ["
	b = bytes.TrimPrefix(b, []byte("goroutine "))

	// Parse the 4707 out of "goroutine 4707 ["
	i := bytes.IndexByte(b, ' ')
	if i < 0 {
		bClone := slices.Clone(b)
		panic(fmt.Sprintf("No space found in %q", bClone))
	}

	str := unsafe.String(&b[0], i)

	n, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		bClone := slices.Clone(b)
		panic(fmt.Sprintf("Failed to parse goroutine ID out of %q: %v", bClone, err))
	}
	return Id(n)
}

func init() {
	// validate that the method works and will not just crash sometime later
	// at runtime. fail fast!
	getViaStack()
}
