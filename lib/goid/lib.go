package goid

import (
	"fmt"
	"unsafe"
)

var goidOffset uintptr

func init() {
	gType := typeByString("runtime.g")
	if gType == nil {
		panic("cannot find runtime.g type")
	}

	fieldGoid, ok := gType.FieldByName("goid")
	if !ok {
		panic("cannot find goid field on runtime.g")
	}

	goidOffset = fieldGoid.Offset

	// validate that we can call Get() without crash. Go currently runs all
	// init() functions always on the goroutine 1, i.e. the main goroutine.
	if id := Get(); id != 1 {
		panic(fmt.Errorf("sanity check: init() not run on goroutine 1, got %d", id))
	}
}

type Id uint64

func Get() Id {
	// HINT: if this does not work, you can always redirect to getViaStack(), this will
	// not use any unsafe code to get the go routine, but will be a lot slower.

	// get the current go routine
	g := getg()

	// calculate pointer to goid field
	ptrToGoid := (*uint64)(unsafe.Pointer(uintptr(g) + goidOffset))

	// and read value
	return Id(*ptrToGoid)
}
