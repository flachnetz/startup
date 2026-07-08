package goid

import "unsafe"

//go:nosplit
func getg() unsafe.Pointer
