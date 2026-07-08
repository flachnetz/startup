package goid

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetIsStable(t *testing.T) {
	id := Get()
	for range 1024 {
		require.Equal(t, id, Get())
	}
}

func TestGetIsIncreasing(t *testing.T) {
	var prevId = Get()

	for range 1024 {
		var id Id

		var wg sync.WaitGroup
		wg.Go(func() { id = Get() })
		wg.Wait()

		// we expect that a the Id is just increasing
		require.Greater(t, id, prevId)

		prevId = id
	}
}

func TestGetMatchesFallback(t *testing.T) {
	for range 1024 {
		var goid, stack Id

		var wg sync.WaitGroup
		wg.Go(func() {
			goid = Get()
			stack = getViaStack()
		})
		wg.Wait()

		require.Equal(t, stack, goid)
	}
}

var blackhole Id

func BenchmarkGetViaStack(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		blackhole += getViaStack()
	}
}

func BenchmarkGet(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		blackhole += Get()
	}
}
