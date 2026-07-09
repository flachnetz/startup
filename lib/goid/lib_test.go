package goid

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetIsStable(t *testing.T) {
	id := Get()
	for range 1_000_000 {
		require.Equal(t, id, Get())
	}
}

func TestGetIsChanging(t *testing.T) {
	seen := make(map[Id]bool, 1_000_000)

	closeCh := make(chan struct{})
	defer close(closeCh)

	for range 1_000_000 {
		idCh := make(chan Id)

		go func() {
			idCh <- Get()
			<-closeCh
		}()

		id := <-idCh

		// we expect that the id is not reused
		// while the go routine is still alive
		require.False(t, seen[id])

		seen[id] = true
	}
}

func TestGetMatchesFallback(t *testing.T) {
	for range 1_000_000 {
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
