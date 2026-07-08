// Package tls provides goroutine-local storage (thread-local storage).
// Values are keyed by their type and scoped to the current goroutine.
package tls

import (
	"sync"

	"github.com/flachnetz/startup/v2/lib/goid"
)

var tls sync.Map

type tlsStore map[any]any

type keyType[V any] struct{}

// Put stores a value in the current goroutine's local storage, keyed by its type.
// Calling Put multiple times with the same type overwrites the previous value.
func Put[V any](value V) {
	id := goid.Get()

	values, ok := tls.Load(id)
	if !ok {
		values = tlsStore{}
		tls.Store(id, values)
	}

	values.(tlsStore)[keyType[V]{}] = value
}

// Get retrieves a value from the current goroutine's local storage by type.
// Returns the value and true if found, or the zero value and false otherwise.
func Get[V any]() (V, bool) {
	id := goid.Get()

	values, ok := tls.Load(id)
	if !ok {
		var vZero V
		return vZero, false
	}

	value, ok := values.(tlsStore)[keyType[V]{}]
	if !ok {
		var vZero V
		return vZero, false
	}

	return value.(V), true
}

// Clear removes the value of the given type from the current goroutine's local storage.
// If no values remain for the goroutine, the entire store entry is cleaned up.
func Clear[V any]() {
	id := goid.Get()

	values, ok := tls.Load(id)
	if !ok {
		return
	}

	store := values.(tlsStore)

	delete(store, keyType[V]{})

	if len(store) == 0 {
		// store is now empty for the current go routine,
		// remove it
		tls.Delete(id)
	}
}
