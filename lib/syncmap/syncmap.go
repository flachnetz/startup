package syncmap

import (
	"sync"
)

type SyncMap[K comparable, V any] struct {
	valuesMu sync.RWMutex
	values   map[K]V
}

func (c *SyncMap[K, V]) Get(key K) (V, bool) {
	c.valuesMu.RLock()
	defer c.valuesMu.RUnlock()

	value, ok := c.values[key]
	return value, ok
}

func (c *SyncMap[K, V]) Set(key K, value V) {
	c.valuesMu.Lock()
	defer c.valuesMu.Unlock()

	if c.values == nil {
		c.values = map[K]V{}
	}

	c.values[key] = value
}
