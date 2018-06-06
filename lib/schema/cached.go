package schema

import (
	"sync"

	"github.com/pkg/errors"
)

type cachedRegistry struct {
	lock        sync.Mutex
	registry    Registry
	schemaCache map[string]string
}

func NewCachedRegistry(registry Registry) Registry {
	return &cachedRegistry{
		registry:    registry,
		schemaCache: make(map[string]string),
	}
}

func (r *cachedRegistry) Get(key string) (string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// check cache
	if codec, ok := r.schemaCache[key]; ok {
		return codec, nil
	}

	// try real registry
	schema, err := r.registry.Get(key)
	if err != nil {
		return "", errors.WithMessage(err, "Could not lookup schema")
	}

	// cache codec
	r.schemaCache[key] = schema
	return schema, nil
}

func (r *cachedRegistry) Set(schema string) (key string, err error) {
	key = Hash(schema)

	r.lock.Lock()
	defer r.lock.Unlock()

	if codec, exists := r.schemaCache[key]; !exists {
		key, err := r.registry.Set(schema)
		if err != nil {
			r.schemaCache[key] = codec
		}
	}
	return key, err
}

func (r *cachedRegistry) Close() error {
	return r.registry.Close()
}
