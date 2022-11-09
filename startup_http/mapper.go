package startup_http

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// A source returns a value for a given key. A source might be backed
// by a map or something else.
type Source interface {
	Get(key string) (string, bool)
}

// Take the given string value, converts it and assigns it to the
// target value. It is expected, that the target value.
type Setter func(value string, target reflect.Value) error

// Type registry for custom setters. Only modify in your modules
// init function.
var CustomTypes = map[reflect.Type]Setter{
	reflect.TypeOf(time.Time{}): setterTime,
}

// cache that maps a combination of type and tag name to a list of
// operations to apply during mapping
var (
	typeCache     = make(map[string]map[reflect.Type][]operation)
	typeCacheLock sync.Mutex
)

type operation struct {
	// the key to look use to lookup the value in the source
	key string

	// a setter instance that will apply a value to a given target
	setter Setter
}

// Runs the operation by looking up the value and passing it to the setter.
// If no value could be found in the source, this method will not
// modify the target.
func (op *operation) Apply(source Source, target reflect.Value) error {
	sourceValue, ok := source.Get(op.key)
	if !ok {
		return nil
	}

	return op.setter(sourceValue, target)
}

// Runs a mapping operation on the given target. The target
// must be a pointer to a struct.
func Map(tag string, source Source, target interface{}) error {
	ptrTarget := reflect.ValueOf(target)
	if ptrTarget.Kind() != reflect.Ptr {
		return fmt.Errorf("expected pointer to struct, got %s", ptrTarget.Kind())
	}

	value := ptrTarget.Elem()
	valueType := value.Type()
	if valueType.Kind() != reflect.Struct {
		return fmt.Errorf("expected struct, got %s", valueType)
	}

	ops := analyzeStructCached(valueType, tag)

	for _, op := range ops {
		err := op.Apply(source, value)
		if err != nil {
			return errors.WithMessage(err, fmt.Sprintf("setting %s", op.key))
		}
	}

	return nil
}

// Analyzes the the given struct type and returns a list of operations
// to apply for mapping. The given tag name will be used for mapping.
// This method uses a cache to speed up getting of operations.
func analyzeStructCached(t reflect.Type, tag string) []operation {
	typeCacheLock.Lock()
	cached := typeCache[tag][t]
	typeCacheLock.Unlock()

	if cached != nil {
		return cached
	}

	ops := analyzeStruct(t, tag)

	typeCacheLock.Lock()

	// get the cache for the current tag
	cache := typeCache[tag]
	if cache == nil {
		// no cache yet, create a new one
		cache = make(map[reflect.Type][]operation)
		typeCache[tag] = cache
	}

	// update ops for this type
	cache[t] = ops

	typeCacheLock.Unlock()

	return ops
}

// Analyzes the given struct and converts it into a list of
// operations for mapping. This method will not use any caching.
func analyzeStruct(t reflect.Type, tag string) []operation {
	var ops []operation

	// track the keys we've seen so we can prevent duplicates
	seen := map[string]bool{}

	for idx := 0; idx < t.NumField(); idx++ {
		field := t.Field(idx)

		// skip unexported fields
		if field.PkgPath != "" {
			continue
		}

		// get the source key value from the tag and skip fields
		// with no or an empty tag value.
		tagValue := field.Tag.Get(tag)
		if tagValue == "" {
			continue
		}

		// check that no tag is used twice.
		if seen[tagValue] {
			panic(fmt.Errorf("key %s mapped twice", tagValue))
		}

		seen[tagValue] = true

		ops = append(ops, operation{
			key:    tagValue,
			setter: setterField(idx, setterOf(field.Type)),
		})
	}

	return ops
}

// Resolves a type into a setter. This method checks for a custom setter
// in CustomTypes first before falling back on generic ones.
func setterOf(t reflect.Type) Setter {
	if setter, ok := CustomTypes[t]; ok {
		return setter
	}

	switch t.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return setterInt

	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return setterUint

	case reflect.Bool:
		return setterBool

	case reflect.String:
		return setterString

	case reflect.Float32, reflect.Float64:
		return setterFloat

	default:
		panic(fmt.Errorf("no setter for type %s", t))
	}
}

func setterField(idx int, setter Setter) Setter {
	return func(value string, target reflect.Value) error {
		err := setter(value, target.Field(idx))
		if err != nil {
			fieldName := target.Type().Field(idx).Name
			return errors.WithMessage(err, fmt.Sprintf("setting field %s", fieldName))
		}

		return nil
	}
}

func setterString(value string, target reflect.Value) error {
	target.SetString(value)
	return nil
}

func setterTime(value string, target reflect.Value) error {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return errors.WithMessage(err, "parsing timestamp")
	}

	target.Set(reflect.ValueOf(parsed))
	return nil
}

func setterBool(value string, target reflect.Value) error {
	b := value == "true" || strings.ToLower(value) == "true"
	target.SetBool(b)
	return nil
}

func setterInt(value string, target reflect.Value) error {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return errors.WithMessage(err, "parsing int value")
	}

	target.SetInt(parsed)
	return nil
}

func setterUint(value string, target reflect.Value) error {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return errors.WithMessage(err, "parsing uint value")
	}

	target.SetUint(parsed)
	return nil
}

func setterFloat(value string, target reflect.Value) error {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return errors.WithMessage(err, "parsing float value")
	}

	target.SetFloat(parsed)
	return nil
}

// Default implementation for a source that uses a
// simple string map for lookup.
type MapSource map[string]string

func (m MapSource) Get(key string) (string, bool) {
	value, ok := m[key]
	return value, ok
}
