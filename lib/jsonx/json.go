package jsonx

import "encoding/json"

// Unmarshal unmarshalls a value of the type T.
func Unmarshal[T any](bytes []byte) (T, error) {
	var target T
	err := json.Unmarshal(bytes, &target)
	return target, err
}
