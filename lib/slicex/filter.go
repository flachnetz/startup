package slicex

// Filter does what it says and creates a new list with all values
// which evaluate the predicate to true.
func Filter[T any](values []T, predicate func(value T) bool) []T {
	var result []T

	for _, value := range values {
		if predicate(value) {
			result = append(result, value)
		}
	}

	return result
}
