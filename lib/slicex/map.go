package slicex

// Map applies the mapper function to each item of the input slice and returns a new slice
// with the mapped values.
func Map[T any, R any](input []T, mapper func(T) R) []R {
	result := MakeWithCapacity[R](len(input))

	for _, value := range input {
		result = append(result, mapper(value))
	}

	return result
}
