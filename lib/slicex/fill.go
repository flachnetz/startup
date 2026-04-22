package slicex

// Fill fills the slice with the given number of values
func Fill[T any](value T, number int) []T {
	result := make([]T, number)
	for i := range number {
		result[i] = value
	}
	return result
}
