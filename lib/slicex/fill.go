package slicex

// Fill fills the slice with the given number of values
func Fill[T any](value T, number int) []T {
	result := make([]T, number)
	for i := 0; i < number; i++ {
		result[i] = value
	}
	return result
}
