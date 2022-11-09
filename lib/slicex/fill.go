package slicex

func Fill[T any](value T, number int) []T {
	result := make([]T, number)
	for i := 0; i < number; i++ {
		result[i] = value
	}
	return result
}
