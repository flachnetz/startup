package lib

func PtrOf[T any](value T) *T {
	return &value
}
