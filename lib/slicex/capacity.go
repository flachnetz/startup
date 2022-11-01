package slicex

// MakeWithCapacity exists to simply create a slice with the provided pre-allocated capacity.
// Why not just use `make([]T, cap)` you might ask? Well, cause that's broken again. You've missed
// the zero indicating a length of zero - again. Thanks go.
func MakeWithCapacity[T any](cap int) []T {
	return make([]T, 0, cap)
}
