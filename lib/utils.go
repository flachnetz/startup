package lib

import "golang.org/x/exp/constraints"

func PtrOf[T any](value T) *T {
	return &value
}

func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}
