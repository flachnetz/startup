package lib

import (
	"context"
	"golang.org/x/exp/constraints"
)

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

type noCancelContext struct {
	context.Context
	lookupContext context.Context
}

// NoCancelContext creates a new context that derives from context.Background, while getting
// all values from the given lookupContext. So even if the lookupContext has a timeout, the
// new context will not inherit this timeout.
func NoCancelContext(lookupContext context.Context) context.Context {
	return &noCancelContext{context.Background(), lookupContext}
}

func (ctx *noCancelContext) Value(key interface{}) interface{} {
	return ctx.lookupContext.Value(key)
}
