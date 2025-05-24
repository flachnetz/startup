package pg

import "context"

// ctx key to disable tracing
var (
	// DisableTracingKey is the context key to disable tracing
	DisableTracingKey = &struct{}{}
)

func NoTraceCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, DisableTracingKey, true)
}
