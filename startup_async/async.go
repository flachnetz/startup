package startup_async

import "context"

type Future[T any] interface {
	Await(ctx context.Context) (T, error)
}

type future[T any] struct {
	await func(ctx context.Context) (T, error)
}

func (f future[T]) Await(ctx context.Context) (T, error) {
	return f.await(ctx)
}

func Run[T any](f func() (T, error)) Future[T] {
	var result T
	var err error
	c := make(chan struct{})
	go func() {
		defer close(c)
		result, err = f()
	}()
	return future[T]{
		await: func(ctx context.Context) (T, error) {
			select {
			case <-ctx.Done():
				if ctxErr := ctx.Err(); ctxErr != nil {
					return result, ctxErr
				}
				return result, err
			case <-c:
				return result, err
			}
		},
	}
}
