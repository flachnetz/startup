package startup_http

import (
	"context"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

type HttpMiddleware func(http.Handler) http.Handler

// Takes a httprouter.Handle middleware and wraps it so it can  be used
// with http.Handler functions.
func AdaptMiddlewareForHttp(middleware HttpRouterMiddleware) HttpMiddleware {
	return func(handler http.Handler) http.Handler {
		wrapped := middleware(func(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
			handler.ServeHTTP(writer, request)
		})

		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			wrapped(writer, request, httprouter.ParamsFromContext(request.Context()))
		})
	}
}

type HttpRouterMiddleware func(httprouter.Handle) httprouter.Handle

// Takes a normal http.Handler middleware and wraps it so it can  be used
// with httprouter.Handle functions.
func AdaptMiddlewareForHttpRouter(w HttpMiddleware) HttpRouterMiddleware {
	return func(handle httprouter.Handle) httprouter.Handle {
		middleware := w(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			handle(writer, request, httprouter.ParamsFromContext(request.Context()))
		}))

		return func(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
			ctx := context.WithValue(request.Context(), httprouter.ParamsKey, params)
			middleware.ServeHTTP(writer, request.WithContext(ctx))
		}
	}
}
