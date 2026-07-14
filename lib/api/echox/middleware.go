package echox

import (
	"slices"

	"github.com/labstack/echo/v5"
)

func MiddlewareChain(middlewares ...echo.MiddlewareFunc) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		for _, mw := range slices.Backward(middlewares) {
			next = mw(next)
		}

		return next
	}
}
