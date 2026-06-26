package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-playground/validator/v10"

	sl "github.com/flachnetz/startup/v2/startup_logging"

	"github.com/labstack/echo/v5"
)

// ErrorResponse is the JSON body returned to clients when a request fails.
// It mirrors the public fields of Error and additionally carries the HTTP
// status code to use for the response.
type ErrorResponse struct {
	Code        ErrorCode  `json:"code"`
	Description string     `json:"description"`
	Attributes  Attributes `json:"attributes"`

	// The status code, defaults to http.StatusInternalServerError if not set
	StatusCode int `json:"-"`
}

// ErrorMapper translates an arbitrary error into an ErrorResponse. It returns
// false to signal that it does not handle the given error, in which case the
// default mapping rules are applied.
type ErrorMapper func(err error) (ErrorResponse, bool)

// ErrorHandler returns an echo error handler. It applies the mapper to the
// error first and falls back to the default error mapping rules if the mapper
// returns false or is not defined. Errors are always logged, and if the
// response has already been committed it can only log, not write a body.
func ErrorHandler(mapper ErrorMapper) echo.HTTPErrorHandler {
	return func(c *echo.Context, err error) {
		ctx := c.Request().Context()

		if isCommitted(c) {
			// the response is already written,
			// we can not really handle the error here.
			slog.ErrorContext(
				ctx, "An error occurred, response already written",
				slog.String("method", c.Request().Method),
				slog.String("path", c.Path()),
				sl.Error(err),
			)

			return
		}

		var ok bool
		var resp ErrorResponse

		if mapper != nil {
			// try custom error mapper first
			resp, ok = mapper(err)
		}

		if !ok {
			// nope, did not work
			resp = mapToErrorResponse(err)
		}

		slog.ErrorContext(
			ctx, "An error occurred",
			slog.String("method", c.Request().Method),
			slog.String("path", c.Path()),
			slog.Int("httpStatus", resp.StatusCode),
			sl.Error(err),
		)

		switch c.Request().Method {
		case http.MethodHead, http.MethodOptions:
			// these methods must not carry a response body
			_ = c.NoContent(resp.StatusCode)

		default:
			_ = c.JSON(resp.StatusCode, resp)
		}
	}
}

// mapToErrorResponse applies the default mapping rules, turning well-known
// error types into an appropriate ErrorResponse. Anything it cannot classify
// is reported as a generic internal server error so that internal details are
// never leaked to the client.
func mapToErrorResponse(err error) (resp ErrorResponse) {
	resp = ErrorResponse{
		Code:        ErrorCodeInternal,
		Description: "internal server error",
		Attributes:  Attributes{},
		StatusCode:  http.StatusInternalServerError,
	}

	// our own structured Error already carries everything we need
	if err, ok := errors.AsType[Error](err); ok {
		resp = err.toErrorResponse()
		return
	}

	// a cancelled context usually means the client went away
	if errors.Is(err, context.Canceled) {
		resp = ErrCanceled.toErrorResponse()
		return
	}

	// a deadline exceeded maps to a timeout
	if errors.Is(err, context.DeadlineExceeded) {
		resp = ErrTimeout.toErrorResponse()
		return
	}

	// request validation failures: expose the offending fields as attributes
	if err, ok := errors.AsType[validator.ValidationErrors](err); ok {
		resp.StatusCode = http.StatusBadRequest
		resp.Description = "validating request"

		for _, field := range err {
			resp.Attributes["field."+field.Field()] = field.Error()
		}

		return
	}

	// request binding/parsing failures: report the affected field
	if err, ok := errors.AsType[*echo.BindingError](err); ok {
		resp.StatusCode = http.StatusBadRequest
		resp.Description = "parsing request failed"
		resp.Attributes["field"] = err.Field
		return
	}

	// any other echo error carries its own status code
	if err, ok := errors.AsType[echoErrorInterface](err); ok {
		resp.StatusCode = err.StatusCode()
		resp.Description = err.Error()
		return
	}

	// we don't know how to handle this any better
	return ErrInternalServerError.toErrorResponse()
}

// isCommitted reports whether the response has already been written to the
// client, meaning we can no longer change the status code or body.
func isCommitted(c *echo.Context) bool {
	response, err := echo.UnwrapResponse(c.Response())
	return err == nil && response != nil && response.Committed
}

// echoErrorInterface matches the echo error type, allowing us to extract its
// HTTP status code without depending on the concrete type.
type echoErrorInterface interface {
	StatusCode() int
	Error() string
	Wrap(err error) error
}
