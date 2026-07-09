package echox

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/flachnetz/startup/v2/lib/api"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v5"
)

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
				slog.String("path", c.Request().URL.Path),
				sl.Error(err),
			)

			return
		}

		var ok bool
		var resp api.ErrorResponse

		if mapper != nil {
			// try custom error mapper first
			resp, ok = mapper(err)
		}

		if !ok {
			// nope, did not work
			resp = mapToErrorResponse(err)
		}

		attrs := []any{
			slog.String("method", c.Request().Method),
			slog.String("path", c.Request().URL.Path),
			slog.Int("httpStatus", resp.StatusCode),
			sl.Error(err),
		}

		switch resp.StatusCode / 100 {
		case 4:
			slog.WarnContext(ctx, "Request failed", attrs...)
		default:
			slog.ErrorContext(ctx, "An error occurred", attrs...)
		}

		switch c.Request().Method {
		case http.MethodHead, http.MethodOptions:
			// these methods must not carry a response body
			_ = c.NoContent(resp.StatusCode)

		default:
			_ = c.JSON(resp.StatusCode, api.ErrorResponseWrapper{Error: resp})
		}
	}
}

// ErrorMapper translates an arbitrary error into an ErrorResponse. It returns
// false to signal that it does not handle the given error, in which case the
// default mapping rules are applied.
type ErrorMapper func(err error) (api.ErrorResponse, bool)

// mapToErrorResponse applies the default mapping rules, turning well-known
// error types into an appropriate ErrorResponse. Anything it cannot classify
// is reported as a generic internal server error so that internal details are
// never leaked to the client.
func mapToErrorResponse(err error) (resp api.ErrorResponse) {
	resp = api.ErrorResponse{
		Code:        api.ErrorCodeInternal,
		Description: "internal server error",
		Attributes:  api.Attributes{},
		StatusCode:  http.StatusInternalServerError,
	}

	// our own structured Error already carries everything we need
	if err, ok := errors.AsType[api.Error](err); ok {
		resp = apiErrorToErrorResponse(err)
		return
	}

	// a cancelled context usually means the client went away
	if errors.Is(err, context.Canceled) {
		resp = apiErrorToErrorResponse(api.ErrCanceled)
		return
	}

	// a deadline exceeded maps to a timeout
	if errors.Is(err, context.DeadlineExceeded) {
		resp = apiErrorToErrorResponse(api.ErrTimeout)
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
	return apiErrorToErrorResponse(api.ErrInternalServerError)
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

// toErrorResponse converts a structured Error into an ErrorResponse,
// falling back to a 500 status code if the error carries an out-of-range HTTP
// status code.
func apiErrorToErrorResponse(e api.Error) api.ErrorResponse {
	statusCode := e.HttpStatusCode
	if statusCode < 400 || statusCode >= 600 {
		// invalid, use default
		statusCode = http.StatusInternalServerError
	}

	return api.ErrorResponse{
		Code:        e.Code,
		Description: e.Description,
		Attributes:  e.Attributes,
		StatusCode:  statusCode,
	}
}
