package echo

import (
	"context"
	"github.com/flachnetz/startup/v2/lib/api"
	"net/http"

	"github.com/labstack/echo/v4/middleware"

	"github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

//lint:ignore U1000
func CustomErrorHandler[E ApiError](errorHandler ErrorHandler[E]) func(error, echo.Context) {
	return func(err error, c echo.Context) {
		errorHandler.HandleError(c.Request().Context(), c, err)
	}
}

type ApiError interface {
	error
	ToErrorResponse() api.ErrorResponse
}

type ErrorHandler[E ApiError] struct {
	UnknownError   func(msg string) error
	TimeoutError   func(msg string) ApiError
	HttpStatusFrom func(ctx context.Context, err error) int
	ToApiError     func(err error) E
}

func (eh *ErrorHandler[E]) toApiError(err error) ApiError {
	if eh.ToApiError != nil {
		return eh.ToApiError(err)
	}
	return api.ErrUnknown.WithDescription(err.Error())
}

func (eh *ErrorHandler[E]) timeoutError(msg string) ApiError {
	if eh.TimeoutError != nil {
		return eh.TimeoutError(msg)
	}
	return api.ErrTimeout.WithDescription(msg)
}

//lint:ignore U1000
func (eh *ErrorHandler[E]) unknownError(msg string) error {
	if eh.UnknownError != nil {
		return eh.UnknownError(msg)
	}
	return api.ErrUnknown
}

func (eh *ErrorHandler[E]) httpStatusFrom(ctx context.Context, err error) int {
	if eh.HttpStatusFrom != nil {
		return eh.HttpStatusFrom(ctx, err)
	}
	httpStatusFrom := http.StatusInternalServerError
	var he *echo.HTTPError
	if errors.As(err, &he) {
		httpStatusFrom = he.Code
	}
	return httpStatusFrom
}

func (eh *ErrorHandler[E]) HandleError(ctx context.Context, c echo.Context, err error) {
	logger := startup_logrus.LoggerOf(ctx)
	var fieldErr *startup_logrus.FieldError
	if errors.As(err, &fieldErr) {
		logger = logger.WithFields(fieldErr.Fields)
	}
	apiError := eh.toApiError(err)
	httpStatusFrom := eh.httpStatusFrom(ctx, err)
	if httpStatusFrom == 499 {
		apiError = eh.timeoutError(apiError.Error())
	}

	LogHttpError(logger, c.Path(), httpStatusFrom, apiError)
	if c.Response().Committed {
		logger.Warn("response already committed")
		return
	}
	switch c.Request().Method {
	case http.MethodHead, http.MethodOptions:
		cErr := c.NoContent(httpStatusFrom)
		if cErr != nil {
			logger.Error(cErr)
		}
	default:
		jErr := c.JSON(httpStatusFrom, apiError.ToErrorResponse())
		if jErr != nil {
			logger.Error(jErr)
		}
	}
}

// LogHttpError logs errors with a different log level depending on the http status code.
func LogHttpError(logger *logrus.Entry, path string, code int, err error) {
	if code/100 == 5 {
		logger.Errorf("%s req=%s", err.Error(), path)
	} else {
		logger.Warnf("%s req=%s", err.Error(), path)
	}
}
