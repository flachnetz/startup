package api

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4/middleware"

	"github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func CustomErrorHandler[E ApiError](errorHandler ErrorHandler[E]) func(error, echo.Context) {
	return func(err error, c echo.Context) {
		errorHandler.HandleError(c.Request().Context(), c, err)
	}
}

type ApiError interface {
	error
	ToErrorResponse() ErrorResponse
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
	return ErrUnknown.WithDescription(err.Error())
}

func (eh *ErrorHandler[E]) timeoutError(msg string) ApiError {
	if eh.TimeoutError != nil {
		return eh.TimeoutError(msg)
	}
	return ErrTimeout.WithDescription(msg)
}

func (eh *ErrorHandler[E]) unknownError(msg string) error {
	if eh.UnknownError != nil {
		return eh.UnknownError(msg)
	}
	return ErrUnknown
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
	apiError := eh.toApiError(err)
	httpStatusFrom := eh.httpStatusFrom(ctx, err)
	if httpStatusFrom == 499 {
		apiError = eh.timeoutError(apiError.Error())
	}

	LogHttpError(logger, c.Path(), httpStatusFrom, apiError)
	jErr := c.JSON(httpStatusFrom, apiError.ToErrorResponse())
	if jErr != nil {
		logger.Error(jErr)
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

func BasicAuthValidator(basicAuthUser string, basicAuthPassword string) middleware.BasicAuthValidator {
	return func(user string, password string, context echo.Context) (bool, error) {
		if user == basicAuthUser && password == basicAuthPassword {
			return true, nil
		}
		return false, nil
	}
}
