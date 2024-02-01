package api

import (
	"context"
	"net/http"

	"github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func CustomErrorHandler[E error](errorHandler ErrorHandler[E]) func(error, echo.Context) {
	return func(err error, c echo.Context) {
		errorHandler.HandleError(c.Request().Context(), c, err)
	}
}

type ErrorHandler[E error] struct {
	UnknownError   func(msg string) error
	TimeoutError   func(msg string) error
	HttpStatusFrom func(ctx context.Context, err error) int
	ToApiError     func(err error) E
}

func (eh *ErrorHandler[E]) toApiError(err error) error {
	if eh.ToApiError != nil {
		return eh.ToApiError(err)
	}
	return ErrUnknown.WithDescription(err.Error())
}

func (eh *ErrorHandler[E]) timeoutError(msg string) error {
	if eh.TimeoutError != nil {
		return eh.TimeoutError(msg)
	}
	return ErrTimeout.WithDescription(msg)
}

func (eh *ErrorHandler[E]) unknownError(msg string) error {
	if eh.UnknownError != nil {
		return errors.New(msg)
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
	logger := startup_logrus.GetLogger(ctx, "HandleError")
	apiError := eh.toApiError(err)
	httpStatusFrom := eh.httpStatusFrom(ctx, apiError)
	if httpStatusFrom == 499 {
		apiError = eh.timeoutError(apiError.Error())
	}

	LogHttpError(logger, c.Path(), httpStatusFrom, apiError)
	jErr := c.JSON(httpStatusFrom, err)
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
