package api

import (
	"context"

	"github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type ErrorHandler[E error] struct {
	NewUnknownError func(msg string) E
	NewTimeoutError func(msg string) E
	HttpStatusFrom  func(ctx context.Context, err error) int
	ToApiError      func(err error) error
}

func (eh *ErrorHandler[E]) HandleError(ctx context.Context, c echo.Context, err error) {
	logger := startup_logrus.GetLogger(ctx, "HandleError")

	var e E
	ok := errors.As(err, &e)
	if ok {
		httpStatusFrom := eh.HttpStatusFrom(ctx, e)
		LogHttpError(logger, c.Path(), httpStatusFrom, err)
		jErr := c.JSON(httpStatusFrom, eh.ToApiError(e))
		if jErr != nil {
			logger.Error(jErr)
		}
	} else {
		code := eh.HttpStatusFrom(ctx, e)
		e = eh.NewUnknownError(err.Error())

		switch code {
		case 499:
			e = eh.NewTimeoutError(err.Error())
		default:
			var he *echo.HTTPError
			if errors.As(err, &he) {
				code = he.Code
			}
		}
		LogHttpError(logger, c.Path(), code, err)

		jErr := c.JSON(code, eh.ToApiError(e))
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
