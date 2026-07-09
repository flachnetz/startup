package echox

import (
	"log/slog"

	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"go.opentelemetry.io/otel/trace"
)

// Logging returns the default logging middleware. If you dont want that,
// you can use the DefaultLoggerConfig and DefaultLogValuesFunc to build your
// own logging middleware
func Logging() echo.MiddlewareFunc {
	return middleware.RequestLoggerWithConfig(DefaultLoggerConfig)
}

var DefaultLoggerConfig = middleware.RequestLoggerConfig{
	LogMethod:     true,
	LogURIPath:    true,
	LogStatus:     true,
	LogLatency:    true,
	HandleError:   true,
	LogValuesFunc: DefaultLogValuesFunc,
}

func DefaultLogValuesFunc(c *echo.Context, v middleware.RequestLoggerValues) error {
	attrs := []slog.Attr{
		slog.String("method", v.Method),
		slog.String("path", v.URIPath),
		slog.Int("status", v.Status),
		slog.Duration("latency", v.Latency),
	}

	if v.Error != nil {
		attrs = append(attrs, sl.Error(v.Error))
	}

	ctx := c.Request().Context()

	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		attrs = append(attrs, slog.String("trace", spanContext.TraceID().String()))
	}

	switch {
	case v.Status >= 500:
		c.Logger().LogAttrs(ctx, slog.LevelError, "request", attrs...)
	case v.Status >= 400:
		c.Logger().LogAttrs(ctx, slog.LevelInfo, "request", attrs...)
	default:
		c.Logger().LogAttrs(ctx, slog.LevelDebug, "request", attrs...)
	}
	return nil
}
