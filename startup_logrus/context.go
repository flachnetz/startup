package startup_logrus

import (
	"context"
	"github.com/sirupsen/logrus"
	"reflect"
)

type loggerKey struct{}

// stores the entry into the context so it can be retrieved with GetLogger later.
func WithLogger(ctx context.Context, logger *logrus.Entry) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// returns the current logger with the given prefix from the context.
func GetLogger(ctx context.Context, prefix string) *logrus.Entry {
	if ctx == nil {
		return emptyEntry.WithField("prefix", prefix)
	}

	logger := ctx.Value(loggerKey{})
	if logger == nil {
		return emptyEntry.WithField("prefix", prefix)
	}

	return logger.(*logrus.Entry).WithField("prefix", prefix)
}

func GetLoggerForObject(ctx context.Context, object interface{}) *logrus.Entry {
	t := reflect.ValueOf(object).Type()

	prefix := t.Name()
	if prefix == "" {
		prefix = t.PkgPath()
	}

	return GetLogger(ctx, prefix)
}
