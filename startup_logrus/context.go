package startup_logrus

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"reflect"
	"regexp"
	"runtime"
)

type loggerKey struct{}

// stores the entry into the context so it can be retrieved with GetLogger later.
func WithLogger(ctx context.Context, logger *logrus.Entry) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// returns the current logger with the given prefix or type name from the context.
func GetLogger(ctx context.Context, object interface{}) *logrus.Entry {
	log := loggerOf(ctx).WithContext(ctx)

	prefix := prefixOf(object)
	if prefix == "" {
		return log
	}

	return log.WithField("prefix", prefix)
}

func loggerOf(ctx context.Context) *logrus.Entry {
	if ctx == nil {
		return emptyEntry
	}

	logger := ctx.Value(loggerKey{})
	if logger == nil {
		return emptyEntry
	}

	return logger.(*logrus.Entry)
}

var reShortName = regexp.MustCompile(`([^/.]+)(?:[.]func[.0-9])?$`)

func prefixOf(object interface{}) string {
	switch object := object.(type) {
	case string:
		return object

	case fmt.Stringer:
		return object.String()

	case nil:
		return ""

	default:
		rev := reflect.ValueOf(object)

		if rev.Kind() == reflect.Func {
			if fn := runtime.FuncForPC(rev.Pointer()); fn != nil {
				if match := reShortName.FindStringSubmatch(fn.Name()); match != nil {
					return match[1]
				}
			}
		}

		t := rev.Type()
		for t.Kind() == reflect.Ptr || t.Kind() == reflect.Interface {
			t = t.Elem()
		}

		prefix := t.Name()
		if prefix == "" {
			prefix = t.PkgPath()
		}

		return prefix
	}
}
