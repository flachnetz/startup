package startup_logrus

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	"reflect"
)

// default empty logger
var emptyEntry = logrus.NewEntry(logrus.StandardLogger())

type h struct{}

func NewTracingHook() logrus.Hook {
	return h{}
}

func (h) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h) Fire(entry *logrus.Entry) error {
	if entry.Context == nil {
		return nil
	}

	// get the span from the entries context
	spanContext := spanContextOf(entry.Context)
	if spanContext == nil {
		return nil
	}

	if entry.Data == nil {
		entry.Data = logrus.Fields{}
	}

	if traceId := traceIdOf(spanContext); traceId != "" {
		// we can not create a new entry here, as logrus does not support "chaining" of hooks,
		// so we will just modify the existing entry here before logging
		entry.Data["traceId"] = traceId
	}

	return nil
}

func spanContextOf(ctx context.Context) opentracing.SpanContext {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return nil
	}

	return span.Context()
}

func traceIdOf(spanContext interface{}) string {
	type hexer interface{ ToHex() string }

	rSpanContext := reflect.ValueOf(spanContext)
	if !rSpanContext.IsValid() || rSpanContext.Kind() != reflect.Struct {
		return ""
	}

	fieldTraceId := rSpanContext.FieldByName("TraceID")
	if !fieldTraceId.IsValid() {
		return ""
	}

	traceId, ok := fieldTraceId.Interface().(hexer)
	if !ok {
		return ""
	}

	return traceId.ToHex()
}
