package startup_tracing

import (
	"github.com/opentracing/opentracing-go"
	zipkintracer "github.com/openzipkin-contrib/zipkin-go-opentracing"
	"github.com/sirupsen/logrus"
)

func init() {
	// add traceId to hooks if available
	logrus.AddHook(logrusHook{})
}

type logrusHook struct{}

func (t logrusHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (t logrusHook) Fire(entry *logrus.Entry) error {
	ctx := entry.Context
	if ctx == nil {
		return nil
	}

	span := CurrentSpanFromContext(ctx)
	if span == nil {
		return nil
	}

	spanContext := span.Context()

	var traceId string

	// in most cases we will probably get a zipkin span, so we can try to get the traceId directly.
	if zipkinSpanContext, ok := spanContext.(zipkintracer.SpanContext); ok {
		traceId = zipkinSpanContext.TraceID.String()

	} else {
		// fallback to injecting the span id into a map and then reading it
		// out of the map again.
		spanCarrier := opentracing.TextMapCarrier{}

		err := span.Tracer().Inject(spanContext, opentracing.TextMap, spanCarrier)
		if err != nil {
			// shit, well doesnt matter.
			return nil
		}

		traceId = spanCarrier["x-b3-traceid"]
	}

	if traceId != "" {
		if entry.Data == nil {
			entry.Data = logrus.Fields{}
		}

		entry.Data["traceId"] = traceId
	}

	return nil
}
