package startup_tracing

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"
)

func Trace(ctx context.Context, op string, fn func(ctx context.Context, span trace.Span) error) (err error) {
	_, err = TraceWithResult(ctx, op, func(ctx context.Context, span trace.Span) (any, error) {
		return nil, fn(ctx, span)
	})

	return err
}

// TraceWithResult traces a child call while propagating the span using the context.
func TraceWithResult[T any](ctx context.Context, op string, fn func(ctx context.Context, span trace.Span) (T, error)) (result T, err error) {
	ctx, span := otel.Tracer("").Start(ctx, op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(TagsFromContext(ctx)...),
	)

	defer func() {
		if err != nil && isNotErrNoRows(err) {
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(attribute.Bool("error", true))
			span.SetAttributes(attribute.String("error_message", err.Error()))
		}

		span.End()
	}()

	result, err = fn(ctx, span)
	return
}

// CurrentSpanFromContext returns the current span, or nil if there is no active span.
func CurrentSpanFromContext(ctx context.Context) trace.Span {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return nil
	}
	return span
}

func isNotErrNoRows(err error) bool {
	return errors.Cause(err) != sql.ErrNoRows
}

var extraTagsKey = "zipkin tags"

// WithTags adds the given tags to the context. All spans that are created using
// functions from this package will automatically set those tags.
func WithTags(ctx context.Context, tags map[string]string) context.Context {
	if tags == nil {
		return context.WithValue(ctx, &extraTagsKey, nil)
	}

	tags = maps.Clone(tags)

	existingTags, _ := ctx.Value(&extraTagsKey).(map[string]string)
	for key, value := range existingTags {
		if _, ok := tags[key]; !ok {
			tags[key] = value
		}
	}

	return context.WithValue(ctx, &extraTagsKey, tags)
}

// WithServiceOverride adds service override tags to the context. See WithTags
func WithServiceOverride(ctx context.Context, service string) context.Context {
	return WithTags(ctx, map[string]string{
		"peer.service": service,
	})
}

// TagsFromContext extracts tags stored in the context as OTel attributes.
func TagsFromContext(ctx context.Context) []attribute.KeyValue {
	tags, _ := ctx.Value(&extraTagsKey).(map[string]string)
	if len(tags) == 0 {
		return nil
	}

	attrs := make([]attribute.KeyValue, 0, len(tags))
	for k, v := range tags {
		attrs = append(attrs, attribute.String(k, v))
	}
	return attrs
}
