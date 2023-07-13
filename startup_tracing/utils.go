package startup_tracing

import (
	"context"
	"database/sql"

	"github.com/flachnetz/startup/v2/startup_logrus"
	"golang.org/x/exp/maps"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/pkg/errors"
)

func Trace(ctx context.Context, op string, fn func(ctx context.Context, span opentracing.Span) error) (err error) {
	_, err = TraceWithResult(ctx, op, func(ctx context.Context, span opentracing.Span) (any, error) {
		return nil, fn(ctx, span)
	})

	return err
}

// TraceWithResult traces a child call while propagating the span using the context. No need
// to call opentracing.ContextWithSpan yourself.
func TraceWithResult[T any](ctx context.Context, op string, fn func(ctx context.Context, span opentracing.Span) (T, error)) (result T, err error) {
	var parentContext opentracing.SpanContext

	parentSpan := CurrentSpanFromContext(ctx)
	if parentSpan != nil {
		parentContext = parentSpan.Context()
	}

	span := opentracing.GlobalTracer().StartSpan(op,
		ext.SpanKindRPCClient,
		opentracing.ChildOf(parentContext),
		TagsFromContext(ctx),
	)

	defer func() {
		if err != nil && isNotErrNoRows(err) {
			span.SetTag("error", true)
			span.SetTag("error_message", err.Error())
		}

		span.Finish()
	}()

	result, err = fn(opentracing.ContextWithSpan(ctx, span), span)
	return
}

// CurrentSpanFromContext returns the current span, or nil.
func CurrentSpanFromContext(ctx context.Context) opentracing.Span {
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		return span
	}

	return nil
}

func isNotErrNoRows(err error) bool {
	return errors.Cause(err) != sql.ErrNoRows
}

var extraTagsKey = "zipkin tags"

// WithTags adds the given Tags to the context. All spans that are created using
// functions from this package will automatically set those tags.
func WithTags(ctx context.Context, tags opentracing.Tags) context.Context {
	if tags == nil {
		// hide the existing tags
		return context.WithValue(ctx, &extraTagsKey, nil)
	}

	// create a defensive copy so no one changes the tags later on
	tags = maps.Clone(tags)

	// fill with missing tags from the existing ones
	existingTags, _ := ctx.Value(&extraTagsKey).(opentracing.Tags)
	for key, value := range existingTags {
		if _, ok := tags[key]; !ok {
			tags[key] = value
		}
	}

	return context.WithValue(ctx, &extraTagsKey, tags)
}

// WithServiceOverride adds a `dd.service` tag to the context. See WithTags
func WithServiceOverride(ctx context.Context, service string) context.Context {
	return WithTags(ctx, opentracing.Tags{"dd.service": service})
}

type tagsFromContext struct {
	ctx context.Context
}

func TagsFromContext(ctx context.Context) opentracing.StartSpanOption {
	return tagsFromContext{ctx}
}

func (s tagsFromContext) Apply(options *opentracing.StartSpanOptions) {
	tags, _ := s.ctx.Value(&extraTagsKey).(opentracing.Tags)
	maps.Copy(options.Tags, tags)

	logger := startup_logrus.GetLogger(s.ctx, nil)
	for key, value := range logger.Data {
		if key == "prefix" {
			continue
		}

		options.Tags["logger."+key] = value
	}
}
