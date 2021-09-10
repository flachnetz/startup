package startup_tracing_pg

import (
	"context"
	"database/sql"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/pkg/errors"
	"regexp"
	"strings"

	"github.com/flachnetz/startup/v2/startup_tracing"
)

type dbHook struct {
	ServiceName string
}

var reSpace = regexp.MustCompile(`\s+`)

func (h *dbHook) Before(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	// lookup if we have a parent span
	parent := startup_tracing.CurrentSpanFromContext(ctx)

	// if we dont have a parent span, we don't do tracing
	if parent == nil {
		return ctx, nil
	}

	span := opentracing.StartSpan(h.ServiceName,
		opentracing.ChildOf(parent.Context()),
		ext.SpanKindRPCClient)

	queryClean := strings.TrimSpace(reSpace.ReplaceAllString(query, " "))
	queryType := queryTypeOf(queryClean)

	// set extra tags for our datadog proxy
	span.SetTag("dd.service", h.ServiceName)
	span.SetTag("dd.resource", queryType)
	span.SetTag("sql.query", queryClean)

	return opentracing.ContextWithSpan(ctx, span), nil
}

func queryTypeOf(query string) string {
	if len(query) > 8 {
		prefix := query[:7]

		if strings.EqualFold("select ", prefix) {
			return "SELECT"
		}

		if strings.EqualFold("insert ", prefix) {
			return "INSERT"
		}

		if strings.EqualFold("delete ", prefix) {
			return "DELETE"
		}

		if strings.EqualFold("update ", prefix) {
			return "UPDATE"
		}
	}

	return "SQL"
}

func (h *dbHook) After(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		span.Finish()
	}

	return ctx, nil
}

func (h *dbHook) OnError(ctx context.Context, err error, query string, args ...interface{}) error {
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		if errors.Cause(err) != sql.ErrNoRows {
			span.SetTag("error", true)
		}
		span.SetTag("err", err.Error())
		span.Finish()
	}

	return err
}
