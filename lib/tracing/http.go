package tracing

import (
	"github.com/modern-go/gls"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/sirupsen/logrus"
	"net/http"
	"regexp"
	"strings"
)

type Middleware func(http.Handler) http.Handler

// Returns a middleware that adds tracing to an http handler.
// This will create a new and empty local storage for the current go routine
// to propagate the tracing context.
func Tracing(service string, op string) Middleware {
	log := logrus.WithField("prefix", "tracing")

	reClean := regexp.MustCompile(`/(?:tenants|sites|games|customers)/[^/]+`)

	return func(handle http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			wireContext, err := opentracing.GlobalTracer().Extract(
				opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(req.Header))

			if err != nil && err != opentracing.ErrSpanContextNotFound {
				// ignore errors
				log.Warnf("Could not extract tracer from http headers: %s", err)
			}

			// start a new server side trace
			serverSpan := opentracing.StartSpan(op, ext.RPCServerOption(wireContext))
			defer serverSpan.Finish()

			// use a clean url as resource
			url := reClean.ReplaceAllStringFunc(req.URL.String(), func(s string) string {
				idx := strings.LastIndexByte(s, '/')
				return s[:idx] + strings.ToUpper(s[:idx])
			})

			serverSpan.SetTag("dd.service", service)
			ext.HTTPUrl.Set(serverSpan, url)
			ext.HTTPMethod.Set(serverSpan, req.Method)

			// record and log the status code of the response
			rl := responseLoggerOf(w)
			defer func() {
				ext.HTTPStatusCode.Set(serverSpan, uint16(rl.status))
			}()

			gls.WithEmptyGls(func() {
				ctx := opentracing.ContextWithSpan(req.Context(), serverSpan)

				WithSpan(serverSpan, func() {
					handle.ServeHTTP(rl, req.WithContext(ctx))
				})
			})()
		})
	}
}

func responseLoggerOf(w http.ResponseWriter) *responseLogger {
	if rl, ok := w.(*responseLogger); ok {
		return rl
	} else {
		return &responseLogger{ResponseWriter: w}
	}
}

// responseLogger is wrapper of http.ResponseWriter that keeps track
// of its HTTP status code
type responseLogger struct {
	http.ResponseWriter
	status int
}

func (l *responseLogger) WriteHeader(s int) {
	l.ResponseWriter.WriteHeader(s)
	l.status = s
}

func (l *responseLogger) Flush() {
	f, ok := l.ResponseWriter.(http.Flusher)
	if ok {
		f.Flush()
	}
}

func Execute(op string, r *http.Request, client *http.Client) (*http.Response, error) {
	if client == nil {
		client = http.DefaultClient
	}

	var err error
	var response *http.Response

	err = TraceChild(op, func(span opentracing.Span) error {
		// inject the spans information into the request so that the
		// other party can pick it up and continue the request.
		span.Tracer().Inject(
			span.Context(),
			opentracing.HTTPHeaders,
			opentracing.HTTPHeadersCarrier(r.Header))

		ext.HTTPMethod.Set(span, r.Method)
		ext.HTTPUrl.Set(span, r.URL.String())

		span.SetTag("dd.service", op)

		response, err = client.Do(r)
		if err == nil {
			ext.HTTPStatusCode.Set(span, uint16(response.StatusCode))
		}

		return err
	})

	return response, err
}
