package startup_tracing

import (
	"context"
	"net/http"
	"regexp"
	"strings"

	"github.com/flachnetz/startup/v2/startup_http"
	. "github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

var (
	reNumber = regexp.MustCompile(`/[0-9]+`)
	reClean  = regexp.MustCompile(`/(?:tenants|sites|games|customers|tickets)/[^/]+`)
	reUUID   = regexp.MustCompile(`/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`)
)

// Returns a middleware that adds tracing to an http handler.
// This will create a new and empty local storage for the current go routine
// to propagate the tracing context.
//
// You can use the tracing middleware multiple time. Using it a second time
// will not start a new trace but will update 'service' and 'operation'.
func Tracing(service string, op string) startup_http.HttpMiddleware {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()

			// Check if we already have a span. This happens when the user is
			// chaining multiple tracing middleware
			if existingSpan := opentracing.SpanFromContext(ctx); existingSpan != nil {
				// update existing span
				existingSpan.SetOperationName(op)
				existingSpan.SetTag("dd.service", service)

				// continue
				handler.ServeHTTP(w, req)
				return
			}

			// extract a span from the incoming request
			wireContext, err := opentracing.GlobalTracer().Extract(
				opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(req.Header))

			if err != nil && err != opentracing.ErrSpanContextNotFound {
				// ignore errors but show a small warning.
				log := GetLogger(ctx, "httpd")
				log.Warnf("Could not extract tracer from http headers: %s", err)
			}

			// start a new server side trace
			serverSpan := opentracing.StartSpan(op, ext.RPCServerOption(wireContext))
			defer serverSpan.Finish()

			// use a clean url as resource
			serverSpan.SetTag("dd.service", service)
			ext.HTTPMethod.Set(serverSpan, req.Method)
			ext.HTTPUrl.Set(serverSpan, cleanUrl(req.URL.String()))

			// record and log the status code of the response
			rl, w := responseLoggerOf(w)
			defer rl.addStatusToSpan(serverSpan)

			// put the span into the context
			ctx = opentracing.ContextWithSpan(ctx, serverSpan)
			handler.ServeHTTP(w, req.WithContext(ctx))
		})
	}
}

func cleanUrl(url string) string {
	url = reClean.ReplaceAllStringFunc(url, func(s string) string {
		idx := strings.LastIndexByte(s, '/')
		return s[:idx] + strings.ToUpper(s[:idx])
	})

	// clean uuid and numbers
	url = reUUID.ReplaceAllString(url, "/UUID")
	url = reNumber.ReplaceAllString(url, "/N")

	return url
}

func Execute(op string, r *http.Request, client *http.Client) (*http.Response, error) {
	if client == nil {
		client = http.DefaultClient
	}

	response, err := Trace(r.Context(), op, func(ctx context.Context, span opentracing.Span) (*http.Response, error) {
		// inject the spans information into the request so that the
		// other party can pick it up and continue the request.
		_ = span.Tracer().Inject(
			span.Context(),
			opentracing.HTTPHeaders,
			opentracing.HTTPHeadersCarrier(r.Header),
		)

		ext.HTTPMethod.Set(span, r.Method)
		ext.HTTPUrl.Set(span, r.URL.String())

		span.SetTag("dd.service", op)

		response, err := client.Do(r)
		if err == nil {
			ext.HTTPStatusCode.Set(span, uint16(response.StatusCode))
		}

		return response, err
	})

	return response, err
}

// Returns a new http.Client that has automatic propagation
// of zipkin trace ids enabled.
func WithSpanPropagation(client *http.Client) *http.Client {
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	clientCopy := *client
	clientCopy.Transport = NewPropagatingRoundTripper(transport)
	return &clientCopy
}

func NewPropagatingRoundTripper(rt http.RoundTripper) http.RoundTripper {
	return tracingRoundTripper{delegate: rt}
}

type tracingRoundTripper struct {
	delegate http.RoundTripper
}

func (rt tracingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	span := opentracing.SpanFromContext(req.Context())
	if span == nil {
		return rt.delegate.RoundTrip(req)
	}

	// create a copy of the original headers object
	headers := make(http.Header, len(req.Header))
	for key, value := range req.Header {
		headers[key] = value
	}

	// inject zipkin context headers, ignore errors
	_ = opentracing.GlobalTracer().Inject(span.Context(),
		opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(headers))

	// create a copy of the original request and update the headers
	reqCopy := *req
	reqCopy.Header = headers

	return rt.delegate.RoundTrip(&reqCopy)
}
