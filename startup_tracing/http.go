package startup_tracing

import (
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

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

// Tracing returns a middleware that adds tracing to an http handler.
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

// WithSpanPropagation returns a new http.Client that has automatic propagation
// of zipkin trace ids enabled, as well as automatic tracing of http client operations (dns, connect, tls-handshake)
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
	parentSpan := opentracing.SpanFromContext(req.Context())
	if parentSpan == nil {
		return rt.delegate.RoundTrip(req)
	}

	// create a new span for this operation
	span := opentracing.GlobalTracer().StartSpan("http-client",
		ext.SpanKindRPCClient,
		opentracing.ChildOf(parentSpan.Context()),
	)

	ext.HTTPMethod.Set(span, req.Method)
	ext.HTTPUrl.Set(span, req.URL.String())

	// create a copy of the original request and inject the http tracing
	httpTraceContext := httptrace.WithClientTrace(req.Context(), newClientTrace(span.Context()))
	reqCopy := req.Clone(httpTraceContext)

	// inject the spans information into the request so that the
	// other party can pick it up and continue the request.
	_ = span.Tracer().Inject(span.Context(),
		opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(reqCopy.Header))

	// do the actual request
	resp, err := rt.delegate.RoundTrip(reqCopy)
	if err != nil {
		// in case of non-http errors, jump out directly
		span.SetTag("error", true)
		span.SetTag("error_message", err.Error())
		span.Finish()

		return nil, err
	}

	// set status code on successful round trip
	ext.HTTPStatusCode.Set(span, uint16(resp.StatusCode))

	// instrument the response body so we can track
	// the actual "end" of the request and finish our span
	resp.Body = bodyGuard(reqCopy, span, resp.Body)

	return resp, nil
}

func bodyGuard(req *http.Request, span opentracing.Span, body io.ReadCloser) io.ReadCloser {
	ctx := req.Context()
	key := req.Method + " " + req.URL.String()

	guard := &readCloserWithTrace{span: span, ReadCloser: body}

	runtime.SetFinalizer(guard, func(guard *readCloserWithTrace) {
		if !guard.closed.Load() {
			log.WithField("prefix", "grave").
				WithContext(ctx).
				Warnf("http.Request body was not closed for %q", key)
		}
	})

	return guard
}

func newClientTrace(parentContext opentracing.SpanContext) *httptrace.ClientTrace {
	var ct httptrace.ClientTrace

	configureDnsHooks(&ct, parentContext)
	configureConnectHooks(&ct, parentContext)
	configureTlsHooks(&ct, parentContext)

	return &ct
}

func configureDnsHooks(ct *httptrace.ClientTrace, parentContext opentracing.SpanContext) {
	var mu sync.Mutex

	var dnsSpan opentracing.Span

	ct.DNSStart = func(info httptrace.DNSStartInfo) {
		defer locked(&mu)()

		if dnsSpan != nil {
			finishSpan(dnsSpan, errors.New("interrupted"))
		}

		dnsSpan = opentracing.GlobalTracer().StartSpan("http-client:dns",
			ext.SpanKindRPCClient,
			opentracing.ChildOf(parentContext),
			opentracing.Tag{Key: "host", Value: info.Host},
		)

	}

	ct.DNSDone = func(info httptrace.DNSDoneInfo) {
		defer locked(&mu)()

		if dnsSpan != nil {
			finishSpan(dnsSpan, info.Err)
			dnsSpan = nil
		}
	}
}

func configureConnectHooks(ct *httptrace.ClientTrace, parentContext opentracing.SpanContext) {
	var mu sync.Mutex
	connSpans := map[string]opentracing.Span{}

	ct.ConnectStart = func(network, addr string) {
		defer locked(&mu)()

		key := network + ":" + addr
		if span := connSpans[key]; span != nil {
			finishSpan(span, errors.New("interrupted"))
		}

		connSpans[key] = opentracing.GlobalTracer().StartSpan("http-client:connect",
			ext.SpanKindRPCClient,
			opentracing.ChildOf(parentContext),
			opentracing.Tag{Key: "network", Value: network},
			opentracing.Tag{Key: "addr", Value: addr},
		)
	}

	ct.ConnectDone = func(network, addr string, err error) {
		defer locked(&mu)()

		key := network + ":" + addr
		if span := connSpans[key]; span != nil {
			finishSpan(span, err)
			delete(connSpans, key)
		}
	}
}

func configureTlsHooks(ct *httptrace.ClientTrace, parentContext opentracing.SpanContext) {
	var mu sync.Mutex
	var tlsSpan opentracing.Span

	ct.TLSHandshakeStart = func() {
		defer locked(&mu)()

		if tlsSpan != nil {
			finishSpan(tlsSpan, errors.New("interrupted"))
			tlsSpan.Finish()
		}

		opentracing.GlobalTracer().StartSpan("http-client:tls-handshake",
			ext.SpanKindRPCClient,
			opentracing.ChildOf(parentContext),
		)
	}

	ct.TLSHandshakeDone = func(state tls.ConnectionState, err error) {
		defer locked(&mu)()

		if tlsSpan != nil {
			finishSpan(tlsSpan, err)
			tlsSpan = nil
		}
	}
}

func locked(m *sync.Mutex) func() {
	m.Lock()
	return m.Unlock
}

func finishSpan(span opentracing.Span, err error) {
	if err != nil {
		span.SetTag("error", true)
		span.SetTag("error_message", err.Error())
	}

	span.Finish()
}

type readCloserWithTrace struct {
	io.ReadCloser
	span   opentracing.Span
	closed atomic.Bool
}

func (r *readCloserWithTrace) Close() error {
	if r.closed.CompareAndSwap(false, true) {
		r.span.Finish()
		return r.ReadCloser.Close()
	}

	return nil
}
