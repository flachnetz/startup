package startup_tracing

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/flachnetz/startup/v2/lib"

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
				log := LoggerOf(ctx)
				log.Warnf("Could not extract tracer from http headers: %s", err)
			}

			// start a new server side trace
			serverSpan := opentracing.StartSpan(op, ext.RPCServerOption(wireContext))
			defer serverSpan.Finish()

			// use a clean url as resource
			serverSpan.SetTag("dd.service", service)
			ext.HTTPMethod.Set(serverSpan, req.Method)
			ext.HTTPUrl.Set(serverSpan, cleanUrl(req.URL))

			// record and log the status code of the response
			rl, w := responseLoggerOf(w)
			defer rl.addStatusToSpan(serverSpan)

			// put the span into the context
			ctx = opentracing.ContextWithSpan(ctx, serverSpan)
			handler.ServeHTTP(w, req.WithContext(ctx))
		})
	}
}

func cleanUrl(u *url.URL) string {
	urlCopy := lib.PtrOf(*u)
	urlCopy.RawQuery = ""
	urlCopy.User = nil

	urlStr := urlCopy.String()

	urlStr = reClean.ReplaceAllStringFunc(urlStr, func(s string) string {
		idx := strings.LastIndexByte(s, '/')
		return s[:idx] + strings.ToUpper(s[:idx])
	})

	// clean uuid and numbers
	urlStr = reUUID.ReplaceAllString(urlStr, "/UUID")
	urlStr = reNumber.ReplaceAllString(urlStr, "/N")

	return urlStr
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
		TagsFromContext(req.Context()),
	)

	ext.HTTPMethod.Set(span, req.Method)
	ext.HTTPUrl.Set(span, cleanUrl(req.URL))

	// create a copy of the original request and inject the http tracing
	httpTraceContext := httptrace.WithClientTrace(req.Context(), newClientTrace(span.Context(), TagsFromContext(req.Context())))
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
	key := req.Method + " " + req.URL.String()
	guard := &readCloserWithTrace{reader: body, span: &autoCloseSpan{Span: span, key: key}}
	runtime.SetFinalizer(guard.span, finalizeAutoCloseSpan)
	return guard
}

func newClientTrace(parentContext opentracing.SpanContext, startOptions opentracing.StartSpanOption) *httptrace.ClientTrace {
	var ct httptrace.ClientTrace

	configureDnsHooks(&ct, parentContext, startOptions)
	configureConnectHooks(&ct, parentContext, startOptions)
	configureTlsHooks(&ct, parentContext, startOptions)

	return &ct
}

func configureDnsHooks(ct *httptrace.ClientTrace, parentContext opentracing.SpanContext, startOptions opentracing.StartSpanOption) {
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
			startOptions,
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

func configureConnectHooks(ct *httptrace.ClientTrace, parentContext opentracing.SpanContext, startOptions opentracing.StartSpanOption) {
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
			startOptions,
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

func configureTlsHooks(ct *httptrace.ClientTrace, parentContext opentracing.SpanContext, startOptions opentracing.StartSpanOption) {
	var mu sync.Mutex
	var tlsSpan opentracing.Span

	ct.TLSHandshakeStart = func() {
		defer locked(&mu)()

		if tlsSpan != nil {
			finishSpan(tlsSpan, errors.New("interrupted"))
			tlsSpan.Finish()
		}

		tlsSpan = opentracing.GlobalTracer().StartSpan("http-client:tls-handshake",
			ext.SpanKindRPCClient,
			opentracing.ChildOf(parentContext),
			startOptions,
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
	reader io.ReadCloser
	span   *autoCloseSpan
}

func (r *readCloserWithTrace) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil {
		if err != io.EOF {
			r.span.SetTag("error", true)
			r.span.SetTag("error_message", fmt.Sprintf("error in read: %s", err))
		}

		r.span.Finish()
	}

	return n, err
}

func (r *readCloserWithTrace) Close() error {
	r.span.Finish()
	runtime.KeepAlive(r.span)

	return r.reader.Close()
}

type autoCloseSpan struct {
	opentracing.Span
	key    string
	closed atomic.Bool
}

func finalizeAutoCloseSpan(span *autoCloseSpan) {
	if span.closed.CompareAndSwap(false, true) {
		log.Warnf("unclosed http.Client span detected for %q", span.key)
		span.Span.SetTag("error", true)
		span.Span.SetTag("error_message", "reader was not closed")
		span.Span.Finish()
	}
}

func (s *autoCloseSpan) Finish() {
	if s.closed.CompareAndSwap(false, true) {
		s.Span.Finish()
	}
}

func (s *autoCloseSpan) FinishWithOptions(opts opentracing.FinishOptions) {
	if s.closed.CompareAndSwap(false, true) {
		s.closed.Store(true)
		s.Span.FinishWithOptions(opts)
	}
}

func (s *autoCloseSpan) SetTag(key string, value interface{}) opentracing.Span {
	if !s.closed.Load() {
		return s.Span.SetTag(key, value)
	}

	return s
}
