package startup_tracing

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/flachnetz/startup/v2/startup_http"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	reNumber = regexp.MustCompile(`/[0-9]+`)
	reClean  = regexp.MustCompile(`/(?:tenants|sites|games|customers|tickets)/[^/]+`)
	reUUID   = regexp.MustCompile(`/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`)
)

// Tracing returns a middleware that adds tracing to an http handler.
func Tracing(service string, op string) startup_http.HttpMiddleware {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			tracer := otel.Tracer(service)

			// Extract propagated context from incoming request headers
			ctx := otel.GetTextMapPropagator().Extract(req.Context(), propagation.HeaderCarrier(req.Header))

			// Check if we already have a valid span (chained middleware)
			if existingSpan := trace.SpanFromContext(ctx); existingSpan.SpanContext().IsValid() {
				existingSpan.SetAttributes(
					attribute.String("peer.service", service),
				)
				// We can't rename an OTel span after creation easily,
				// but we update attributes. Continue with existing span.
				handler.ServeHTTP(w, req.WithContext(ctx))
				return
			}

			ctx, serverSpan := tracer.Start(ctx, op,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					attribute.String("peer.service", service),
					semconv.HTTPRequestMethodKey.String(req.Method),
					attribute.String("http.url", cleanUrl(req.URL)),
				),
			)
			defer serverSpan.End()

			// record the status code of the response
			rl := statusWriter(w)

			defer func() {
				serverSpan.SetAttributes(attribute.Int("http.status_code", rl.status))
				if rl.status >= 500 {
					serverSpan.SetStatus(codes.Error, http.StatusText(rl.status))
				}
			}()

			handler.ServeHTTP(rl, req.WithContext(ctx))
		})
	}
}

func cleanUrl(u *url.URL) string {
	urlCopy := new(*u)
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
// of trace context enabled.
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
	ctx := req.Context()
	parentSpan := trace.SpanFromContext(ctx)
	if !parentSpan.SpanContext().IsValid() {
		return rt.delegate.RoundTrip(req)
	}

	tracer := otel.Tracer("")
	ctx, span := tracer.Start(ctx, "http-client",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.HTTPRequestMethodKey.String(req.Method),
			attribute.String("http.url", cleanUrl(req.URL)),
		),
		trace.WithAttributes(TagsFromContext(ctx)...),
	)

	// create a copy of the original request and inject trace context
	httpTraceContext := httptrace.WithClientTrace(ctx, newClientTrace(ctx))
	reqCopy := req.Clone(httpTraceContext)

	// inject the trace context into the request headers
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(reqCopy.Header))

	// do the actual request
	resp, err := rt.delegate.RoundTrip(reqCopy)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.Bool("error", true), attribute.String("error_message", err.Error()))
		span.End()
		return nil, err
	}

	span.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))

	// instrument the response body
	resp.Body = bodyGuard(reqCopy, span, resp.Body)

	return resp, nil
}

func bodyGuard(req *http.Request, span trace.Span, body io.ReadCloser) io.ReadCloser {
	key := req.Method + " " + req.URL.String()
	guard := &readCloserWithTrace{reader: body, span: &autoCloseSpan{Span: span, key: key}}
	runtime.SetFinalizer(guard.span, finalizeAutoCloseSpan)
	return guard
}

func newClientTrace(ctx context.Context) *httptrace.ClientTrace {
	var ct httptrace.ClientTrace

	configureDnsHooks(&ct, ctx)
	configureConnectHooks(&ct, ctx)
	configureTlsHooks(&ct, ctx)

	return &ct
}

func configureDnsHooks(ct *httptrace.ClientTrace, parentCtx context.Context) {
	var mu sync.Mutex
	var dnsSpan trace.Span

	ct.DNSStart = func(info httptrace.DNSStartInfo) {
		defer locked(&mu)()

		if dnsSpan != nil {
			finishSpan(dnsSpan, errors.New("interrupted"))
		}

		_, dnsSpan = otel.Tracer("").Start(parentCtx, "http-client:dns",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attribute.String("host", info.Host)),
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

func configureConnectHooks(ct *httptrace.ClientTrace, parentCtx context.Context) {
	var mu sync.Mutex
	connSpans := map[string]trace.Span{}

	ct.ConnectStart = func(network, addr string) {
		defer locked(&mu)()

		key := network + ":" + addr
		if span := connSpans[key]; span != nil {
			finishSpan(span, errors.New("interrupted"))
		}

		_, connSpans[key] = otel.Tracer("").Start(parentCtx, "http-client:connect",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				attribute.String("network", network),
				attribute.String("addr", addr),
			),
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

func configureTlsHooks(ct *httptrace.ClientTrace, parentCtx context.Context) {
	var mu sync.Mutex
	var tlsSpan trace.Span

	ct.TLSHandshakeStart = func() {
		defer locked(&mu)()

		if tlsSpan != nil {
			finishSpan(tlsSpan, errors.New("interrupted"))
		}

		_, tlsSpan = otel.Tracer("").Start(parentCtx, "http-client:tls-handshake",
			trace.WithSpanKind(trace.SpanKindClient),
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

func finishSpan(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.Bool("error", true), attribute.String("error_message", err.Error()))
	}
	span.End()
}

type readCloserWithTrace struct {
	reader io.ReadCloser
	span   *autoCloseSpan
}

func (r *readCloserWithTrace) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil {
		if err != io.EOF {
			r.span.SetStatus(codes.Error, fmt.Sprintf("error in read: %s", err))
			r.span.SetAttributes(attribute.Bool("error", true), attribute.String("error_message", fmt.Sprintf("error in read: %s", err)))
		}
		r.span.End()
	}
	return n, err
}

func (r *readCloserWithTrace) Close() error {
	r.span.End()
	runtime.KeepAlive(r.span)
	return r.reader.Close()
}

type autoCloseSpan struct {
	trace.Span
	key    string
	closed atomic.Bool
}

func finalizeAutoCloseSpan(span *autoCloseSpan) {
	if span.closed.CompareAndSwap(false, true) {
		slog.Warn("unclosed http.Client span detected", slog.String("key", span.key))
		span.SetStatus(codes.Error, "reader was not closed")
		span.SetAttributes(attribute.Bool("error", true), attribute.String("error_message", "reader was not closed"))
		span.Span.End()
	}
}

func (s *autoCloseSpan) End(options ...trace.SpanEndOption) {
	if s.closed.CompareAndSwap(false, true) {
		s.Span.End(options...)
	}
}

func (s *autoCloseSpan) SetAttributes(kv ...attribute.KeyValue) {
	if !s.closed.Load() {
		s.Span.SetAttributes(kv...)
	}
}

func (s *autoCloseSpan) SetStatus(code codes.Code, description string) {
	if !s.closed.Load() {
		s.Span.SetStatus(code, description)
	}
}
