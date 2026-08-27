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

	gls "github.com/flachnetz/startup/v2/lib/tls"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	reNumber = regexp.MustCompile(`^[0-9]+$`)
	reUUID   = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	reHex    = regexp.MustCompile(`(?i)^[0-9a-f]{16,}$`)

	// ULID: 26 chars of Crockford base32 (no I, L, O, U).
	reULID = regexp.MustCompile(`(?i)^[0-7][0-9ABCDEFGHJKMNPQRSTVWXYZ]{25}$`)

	// idCollections are path segments whose next segment is an id even when it
	// does not look like one (slugs, external references).
	idCollections = map[string]bool{
		"tenants": true, "sites": true, "games": true, "customers": true, "tickets": true,
	}
)

type Middleware func(http.Handler) http.Handler

// Tracing returns a middleware that adds tracing to an http handler.
func Tracing(service string, op string) Middleware {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			tracer := otel.Tracer(service)

			// Extract propagated context from incoming request headers
			ctx := otel.GetTextMapPropagator().Extract(req.Context(), propagation.HeaderCarrier(req.Header))

			// A server span is always started, also when the caller propagated a
			// trace: after Extract the context holds a non-recording remote span,
			// so a "do we already have a span" check would be true for every
			// propagated request and this service would contribute no span of its
			// own - its client and db spans would then hang off the caller's span.
			// The extracted context is the parent.

			// op alone ("serve") cannot be grouped by endpoint, so the request is
			// appended: "serve GET /v1/orders/UUID". The op prefix stays first so
			// queries that matched on it keep working with a prefix match.
			resource := resourceOf(req.Method, req.URL)

			ctx, serverSpan := tracer.Start(
				ctx, op+" "+resource,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					attribute.String("peer.service", service),
					attribute.String("operation", op),
					attribute.String("resource.name", resource),
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

			traceId := serverSpan.SpanContext().TraceID()

			// you should typically propagate your trace via context, but for logging
			// it might happen that someone calls slog.Warn instead of slog.WarnContext. In this case,
			// we use goroutine local storage as a fallback to store the threadId.
			gls.Put(sl.ThreadLocalTraceID(traceId))
			defer gls.Clear[sl.ThreadLocalTraceID]()

			handler.ServeHTTP(rl, req.WithContext(ctx))
		})
	}
}

func cleanUrl(u *url.URL) string {
	urlCopy := new(*u)
	urlCopy.RawQuery = ""
	urlCopy.User = nil
	urlCopy.Path = cleanPath(u.Path)

	return urlCopy.String()
}

// cleanPath replaces id segments with a placeholder named after the collection
// they belong to: /orders/6ba7…/config becomes /orders/ORDER_ID/config. This
// keeps one endpoint one span name (so latency can be averaged over it) while
// staying readable, which a bare /UUID does not.
func cleanPath(path string) string {
	if path == "" || path == "/" {
		return path
	}

	segments := strings.Split(path, "/")
	for i, segment := range segments {
		previous := ""
		if i > 0 {
			previous = segments[i-1]
		}

		if !looksLikeID(segment) && !idCollections[previous] {
			continue
		}

		segments[i] = placeholderFor(previous)
	}

	return strings.Join(segments, "/")
}

func looksLikeID(segment string) bool {
	return reNumber.MatchString(segment) ||
		reUUID.MatchString(segment) ||
		reULID.MatchString(segment) ||
		reHex.MatchString(segment) ||
		isOpaqueToken(segment)
}

// isOpaqueToken catches the remaining opaque shapes (KSUID, nanoid, base62
// external references): long, alphanumeric, carrying at least one digit. Real
// path words are shorter, so a false positive costs a wrong label, not
// cardinality.
func isOpaqueToken(segment string) bool {
	if len(segment) < 20 {
		return false
	}

	hasDigit := false
	for _, r := range segment {
		switch {
		case r >= '0' && r <= '9':
			hasDigit = true
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		default:
			return false
		}
	}

	return hasDigit
}

// placeholderFor names the placeholder after the preceding collection segment,
// e.g. "orders" -> ORDER_ID. Plural handling is deliberately crude (English "s"
// and "ies"); a wrong singular is a cosmetic issue, not a cardinality one.
func placeholderFor(collection string) string {
	if collection == "" || looksLikeID(collection) {
		return "ID"
	}

	singular := collection
	switch {
	case strings.HasSuffix(singular, "ies"):
		singular = strings.TrimSuffix(singular, "ies") + "y"
	case strings.HasSuffix(singular, "sses"), strings.HasSuffix(singular, "xes"):
		singular = strings.TrimSuffix(singular, "es")
	case strings.HasSuffix(singular, "s"):
		singular = strings.TrimSuffix(singular, "s")
	}

	return strings.ToUpper(strings.ReplaceAll(singular, "-", "_")) + "_ID"
}

// resourceOf names a span after the request it describes, e.g.
// "PUT /orders/ORDER_ID/config". Host and query are left out: they carry no
// information a trace view can group by, while the path prefix already
// identifies the peer (also behind an api gateway).
func resourceOf(method string, u *url.URL) string {
	path := cleanPath(u.Path)
	if path == "" {
		path = "/"
	}

	return method + " " + path
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

	resource := resourceOf(req.Method, req.URL)

	tracer := otel.Tracer("")
	ctx, span := tracer.Start(
		ctx, resource,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.HTTPRequestMethodKey.String(req.Method),
			attribute.String("http.url", cleanUrl(req.URL)),
			attribute.String("resource.name", resource),
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

		_, dnsSpan = otel.Tracer("").Start(
			parentCtx, "http-client:dns",
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

		_, connSpans[key] = otel.Tracer("").Start(
			parentCtx, "http-client:connect",
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

		_, tlsSpan = otel.Tracer("").Start(
			parentCtx, "http-client:tls-handshake",
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
