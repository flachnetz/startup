package startup_tracing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestResourceOf(t *testing.T) {
	cases := []struct {
		name   string
		method string
		rawURL string
		want   string
	}{
		{
			name:   "host and query are dropped",
			method: "POST",
			rawURL: "http://api-gateway-internal.platform.svc.cluster.local/payments/v1/sessions?debug=1",
			want:   "POST /payments/v1/sessions",
		},
		{
			name:   "uuid is named after its collection",
			method: "GET",
			rawURL: "http://host/orders/v1/orders/3f1a2b4c-1111-4222-8333-444455556666",
			want:   "GET /orders/v1/orders/ORDER_ID",
		},
		{
			name:   "id in the middle keeps the trailing segments",
			method: "PUT",
			rawURL: "http://host/orders/3f1a2b4c-1111-4222-8333-444455556666/config",
			want:   "PUT /orders/ORDER_ID/config",
		},
		{
			name:   "numeric id is named after its collection",
			method: "DELETE",
			rawURL: "http://host/limits/v1/reservations/4711",
			want:   "DELETE /limits/v1/reservations/RESERVATION_ID",
		},
		{
			name:   "two ids in one path stay distinguishable",
			method: "GET",
			rawURL: "http://host/orders/4711/items/0815",
			want:   "GET /orders/ORDER_ID/items/ITEM_ID",
		},
		{
			name:   "opaque hex token is an id too",
			method: "GET",
			rawURL: "http://host/payments/v1/sessions/a1b2c3d4e5f60718",
			want:   "GET /payments/v1/sessions/SESSION_ID",
		},
		{
			name:   "ulid is an id",
			method: "GET",
			rawURL: "http://host/v1/orders/01ARZ3NDEKTSV4RRFFQ69G5FAV",
			want:   "GET /v1/orders/ORDER_ID",
		},
		{
			name:   "lowercase ulid is an id",
			method: "GET",
			rawURL: "http://host/v1/orders/01arz3ndektsv4rrffq69g5fav",
			want:   "GET /v1/orders/ORDER_ID",
		},
		{
			name:   "ksuid is an id",
			method: "GET",
			rawURL: "http://host/v1/payments/0ujsswThIGTUYm2K8FjOOfXtY1K",
			want:   "GET /v1/payments/PAYMENT_ID",
		},
		{
			name:   "uppercase uuid is an id",
			method: "GET",
			rawURL: "http://host/v1/orders/3F1A2B4C-1111-4222-8333-444455556666",
			want:   "GET /v1/orders/ORDER_ID",
		},
		{
			name:   "a plain word is not an id",
			method: "POST",
			rawURL: "http://host/v1/orders/checkout",
			want:   "POST /v1/orders/checkout",
		},
		{
			name:   "long dashed word stays literal",
			method: "GET",
			rawURL: "http://host/v1/orders/unsubscribe-confirmation-page",
			want:   "GET /v1/orders/unsubscribe-confirmation-page",
		},
		{
			name:   "known collection normalises a slug id",
			method: "GET",
			rawURL: "http://host/v1/tenants/bmg-portugal/config",
			want:   "GET /v1/tenants/TENANT_ID/config",
		},
		{
			name:   "version segment is not an id",
			method: "GET",
			rawURL: "http://host/v1/orders",
			want:   "GET /v1/orders",
		},
		{
			name:   "empty path becomes root",
			method: "GET",
			rawURL: "http://host",
			want:   "GET /",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u, err := url.Parse(c.rawURL)
			require.NoError(t, err)
			assert.Equal(t, c.want, resourceOf(c.method, u))
		})
	}
}

func TestTracingRoundTripper_SpanNamedAfterRequest(t *testing.T) {
	recorder := recordSpans(t)

	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer peer.Close()

	client := http.Client{Transport: NewPropagatingRoundTripper(http.DefaultTransport)}

	_, err := TraceWithResult(t.Context(), "payment.StartSession", func(ctx context.Context, _ trace.Span) (any, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, peer.URL+"/payments/v1/sessions", nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		return nil, resp.Body.Close()
	})
	require.NoError(t, err)

	var names []string
	for _, span := range recorder.Ended() {
		names = append(names, span.Name())
	}
	assert.Contains(t, names, "POST /payments/v1/sessions")
	assert.NotContains(t, names, "http-client")
}

func TestTracing_ServerSpanNamedAfterRequest(t *testing.T) {
	recorder := recordSpans(t)

	handler := Tracing("order-service", "serve")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/orders/3f1a2b4c-1111-4222-8333-444455556666", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, "serve GET /v1/orders/ORDER_ID", ended[0].Name(),
		"op stays the name prefix so existing queries keep matching")

	attrs := map[string]string{}
	for _, kv := range ended[0].Attributes() {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	assert.Equal(t, "serve", attrs["operation"], "op stays queryable as an attribute")
	assert.Equal(t, "GET /v1/orders/ORDER_ID", attrs["resource.name"])
}

// A propagated request must still get a server span of this service, as a child
// of the caller's span. Without it every client and db span of the request hangs
// off the caller and the request has no local root.
func TestTracing_ServerSpanIsCreatedForAPropagatedRequest(t *testing.T) {
	recorder := recordSpans(t)

	prevProp := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prevProp) })

	var childTraceID trace.TraceID
	var childParent trace.SpanID
	handler := Tracing("order-service", "serve")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span := trace.SpanFromContext(r.Context())
		childTraceID = span.SpanContext().TraceID()
		childParent = span.SpanContext().SpanID()
		w.WriteHeader(http.StatusOK)
	}))

	callerTraceID, err := trace.TraceIDFromHex("5a697f07e03d04999ad863247383871a")
	require.NoError(t, err)
	callerSpanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/orders/checkout", nil)
	req.Header.Set("traceparent", "00-"+callerTraceID.String()+"-"+callerSpanID.String()+"-01")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	ended := recorder.Ended()
	require.Len(t, ended, 1, "a propagated request must still produce a server span")
	assert.Equal(t, "serve POST /v1/orders/checkout", ended[0].Name())
	assert.Equal(t, callerTraceID, ended[0].SpanContext().TraceID(), "same trace as the caller")
	assert.Equal(t, callerSpanID, ended[0].Parent().SpanID(), "child of the caller's span")
	assert.Equal(t, callerTraceID, childTraceID, "handler sees the server span")
	assert.Equal(t, ended[0].SpanContext().SpanID(), childParent,
		"handler's context carries the new server span, so its children nest under it")

	attrs := map[string]string{}
	for _, kv := range ended[0].Attributes() {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	assert.Equal(t, "order-service", attrs["peer.service"])
	assert.Equal(t, "200", attrs["http.status_code"])
}

func TestLooksLikeID_TokenShapes(t *testing.T) {
	ids := []string{
		"4711",
		"3f1a2b4c-1111-4222-8333-444455556666",
		"01ARZ3NDEKTSV4RRFFQ69G5FAV",       // ulid
		"01arz3ndektsv4rrffq69g5fav",       // ulid, lowercase
		"0ujsswThIGTUYm2K8FjOOfXtY1K",      // ksuid
		"V1StGXR8Z5jdHi6BmyT" + "1234",     // nanoid-ish
		"a1b2c3d4e5f60718",                 // 16 hex
		"deadbeefdeadbeefdeadbeefdeadbeef", // 32 hex, no digits beyond a-f
	}
	for _, id := range ids {
		assert.True(t, looksLikeID(id), id)
	}

	notIDs := []string{
		"",
		"v1",
		"orders",
		"checkout",
		"config",
		"unsubscribe-confirmation-page", // long, but not alphanumeric
		"internalconfiguration",         // long alphanumeric, no digit
	}
	for _, s := range notIDs {
		assert.False(t, looksLikeID(s), s)
	}
}

func TestPlaceholderFor_PluralForms(t *testing.T) {
	cases := map[string]string{
		"orders":         "ORDER_ID",
		"entries":        "ENTRY_ID",
		"addresses":      "ADDRESS_ID",
		"participations": "PARTICIPATION_ID",
		"config":         "CONFIG_ID",
		"draw-entries":   "DRAW_ENTRY_ID",
		"":               "ID",
	}

	for collection, want := range cases {
		assert.Equal(t, want, placeholderFor(collection), collection)
	}
}
