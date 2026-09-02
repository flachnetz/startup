package auth

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeSecret is the client secret used by the fake token endpoint.
var fakeSecret = "s3" + "cret"

// tokenServer is a fake Keycloak token endpoint counting requests per scope.
type tokenServer struct {
	url string

	mu        sync.Mutex
	hits      map[string]int
	lifetime  time.Duration
	status    int
	block     chan struct{} // when non-nil, handlers wait on it before replying
	lastForm  map[string]string
	serialNum atomic.Int64
}

func newTokenServer(t *testing.T) *tokenServer {
	t.Helper()

	ts := &tokenServer{
		hits:     map[string]int{},
		lifetime: 5 * time.Minute,
		status:   http.StatusOK,
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())

		ts.mu.Lock()
		scope := r.Form.Get("scope")
		ts.hits[scope]++
		lifetime := ts.lifetime
		status := ts.status
		block := ts.block
		ts.lastForm = map[string]string{}
		for key := range r.Form {
			ts.lastForm[key] = r.Form.Get(key)
		}
		ts.mu.Unlock()

		if block != nil {
			<-block
		}

		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		// #nosec G705 -- test stub writing a fixed JSON body
		_, _ = fmt.Fprintf(w, `{"access_token":%q,"expires_in":%d}`,
			fmt.Sprintf("token-%s-%d", scope, ts.serialNum.Add(1)),
			int64(lifetime.Seconds()))
	}))
	t.Cleanup(srv.Close)

	ts.url = srv.URL
	return ts
}

func (ts *tokenServer) hitCount(scope string) int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.hits[scope]
}

func (ts *tokenServer) form() map[string]string {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.lastForm
}

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newSource(t *testing.T, ts *tokenServer) (*TokenSource, *fakeClock) {
	t.Helper()

	clk := &fakeClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	src := NewTokenSource(t.Context(), Options{
		TokenURL:     ts.url,
		ClientID:     "order-service",
		ClientSecret: fakeSecret,
		Now:          clk.Now,
	})

	return src, clk
}

func TestTokenSource_SendsAudienceAsScope(t *testing.T) {
	ts := newTokenServer(t)
	src, _ := newSource(t, ts)

	_, err := src.Token(t.Context(), "payment-service")
	require.NoError(t, err)

	form := ts.form()
	require.Equal(t, "client_credentials", form["grant_type"])
	require.Equal(t, "payment-service", form["scope"])
	require.Equal(t, "order-service", form["client_id"])
	require.Equal(t, fakeSecret, form["client_secret"])
}

func TestTokenSource_CachesUntilRefreshWindow(t *testing.T) {
	ts := newTokenServer(t)
	src, clk := newSource(t, ts)

	first, err := src.Token(t.Context(), "payment-service")
	require.NoError(t, err)

	// well inside the refresh window: served from cache, no upstream call
	clk.advance(1 * time.Minute)

	second, err := src.Token(t.Context(), "payment-service")
	require.NoError(t, err)

	require.Equal(t, first, second)
	require.Equal(t, 1, ts.hitCount("payment-service"))
}

func TestTokenSource_RefetchesAfterExpiry(t *testing.T) {
	ts := newTokenServer(t)
	src, clk := newSource(t, ts)

	first, err := src.Token(t.Context(), "payment-service")
	require.NoError(t, err)

	// past expiresAt (lifetime 5m minus the 10s guard)
	clk.advance(5 * time.Minute)

	second, err := src.Token(t.Context(), "payment-service")
	require.NoError(t, err)

	require.NotEqual(t, first, second)
	require.Equal(t, 2, ts.hitCount("payment-service"))
}

func TestTokenSource_ServesStaleTokenWhileRefreshing(t *testing.T) {
	ts := newTokenServer(t)
	src, clk := newSource(t, ts)

	first, err := src.Token(t.Context(), "payment-service")
	require.NoError(t, err)

	// block the next upstream call, then move into the refresh window: the
	// caller must be served the still-valid token instead of waiting
	ts.mu.Lock()
	ts.block = make(chan struct{})
	block := ts.block
	ts.mu.Unlock()

	clk.advance(4*time.Minute + 30*time.Second)

	done := make(chan string, 1)
	go func() {
		token, tokenErr := src.Token(t.Context(), "payment-service")
		require.NoError(t, tokenErr)
		done <- token
	}()

	select {
	case token := <-done:
		require.Equal(t, first, token, "stale but valid token must be served")
	case <-time.After(2 * time.Second):
		t.Fatal("Token blocked on a background refresh")
	}

	close(block)
}

func TestTokenSource_ConcurrentColdFetchesCollapse(t *testing.T) {
	ts := newTokenServer(t)
	src, _ := newSource(t, ts)

	const callers = 50

	var wg sync.WaitGroup
	tokens := make([]string, callers)

	for i := range callers {
		wg.Go(func() {
			token, err := src.Token(t.Context(), "payment-service")
			require.NoError(t, err)
			tokens[i] = token
		})
	}
	wg.Wait()

	require.Equal(t, 1, ts.hitCount("payment-service"))
	for _, token := range tokens {
		require.Equal(t, tokens[0], token)
	}
}

func TestTokenSource_AudiencesDoNotBlockEachOther(t *testing.T) {
	ts := newTokenServer(t)
	src, _ := newSource(t, ts)

	// hold the first audience's fetch open
	ts.mu.Lock()
	ts.block = make(chan struct{})
	block := ts.block
	ts.mu.Unlock()

	slow := make(chan struct{})
	go func() {
		defer close(slow)
		_, err := src.Token(t.Context(), "payment-service")
		require.NoError(t, err)
	}()

	// wait until the blocked request is actually in the handler
	require.Eventually(t, func() bool {
		return ts.hitCount("payment-service") == 1
	}, 2*time.Second, 10*time.Millisecond)

	// a different audience must not wait behind it
	ts.mu.Lock()
	ts.block = nil
	ts.mu.Unlock()

	fast := make(chan string, 1)
	go func() {
		token, err := src.Token(t.Context(), "limit-service")
		require.NoError(t, err)
		fast <- token
	}()

	select {
	case token := <-fast:
		require.NotEmpty(t, token)
	case <-time.After(2 * time.Second):
		t.Fatal("fetch for a second audience blocked behind the first")
	}

	close(block)
	<-slow
}

func TestTokenSource_UpstreamErrorPropagates(t *testing.T) {
	ts := newTokenServer(t)
	ts.status = http.StatusUnauthorized

	src, _ := newSource(t, ts)

	token, err := src.Token(t.Context(), "payment-service")
	require.Error(t, err)
	require.Empty(t, token, "a failed fetch must never yield an empty bearer")
	require.NotContains(t, err.Error(), fakeSecret, "client secret must not leak into errors")
}

func TestTokenSource_MissingAccessTokenIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"expires_in":300}`)
	}))
	t.Cleanup(srv.Close)

	src := NewTokenSource(context.Background(), Options{
		TokenURL:     srv.URL,
		ClientID:     "order-service",
		ClientSecret: fakeSecret,
	})

	_, err := src.Token(t.Context(), "payment-service")
	require.ErrorIs(t, err, ErrNoToken)
}

func TestTokenSource_ErrorNeverLeaksTheClientSecret(t *testing.T) {
	ts := newTokenServer(t)
	ts.status = http.StatusUnauthorized

	src, _ := newSource(t, ts)

	_, err := src.Token(t.Context(), "payment-service")
	require.Error(t, err)
	require.NotContains(t, err.Error(), fakeSecret)
}

func TestTransport_AddsBearerWithoutMutatingTheRequest(t *testing.T) {
	ts := newTokenServer(t)
	src, _ := newSource(t, ts)

	var seen string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("Authorization")
	}))
	t.Cleanup(upstream.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, upstream.URL, nil)
	require.NoError(t, err)

	transport := &Transport{Source: src, Audience: "payment-service"}
	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	require.Equal(t, "Bearer token-payment-service-1", seen)

	// the caller's request must come back untouched
	require.Empty(t, req.Header.Get("Authorization"))
}

func TestTransport_TokenFailureDoesNotReachTheUpstream(t *testing.T) {
	ts := newTokenServer(t)
	ts.status = http.StatusUnauthorized

	src, _ := newSource(t, ts)

	var called atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Store(true)
	}))
	t.Cleanup(upstream.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, upstream.URL, nil)
	require.NoError(t, err)

	transport := &Transport{Source: src, Audience: "payment-service"}
	_, err = transport.RoundTrip(req)
	require.Error(t, err)
	require.False(t, called.Load())
}
