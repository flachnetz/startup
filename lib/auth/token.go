// Package auth obtains service-to-service access tokens from Keycloak using the
// OAuth2 client_credentials grant.
//
// Tokens are cached per audience. A cache hit never takes a lock and never waits
// on the network: once a token is warm, callers are served from an atomic read
// while refreshes happen in the background. Concurrent cold fetches for the same
// audience collapse into a single request; different audiences never block each
// other.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flachnetz/startup/v2/lib/clock"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"golang.org/x/sync/singleflight"
)

const (
	// refreshLead is how long before expiry a token is refreshed in the
	// background while still being served to callers.
	refreshLead = 60 * time.Second

	// expiryGuard keeps a safety margin so a token handed out here does not
	// expire in flight at the callee.
	expiryGuard = 10 * time.Second

	// backgroundFetchTimeout bounds a refresh that no caller is waiting for.
	backgroundFetchTimeout = 10 * time.Second
)

// ErrNoToken reports that Keycloak returned a response without an access token.
var ErrNoToken = errors.New("keycloak returned no access token")

// TokenSource issues and caches client_credentials tokens for one Keycloak
// client. It is safe for concurrent use and must be created with NewTokenSource.
type TokenSource struct {
	tokenURL     string
	clientID     string
	clientSecret string
	httpClient   *http.Client

	// now is injectable for tests; defaults to clock.GlobalClock.Now.
	now func() time.Time

	// baseContext parents background refreshes so they outlive the request that
	// triggered them but still stop on shutdown.
	baseContext context.Context

	// entries maps audience -> *atomic.Pointer[cachedToken]. Reads are lock-free.
	entries sync.Map

	// fetches collapses concurrent fetches for the same audience.
	fetches singleflight.Group
}

// cachedToken is an immutable snapshot of one audience's token.
type cachedToken struct {
	value string

	// refreshAfter is when a background refresh should start. Between
	// refreshAfter and expiresAt the token is still served to callers.
	refreshAfter time.Time

	// expiresAt is the last moment the token may be handed out.
	expiresAt time.Time
}

// Options configures a TokenSource.
type Options struct {
	// TokenURL is the Keycloak token endpoint, taken from the rotator-managed
	// secret (token_endpoint).
	TokenURL string

	// ClientID and ClientSecret authenticate this service against Keycloak.
	ClientID     string
	ClientSecret string

	// HTTPClient is used for token requests. Defaults to a client with a 5s
	// timeout.
	HTTPClient *http.Client

	// Now defaults to clock.GlobalClock.Now.
	Now func() time.Time
}

// NewTokenSource returns a TokenSource. ctx parents background refreshes, so
// pass the application context, not a request context.
func NewTokenSource(ctx context.Context, opts Options) *TokenSource {
	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}

	now := opts.Now
	if now == nil {
		now = clock.GlobalClock.Now
	}

	return &TokenSource{
		tokenURL:     opts.TokenURL,
		clientID:     opts.ClientID,
		clientSecret: opts.ClientSecret,
		httpClient:   httpClient,
		now:          now,
		baseContext:  ctx,
	}
}

// Token returns an access token valid for audience. A warm token is returned
// without blocking; a token inside its refresh window is returned immediately
// while a refresh runs in the background. Only a cold or expired token makes the
// caller wait, and then only one caller per audience actually calls Keycloak.
func (t *TokenSource) Token(ctx context.Context, audience string) (string, error) {
	slot := t.slot(audience)
	now := t.now()

	if token := slot.Load(); token != nil {
		switch {
		case now.Before(token.refreshAfter):
			return token.value, nil

		case now.Before(token.expiresAt):
			// still usable: serve it and refresh out of band so no request pays
			// the latency of a token round trip
			t.refreshInBackground(audience)
			return token.value, nil
		}
	}

	return t.fetchShared(ctx, audience)
}

// slot returns the cache slot for an audience, creating it on first use.
func (t *TokenSource) slot(audience string) *atomic.Pointer[cachedToken] {
	if existing, ok := t.entries.Load(audience); ok {
		return existing.(*atomic.Pointer[cachedToken])
	}

	actual, _ := t.entries.LoadOrStore(audience, &atomic.Pointer[cachedToken]{})
	return actual.(*atomic.Pointer[cachedToken])
}

// fetchShared fetches a token, collapsing concurrent calls for the same
// audience into one request. Callers for other audiences are unaffected.
func (t *TokenSource) fetchShared(ctx context.Context, audience string) (string, error) {
	value, err, _ := t.fetches.Do(audience, func() (any, error) {
		return t.fetchAndStore(ctx, audience)
	})
	if err != nil {
		return "", err
	}

	return value.(string), nil
}

// refreshInBackground starts a refresh for audience unless one is already in
// flight. It never blocks the caller.
func (t *TokenSource) refreshInBackground(audience string) {
	go func() {
		// DEV-NOTE: detached from the triggering request on purpose - that
		// request is served from cache and will finish long before this.
		ctx, cancel := context.WithTimeout(t.baseContext, backgroundFetchTimeout)
		defer cancel()

		if _, err, _ := t.fetches.Do(audience, func() (any, error) {
			return t.fetchAndStore(ctx, audience)
		}); err != nil {
			slog.WarnContext(ctx, "background refresh of service token failed",
				slog.String("audience", audience), sl.Error(err))
		}
	}()
}

// fetchAndStore performs the client_credentials request and updates the cache.
func (t *TokenSource) fetchAndStore(ctx context.Context, audience string) (string, error) {
	token, lifetime, err := t.request(ctx, audience)
	if err != nil {
		return "", err
	}

	now := t.now()
	t.slot(audience).Store(&cachedToken{
		value:        token,
		refreshAfter: now.Add(refreshWindow(lifetime)),
		expiresAt:    now.Add(max(lifetime-expiryGuard, 0)),
	})

	return token, nil
}

// refreshWindow returns how long a token of the given lifetime is served before
// a background refresh starts. Short-lived tokens are refreshed at half their
// lifetime so the window never lands in the past.
func refreshWindow(lifetime time.Duration) time.Duration {
	if lifetime <= 2*refreshLead {
		return lifetime / 2
	}

	return lifetime - refreshLead
}

// request calls the Keycloak token endpoint. The audience is requested as a
// scope, which makes Keycloak narrow the token's aud claim to that service.
func (t *TokenSource) request(ctx context.Context, audience string) (string, time.Duration, error) {
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {t.clientID},
		"client_secret": {t.clientSecret},
		"scope":         {audience},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.tokenURL,
		strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("fetch token for %q: %w", audience, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		// the body can carry the client secret back in an error description,
		// so only the status is reported
		_, _ = io.Copy(io.Discard, resp.Body)
		return "", 0, fmt.Errorf("fetch token for %q: unexpected status %d", audience, resp.StatusCode)
	}

	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", 0, fmt.Errorf("decode token response for %q: %w", audience, err)
	}

	if body.AccessToken == "" {
		return "", 0, fmt.Errorf("audience %q: %w", audience, ErrNoToken)
	}

	lifetime := time.Duration(body.ExpiresIn) * time.Second
	if lifetime <= 0 {
		// Keycloak always sends expires_in; treat a missing value as
		// single-use rather than as an immortal token
		lifetime = expiryGuard
	}

	return body.AccessToken, lifetime, nil
}

// Transport adds a client-credentials bearer token for one audience to every
// request it forwards. One Transport per callee audience; all of them should
// share a single TokenSource so the token cache is shared too.
type Transport struct {
	// Source issues the tokens. Required.
	Source *TokenSource

	// Audience is the callee's Keycloak client id, which becomes the token's
	// aud claim. Required.
	Audience string

	// Next defaults to http.DefaultTransport.
	Next http.RoundTripper
}

// RoundTrip attaches the bearer to a clone of the request, because RoundTrip
// must not modify the request it is given.
func (t *Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	token, err := t.Source.Token(r.Context(), t.Audience)
	if err != nil {
		return nil, fmt.Errorf("authenticate request to %q: %w", t.Audience, err)
	}

	clone := r.Clone(r.Context())
	clone.Header.Set("Authorization", "Bearer "+token)

	next := t.Next
	if next == nil {
		next = http.DefaultTransport
	}

	return next.RoundTrip(clone)
}
