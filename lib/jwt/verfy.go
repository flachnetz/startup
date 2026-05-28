package jwt

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/flachnetz/startup/v2/startup_tracing"
	"github.com/jwx-go/jwkfetch/v4"
	"github.com/lestrrat-go/httprc/v3"
	"github.com/lestrrat-go/jwx/v4/jwt"
)

type Token = jwt.Token

func NewBuilder() *jwt.Builder {
	return jwt.NewBuilder()
}

type TokenVerifier struct {
	Close context.CancelFunc
	cache *jwkfetch.Cache
	url   string
}

func NewTokenVerifier(ctx context.Context, url string) (*TokenVerifier, error) {
	ctx, cancelContext := context.WithCancel(ctx)

	httpClient := startup_tracing.WithSpanPropagation(&http.Client{
		Timeout: 3 * time.Second,
	})

	httprcClient := httprc.NewClient(httprc.WithHTTPClient(httpClient))

	cache, err := jwkfetch.NewCache(ctx, httprcClient)
	if err != nil {
		cancelContext()
		return nil, fmt.Errorf("create http client: %w", err)
	}

	err = cache.Register(ctx, url,
		jwkfetch.WithWaitReady(true),
		jwkfetch.WithMinInterval(10*time.Second),
		jwkfetch.WithMaxInterval(60*time.Second))

	if err != nil {
		// cancel the background process due to the rror
		cancelContext()

		// stop watching this url
		return nil, fmt.Errorf("warm jwk cache: %w", err)
	}

	v := &TokenVerifier{
		Close: cancelContext,
		cache: cache,
		url:   url,
	}

	return v, nil
}

func (v *TokenVerifier) Verify(ctx context.Context, rawToken string) (jwt.Token, error) {
	keySet, err := v.cache.Lookup(ctx, v.url)
	if err != nil {
		return nil, fmt.Errorf("get jwt keyset: %w", err)
	}

	token, err := jwt.Parse(
		[]byte(rawToken),

		// verify signature using JWKS
		jwt.WithKeySet(keySet),

		// validate exp/nbf/iat automatically
		jwt.WithValidate(true),
	)

	if err != nil {
		return nil, fmt.Errorf("verify jwt: %w", err)
	}

	return token, nil
}
