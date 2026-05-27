package auth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/flachnetz/startup/v2/startup_base"
)

type Transport struct {
	Next http.RoundTripper

	TokenURL     string
	ClientId     string
	ClientSecret string

	state atomic.Pointer[authState]
}

func (k *Transport) RoundTrip(request *http.Request) (*http.Response, error) {
	state, err := k.ensureAuthState()
	if err != nil {
		return nil, fmt.Errorf("refresh access token: %w", err)
	}

	request.Header.Set("Authorization", "Bearer "+state.AccessToken)
	defer request.Header.Del("Authorization")

	return k.execute(request)
}

func (k *Transport) execute(request *http.Request) (*http.Response, error) {
	next := k.Next
	if next == nil {
		next = http.DefaultTransport
	}

	return next.RoundTrip(request)
}

func (k *Transport) ensureAuthState() (*authState, error) {
	if state := k.state.Load(); state != nil {
		valid := time.Until(state.ExpiresAt) > 10*time.Second
		if valid {
			return state, nil
		}
	}

	now := time.Now()

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", k.ClientId)
	form.Set("client_secret", k.ClientSecret)
	body := bytes.NewReader([]byte(form.Encode()))

	// we need a new access token
	req, err := http.NewRequest("POST", k.TokenURL, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := k.execute(req)
	if err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	}

	defer startup_base.Close(resp.Body, "Close response body")

	var token tokenJSON
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	state := &authState{
		ExpiresAt:   now.Add(time.Duration(token.ExpiresIn) * time.Second),
		AccessToken: token.AccessToken,
	}

	k.state.Store(state)

	return state, nil
}

type authState struct {
	ExpiresAt   time.Time
	AccessToken string
}

type tokenJSON struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
}
