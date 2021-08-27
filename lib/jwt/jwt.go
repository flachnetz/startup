package jwt

import (
	"crypto/tls"
	"github.com/benbjohnson/clock"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jwt"
	"github.com/pkg/errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type JwtStruct struct {
	UserName       string        `json:"user_name"`
	Uxid           string        `json:"uxid"`
	SessionID      string        `json:"sessionId"`
	CustomerNumber string        `json:"customerNumber"`
	Locale         string        `json:"locale"`
	DeviceID       string        `json:"deviceId"`
	Authorities    []interface{} `json:"authorities"`
	ClientID       string        `json:"client_id"`
	Site           string        `json:"site"`
	Scope          []interface{} `json:"scope"`
	GrantType          []interface{} `json:"grant_type"`
}

type JwtService struct {
	clock     clock.Clock
	jwkKeySet *jwk.Set
}

func NewJwtService(jwkResourceUrl string, clock clock.Clock) (*JwtService, error) {
	set, err := GetJwk(jwkResourceUrl, &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to receive jwk: %s")
	}
	return &JwtService{jwkKeySet: set, clock: clock}, nil
}

func (j *JwtService) GetJwtToken(authHeader string) (*JwtStruct, error) {
	claims := &JwtStruct{}

	if strings.Contains(authHeader, "Bearer") {
		authHeader = authHeader[7:]
	}

	// parse and check signature
	t, err := jwt.ParseBytes([]byte(authHeader), jwt.WithOpenIDClaims(), jwt.WithKeySet(j.jwkKeySet))
	if err != nil {
		return nil, err
	}

	// now verify content
	err = jwt.Verify(t, jwt.WithClock(j.clock))
	if err != nil {
		return nil, err
	}

	if v, ok := t.Get("userName"); ok {
		claims.UserName = v.(string)
	}

	if v, ok := t.Get("uxid"); ok {
		claims.Uxid = v.(string)
	}

	if v, ok := t.Get("sessionID"); ok {
		claims.SessionID = v.(string)
	}

	if v, ok := t.Get("customerNumber"); ok {
		claims.CustomerNumber = strconv.Itoa(int(v.(float64)))
	}

	if v, ok := t.Get("locale"); ok {
		claims.Locale = v.(string)
	}

	if v, ok := t.Get("deviceID"); ok {
		claims.DeviceID = v.(string)
	}

	if v, ok := t.Get("authorities"); ok {
		claims.Authorities = v.([]interface{})
	}

	if v, ok := t.Get("clientID"); ok {
		claims.ClientID = v.(string)
	}

	if v, ok := t.Get("site"); ok {
		claims.Site = strings.ToLower(v.(string))
	}

	if v, ok := t.Get("scope"); ok {
		claims.Scope = v.([]interface{})
	}

	if v, ok := t.Get("grant_type"); ok {
		claims.GrantType = v.([]interface{})
	}

	return claims, err
}

func (j *JwtService) GetJwtTokenFromRequest(req *http.Request) (*JwtStruct, error) {
	if j.jwkKeySet == nil {
		return nil, errors.New("cannot verify jwt because jwk key set is missing")
	}

	authHeader := req.Header.Get("Authorization")
	if authHeader == "" {
		authHeader = req.Header.Get("authorization")
	}
	if authHeader == "" {
		return nil, nil
	}

	return j.GetJwtToken(authHeader[7:])
}

func GetJwk(url string, httpClient *http.Client) (*jwk.Set, error) {
	set, err := jwk.Fetch(url, jwk.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}

	if len(set.Keys) == 0 {
		return nil, errors.New("no jwk keys found")
	}

	var key interface{}
	if err := set.Keys[0].Raw(&key); err != nil {
		return nil, err
	}

	return set, nil
}
