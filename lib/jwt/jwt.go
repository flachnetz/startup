package jwt

import (
	"context"
	"crypto/tls"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jwt"
	"github.com/pkg/errors"
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
	GrantType      string        `json:"grant_type"`
}

type Service interface {
	GetJwtToken(authHeader string) (*JwtStruct, error)
	GetJwtTokenFromRequest(req *http.Request) (*JwtStruct, error)
}

type jwtService struct {
	clock     clock.Clock
	jwkKeySet jwk.Set
}

func NewJwtService(jwkResourceUrl string, clock clock.Clock) (Service, error) {
	set, err := GetJwk(context.Background(), jwkResourceUrl, &http.Client{
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
	return &jwtService{jwkKeySet: set, clock: clock}, nil
}

func (j *jwtService) GetJwtToken(authHeader string) (*JwtStruct, error) {
	return GetJwtToken(authHeader, jwt.WithKeySet(j.jwkKeySet), jwt.WithClock(j.clock), jwt.WithValidate(true))
}

func (j *jwtService) GetJwtTokenFromRequest(req *http.Request) (*JwtStruct, error) {
	if j.jwkKeySet == nil {
		return nil, errors.New("cannot verify jwt because jwk key set is missing")
	}

	authHeader := req.Header.Get("authorization")
	if authHeader == "" {
		return nil, jwt.NewValidationError(errors.New("authorization header is empty or not set"))
	}

	return j.GetJwtToken(authHeader[7:])
}

func GetJwk(ctx context.Context, url string, httpClient *http.Client) (jwk.Set, error) {
	set, err := jwk.Fetch(ctx, url, jwk.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}

	if set.Len() == 0 {
		return nil, errors.New("no jwk keys found")
	}

	return set, nil
}

func GetJwtToken(authHeader string, options ...jwt.ParseOption) (*JwtStruct, error) {
	claims := &JwtStruct{}

	if strings.Contains(authHeader, "Bearer") {
		authHeader = authHeader[7:]
	}

	// parse and check signature
	t, err := jwt.ParseString(authHeader, options...)
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
		t := reflect.TypeOf(v)
		switch t.Kind() {
		case reflect.Int:
			claims.CustomerNumber = strconv.Itoa(v.(int))
		case reflect.Float64:
			claims.CustomerNumber = strconv.Itoa(int(v.(float64)))
		case reflect.String:
			claims.CustomerNumber = v.(string)
		default:
			return nil, errors.New("customerNumber is not a string or float64")
		}
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
		claims.GrantType = v.(string)
	}

	return claims, err
}
