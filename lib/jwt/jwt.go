package jwt

import (
	"context"
	"crypto/tls"
	"github.com/benbjohnson/clock"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jwt"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type JwtStruct struct {
	UserName       string   `json:"user_name"`
	Uxid           string   `json:"uxid"`
	SessionID      string   `json:"sessionId"`
	CustomerNumber string   `json:"customerNumber"`
	Locale         string   `json:"locale"`
	DeviceID       string   `json:"deviceId"`
	Authorities    []string `json:"authorities"`
	ClientID       string   `json:"client_id"`
	Site           string   `json:"site"`
	Scope          []string `json:"scope"`
}

type JwtService struct {
	clock     clock.Clock
	jwkKeySet *jwk.Set
}

func NewJwtService(jwkResourceUrl string, clock clock.Clock) (*JwtService, error) {
	set, err := GetJwk(jwkResourceUrl, http.Client{
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

	// parse and check signature
	t, err := jwt.ParseBytes([]byte(authHeader[7:]), jwt.WithOpenIDClaims(), jwt.WithKeySet(j.jwkKeySet))
	if err != nil {
		return nil, err
	}

	// now verify content
	err = jwt.Verify(t, jwt.WithClock(j.clock))
	if err != nil {
		return nil, err
	}

	if v, ok := t.Get("UserName"); ok {
		claims.UserName = v.(string)
	}
	if v, ok := t.Get("Uxid"); ok {
		claims.Uxid = v.(string)
	}
	if v, ok := t.Get("SessionID"); ok {
		claims.SessionID = v.(string)
	}
	if v, ok := t.Get("CustomerNumber"); ok {
		claims.CustomerNumber = strconv.Itoa(int(v.(float64)))
	}
	if v, ok := t.Get("Locale"); ok {
		claims.Locale = v.(string)
	}
	if v, ok := t.Get("DeviceID"); ok {
		claims.DeviceID = v.(string)
	}
	if v, ok := t.Get("Authorities"); ok {
		claims.Authorities = v.([]string)
	}
	if v, ok := t.Get("ClientID"); ok {
		claims.ClientID = v.(string)
	}
	if v, ok := t.Get("Site"); ok {
		claims.Site = strings.ToLower(v.(string))
	}
	if v, ok := t.Get("Scope"); ok {
		claims.Scope = v.([]string)
	}

	return claims, err
}

func (j *JwtService) GetJwtTokenFromContext(ctx context.Context) (*JwtStruct, error) {

	if j.jwkKeySet == nil {
		return nil, errors.New("cannot verify jwt because jwk key set is missing")
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.New("cannot read header information")
	}

	authHeader, exists := md["authorization"]
	if !exists || len(authHeader) != 1 { // return empty claim when not jwt token is given
		return nil, nil
	}

	return j.GetJwtToken(authHeader[0])
}

func GetJwk(url string, httpClient http.Client) (*jwk.Set, error) {

	set, err := jwk.Fetch(url, jwk.WithHTTPClient(&httpClient))
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
