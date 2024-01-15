package jwt

import (
	"github.com/golang-jwt/jwt/v5"
	"net/http"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/pkg/errors"
)

type MockService struct {
	Jwt JwtStruct
}

func (m *MockService) GetJwtToken(authHeader string) (*JwtStruct, error) {
	return GetJwtToken(authHeader)
}

func (m *MockService) GetJwtTokenFromRequest(req *http.Request) (*JwtStruct, error) {
	authHeader := req.Header.Get("authorization")
	if authHeader == "" {
		return nil, errors.New("authorization header is empty or not set")
	}

	return m.GetJwtToken(authHeader[7:])
}

func CreateJWT(clock clock.Clock, secret, site, customerNumber string) (string, error) {
	// Define the claims for the JWT
	claims := jwt.MapClaims{
		"site":           site,
		"customerNumber": customerNumber,
		"iat":            clock.Now().Unix(),
		"exp":            clock.Now().Add(time.Hour * 24).Unix(),
	}

	// Create a new JWT token with the claims
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// Sign the token with a secret key
	secretKey := []byte(secret)
	jwtString, err := token.SignedString(secretKey)
	if err != nil {
		return "", err
	}

	return jwtString, nil
}
