package jwt

import (
	"net/http"

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
