package jwt

import (
	"net/http"
)

type MockService struct {
	Jwt JwtStruct
}

func (m *MockService) GetJwtToken(authHeader string) (*JwtStruct, error) {
	return &m.Jwt, nil
}

func (m *MockService) GetJwtTokenFromRequest(req *http.Request) (*JwtStruct, error) {
	return &m.Jwt, nil
}
