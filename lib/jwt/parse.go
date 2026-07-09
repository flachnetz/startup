package jwt

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/lestrrat-go/jwx/v4/jwt"
)

type Verifier interface {
	// Verify parses & verifies the token.
	Verify(ctx context.Context, rawToken string) (jwt.Token, error)
}

// ParseJWT parses and validates the provided token. The claims are then parsed into
// the provided type T and returned on success.
func ParseJWT[T any](ctx context.Context, verifier Verifier, rawToken string) (T, jwt.Token, error) {
	var tZero T

	token, err := verifier.Verify(ctx, rawToken)
	if err != nil {
		return tZero, nil, fmt.Errorf("verify token: %w", err)
	}

	claims, err := parseClaims[T](token)
	if err != nil {
		return tZero, nil, fmt.Errorf("parse claims: %w", err)
	}

	return claims, token, nil
}

func parseClaims[T any](token jwt.Token) (T, error) {
	var tZero T

	untypedClaims := maps.Collect(token.Claims())

	jsonClaims, err := json.Marshal(untypedClaims)
	if err != nil {
		return tZero, fmt.Errorf("serialize claims to json: %w", err)
	}

	var claims T
	err = json.Unmarshal(jsonClaims, &claims)
	if err != nil {
		return tZero, fmt.Errorf("deserialize claims from json: %w", err)
	}

	return claims, nil
}
