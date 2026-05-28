package jwt

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/labstack/echo/v5"
	"github.com/lestrrat-go/jwx/v4/jwt"
)

var reAuthBearer = regexp.MustCompile(`(?i)^Bearer\s+`)

var ErrNoToken = errors.New("no token")

type VerifyTokenError struct {
	Cause error
}

func (v VerifyTokenError) Error() string {
	return fmt.Sprintf("verify token: %s", v.Cause.Error())
}

func (v VerifyTokenError) Unwrap() error {
	return v.Cause
}

type MiddlewareOptions[Claims any] struct {
	TokenVerifier *TokenVerifier
	UpdateContext func(c *echo.Context, token jwt.Token, claims Claims) error
}

func Middleware[Claims any](opts MiddlewareOptions[Claims]) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			rawToken := reAuthBearer.ReplaceAllLiteralString(
				c.Request().Header.Get("authorization"), "",
			)

			if rawToken == "" {
				return ErrNoToken
			}

			ctx := c.Request().Context()

			claims, token, err := ParseJWT[Claims](ctx, opts.TokenVerifier, rawToken)
			if err != nil {
				return VerifyTokenError{Cause: err}
			}

			if startup_base.IsDevelopment() {
				fmt.Println()

				for key, value := range token.Claims() {
					// print the raw claims
					fmt.Println("Claim:", key, value)
				}
			}

			if err := opts.UpdateContext(c, token, claims); err != nil {
				return fmt.Errorf("update context: %w", err)
			}

			return next(c)
		}
	}
}
