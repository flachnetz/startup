package jwt

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"regexp"

	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/labstack/echo/v5"
	"github.com/lestrrat-go/jwx/v4/jwt"
)

var reAuthBearer = regexp.MustCompile(`(?i)^Bearer\s+`)

var ErrUnauthorized = echo.NewHTTPError(http.StatusUnauthorized, "unauthorized")

// ErrNoToken unwraps to ErrUnauthorized
var ErrNoToken = ErrUnauthorized.Wrap(errors.New("no token"))

type MiddlewareOptions[Claims any] struct {
	TokenVerifier Verifier
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
				return ErrUnauthorized.Wrap(fmt.Errorf("verify token failed with error: %w", err))
			}

			if startup_base.IsDevelopment() {
				exp, _ := token.Expiration()

				slog.DebugContext(
					ctx,
					"Got valid jwt in middleware",
					slog.Any("validUntil", exp),
					slog.Any("claims", maps.Collect(token.Claims())),
				)
			}

			if err := opts.UpdateContext(c, token, claims); err != nil {
				return fmt.Errorf("update context: %w", err)
			}

			return next(c)
		}
	}
}
