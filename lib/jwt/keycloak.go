package jwt

import (
	"context"
	"net/http"
	"slices"
	"strings"

	"github.com/flachnetz/startup/v2/lib/actor"
	"github.com/labstack/echo/v5"
)

// ErrForbidden is returned when a token is valid but not allowed to perform the
// request: wrong audience, or a missing role.
var ErrForbidden = echo.NewHTTPError(http.StatusForbidden, "forbidden")

// serviceAccountPrefix is how Keycloak names the subject of a
// client_credentials token: service-account-<client-id>.
const serviceAccountPrefix = "service-account-"

// Role names used across the platform. The levels are ordered: read is any call
// that changes no state, write is a normal business state change (a refund that
// follows the service's own rules included), admin is anything outside the
// business rules such as forcing a state, replaying, or deleting.
const (
	RoleRead  = "read"
	RoleWrite = "write"
	RoleAdmin = "admin"
)

// roleRank orders the platform roles so a higher level satisfies a lower one.
var roleRank = map[string]int{RoleRead: 1, RoleWrite: 2, RoleAdmin: 3}

// ClientRoles is the role list Keycloak reports for one client.
type ClientRoles struct {
	Roles []string `json:"roles"`
}

// KeycloakClaims are the claims of a Keycloak access token.
//
// DEV-NOTE: resource_access is a generic map, not a per-service struct. Every
// hand-written copy of this middleware hardcoded its own client key, which made
// the code un-shareable and, in one case, mislabelled (a field named
// PlayerService tagged limit-service).
type KeycloakClaims struct {
	Email          string                 `json:"email"`
	ResourceAccess map[string]ClientRoles `json:"resource_access"`
}

// Identity is the verified caller of a request.
type Identity struct {
	// Subject is the Keycloak sub claim: a user id for a human,
	// service-account-<client-id> for a service.
	Subject string

	// Email is set for humans only.
	Email string

	// Audience is the client id this token was accepted for, which is also the
	// key its roles were read from.
	Audience string

	// Roles are the caller's roles on Audience, without the hierarchy applied.
	Roles []string
}

// HasRole reports whether the identity satisfies role. The platform roles are
// hierarchical, so admin satisfies write and read, and write satisfies read.
// A role outside the hierarchy must be held exactly.
func (i Identity) HasRole(role string) bool {
	if slices.Contains(i.Roles, role) {
		return true
	}

	required, ok := roleRank[role]
	if !ok {
		return false
	}

	for _, held := range i.Roles {
		if rank, ok := roleRank[held]; ok && rank >= required {
			return true
		}
	}

	return false
}

// IsService reports whether the caller is a service account rather than a human.
func (i Identity) IsService() bool {
	return strings.HasPrefix(i.Subject, serviceAccountPrefix)
}

// Actor returns the audit actor for this identity: a service account becomes
// the calling service under its client id, anything else a staff user.
func (i Identity) Actor() actor.Actor {
	if i.IsService() {
		return actor.Actor{
			Type: actor.TypeService,
			Id:   strings.TrimPrefix(i.Subject, serviceAccountPrefix),
		}
	}

	return actor.Actor{Type: actor.TypeUser, Id: i.Subject, Label: i.Email}
}

type identityKey struct{}

// WithIdentity returns a context carrying the verified identity.
func WithIdentity(ctx context.Context, i Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, i)
}

// IdentityFrom returns the identity carried by ctx, if any.
func IdentityFrom(ctx context.Context) (Identity, bool) {
	i, ok := ctx.Value(identityKey{}).(Identity)
	return i, ok
}

// KeycloakRoleMiddleware verifies a Keycloak access token, requires that it was
// issued for audience and carries every role in roles on that audience, and puts
// the resulting Identity plus the audit actor into the request context.
//
// audience is this service's own Keycloak client id, which is also its service
// name. Passing no roles requires a valid token for the audience and nothing
// more.
//
// DEV-NOTE: see BauerMediaGroup-Stardust/platform-gitops
// docs/plans/keycloak-service-auth.md section 2. Authentication is the gateway's
// job, authorization is this middleware's; the audience check is what stops a
// token minted for another service from being replayed here.
func KeycloakRoleMiddleware(verifier Verifier, audience string, roles ...string) echo.MiddlewareFunc {
	return Middleware(MiddlewareOptions[KeycloakClaims]{
		TokenVerifier: verifier,
		UpdateContext: func(c *echo.Context, token Token, claims KeycloakClaims) error {
			audiences, _ := token.Audience()
			if !slices.Contains(audiences, audience) {
				return ErrForbidden
			}

			subject, _ := token.Subject()

			identity := Identity{
				Subject:  subject,
				Email:    claims.Email,
				Audience: audience,
				Roles:    claims.ResourceAccess[audience].Roles,
			}

			for _, role := range roles {
				if !identity.HasRole(role) {
					return ErrForbidden
				}
			}

			ctx := WithIdentity(c.Request().Context(), identity)
			ctx = actor.WithActor(ctx, identity.Actor())
			c.SetRequest(c.Request().WithContext(ctx))

			return nil
		},
	})
}
