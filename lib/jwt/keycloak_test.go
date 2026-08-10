package jwt_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flachnetz/startup/v2/lib/actor"
	"github.com/flachnetz/startup/v2/lib/jwt"
	"github.com/flachnetz/startup/v2/lib/jwtest"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
)

const testAudience = "payment-service"

// keycloakToken mints a Keycloak-shaped access token for the given audience,
// subject and roles on that audience.
func keycloakToken(store *jwtest.KeyStore, audience, subject string, roles ...string) string {
	return store.Sign(jwt.NewBuilder().
		Expiration(time.Now().Add(10*time.Minute)).
		Subject(subject).
		Audience([]string{audience}).
		Claim("email", "jane@example.com").
		Claim("resource_access", map[string]any{
			audience: map[string]any{"roles": roles},
		}))
}

// protectedRoute serves one route behind KeycloakRoleMiddleware and returns a
// request helper. The handler reports the identity and actor it sees.
func protectedRoute(t *testing.T, roles ...string) (*jwtest.KeyStore, func(bearer string) *httptest.ResponseRecorder) {
	t.Helper()

	store := jwtest.Store()
	jwks := jwtest.Serve(t, store)

	verifier, err := jwt.NewTokenVerifier(t.Context(), jwks.URL)
	require.NoError(t, err)
	t.Cleanup(verifier.Close)

	e := echo.New()
	e.GET("/protected", func(c *echo.Context) error {
		identity, ok := jwt.IdentityFrom(c.Request().Context())
		require.True(t, ok, "identity must be in the request context")

		who, ok := actor.FromContext(c.Request().Context())
		require.True(t, ok, "actor must be in the request context")

		return c.JSON(http.StatusOK, map[string]any{
			"subject":   identity.Subject,
			"audience":  identity.Audience,
			"roles":     identity.Roles,
			"actorType": string(who.Type),
			"actorId":   who.Id,
			"actorText": who.Label,
		})
	}, jwt.KeycloakRoleMiddleware(verifier, testAudience, roles...))

	return store, func(bearer string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/protected", nil)
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}

		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		return rec
	}
}

func TestKeycloakRoleMiddleware_RejectsMissingToken(t *testing.T) {
	_, call := protectedRoute(t, jwt.RoleRead)
	require.Equal(t, http.StatusUnauthorized, call("").Code)
}

func TestKeycloakRoleMiddleware_RejectsForeignSignature(t *testing.T) {
	_, call := protectedRoute(t, jwt.RoleRead)

	// signed by a key the verifier does not know
	other := jwtest.Store()
	require.Equal(t, http.StatusUnauthorized,
		call(keycloakToken(other, testAudience, "sub-1", jwt.RoleRead)).Code)
}

func TestKeycloakRoleMiddleware_RejectsWrongAudience(t *testing.T) {
	store, call := protectedRoute(t, jwt.RoleRead)

	// a token minted for another service must not be replayable here, even with
	// the right roles
	require.Equal(t, http.StatusForbidden,
		call(keycloakToken(store, "order-service", "sub-1", jwt.RoleRead)).Code)
}

func TestKeycloakRoleMiddleware_RejectsMissingRole(t *testing.T) {
	store, call := protectedRoute(t, jwt.RoleWrite)

	require.Equal(t, http.StatusForbidden,
		call(keycloakToken(store, testAudience, "sub-1", jwt.RoleRead)).Code)
}

func TestKeycloakRoleMiddleware_RejectsRolesOfAnotherClient(t *testing.T) {
	store, call := protectedRoute(t, jwt.RoleWrite)

	// roles are read from the audience's key only: write on some other client
	// grants nothing here
	token := store.Sign(jwt.NewBuilder().
		Expiration(time.Now().Add(10*time.Minute)).
		Subject("sub-1").
		Audience([]string{testAudience}).
		Claim("resource_access", map[string]any{
			"order-service": map[string]any{"roles": []string{jwt.RoleAdmin}},
		}))

	require.Equal(t, http.StatusForbidden, call(token).Code)
}

func TestKeycloakRoleMiddleware_AcceptsHigherRole(t *testing.T) {
	store, call := protectedRoute(t, jwt.RoleRead)

	// admin satisfies read
	rec := call(keycloakToken(store, testAudience, "sub-1", jwt.RoleAdmin))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"audience":"payment-service"`)
}

func TestKeycloakRoleMiddleware_HumanBecomesUserActor(t *testing.T) {
	store, call := protectedRoute(t, jwt.RoleWrite)

	rec := call(keycloakToken(store, testAudience, "9f1c-sub", jwt.RoleWrite))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"actorType":"user"`)
	require.Contains(t, rec.Body.String(), `"actorId":"9f1c-sub"`)
	require.Contains(t, rec.Body.String(), `"actorText":"jane@example.com"`)
}

func TestKeycloakRoleMiddleware_ServiceAccountBecomesServiceActor(t *testing.T) {
	store, call := protectedRoute(t, jwt.RoleWrite)

	rec := call(keycloakToken(store, testAudience, "service-account-order-service", jwt.RoleWrite))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"actorType":"service"`)
	require.Contains(t, rec.Body.String(), `"actorId":"order-service"`)
}

func TestKeycloakRoleMiddleware_RejectsPlayerToken(t *testing.T) {
	store, call := protectedRoute(t, jwt.RoleRead)

	// a player-service session token has no audience for this service and no
	// resource_access at all
	playerToken := store.Sign(jwt.NewBuilder().
		Expiration(time.Now().Add(10 * time.Minute)).
		Subject("player-123"))

	require.Equal(t, http.StatusForbidden, call(playerToken).Code)
}

func TestIdentity_HasRoleHierarchy(t *testing.T) {
	admin := jwt.Identity{Roles: []string{jwt.RoleAdmin}}
	require.True(t, admin.HasRole(jwt.RoleRead))
	require.True(t, admin.HasRole(jwt.RoleWrite))
	require.True(t, admin.HasRole(jwt.RoleAdmin))

	write := jwt.Identity{Roles: []string{jwt.RoleWrite}}
	require.True(t, write.HasRole(jwt.RoleRead))
	require.True(t, write.HasRole(jwt.RoleWrite))
	require.False(t, write.HasRole(jwt.RoleAdmin))

	read := jwt.Identity{Roles: []string{jwt.RoleRead}}
	require.True(t, read.HasRole(jwt.RoleRead))
	require.False(t, read.HasRole(jwt.RoleWrite))

	// a role outside the hierarchy must be held exactly, admin does not imply it
	require.False(t, admin.HasRole("refund-approver"))
	require.True(t, jwt.Identity{Roles: []string{"refund-approver"}}.HasRole("refund-approver"))

	require.False(t, jwt.Identity{}.HasRole(jwt.RoleRead))
}
