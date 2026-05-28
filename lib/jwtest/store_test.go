package jwtest

import (
	"maps"
	"testing"
	"time"

	"github.com/flachnetz/startup/v2/lib/jwt"
	"github.com/stretchr/testify/require"
)

func TestGenerateKeyPair(t *testing.T) {
	store := Store()
	url := Serve(t, store).URL

	// create a signed token
	signed := store.Sign(jwt.NewBuilder().
		Expiration(time.Now().Add(10*time.Minute)).
		Claim("foo", map[string]string{"foo": "bar"}).
		Claim("bla", "blabla").
		Audience([]string{"service-a"}))

	t.Log(signed)

	// create a Verifier and point it to the token
	ctx := t.Context()
	verifier, err := jwt.NewTokenVerifier(ctx, url)
	require.NoError(t, err)

	parsed, err := verifier.Verify(ctx, signed)
	require.NoError(t, err)

	t.Log(maps.Collect(parsed.Claims()))
}
