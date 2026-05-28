package jwtest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lestrrat-go/jwx/v4/jwa"
	"github.com/lestrrat-go/jwx/v4/jwk"
	"github.com/lestrrat-go/jwx/v4/jwt"
)

// KeyStore holds an ECDSA private key for signing JWTs and a public JWKS
// that can be served from a mock JWKS endpoint for verification.
type KeyStore struct {
	PrivateKey jwk.ECDSAPrivateKey
	PublicSet  jwk.Set
}

// Store creates a new ECDSA P-256 key pair suitable for signing and
// verifying JWTs with the ES256 algorithm.
func Store() *KeyStore {
	store, err := newStore()
	if err != nil {
		panic(err)
	}

	return store
}

func Serve(t *testing.T, store *KeyStore) *httptest.Server {
	server := httptest.NewServer(handleJWKs(store.PublicSet))
	t.Cleanup(server.CloseClientConnections)
	t.Cleanup(server.Close)
	return server
}

// SignToken signs the given JWT token using the key pair's private key with ES256
// and returns the serialized compact-form token string.
func (kp *KeyStore) SignToken(token jwt.Token) string {
	signed, err := jwt.Sign(token, jwt.WithKey(jwa.ES256(), kp.PrivateKey))
	if err != nil {
		panic(fmt.Errorf("sign token: %w", err))
	}

	return string(signed)
}

// Sign signs the given JWT token using the key pair's private key with ES256
// and returns the serialized compact-form token string.
func (kp *KeyStore) Sign(builder *jwt.Builder) string {
	token, err := builder.Build()
	if err != nil {
		panic(err)
	}

	return kp.SignToken(token)
}

// newStore creates a new ECDSA P-256 key pair suitable for signing and
// verifying JWTs with the ES256 algorithm.
func newStore() (*KeyStore, error) {
	// Generate a raw ECDSA P-256 private key.
	rawKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ecdsa key: %w", err)
	}

	// Import the raw private key into a JWK.
	privKey, err := jwk.Import[jwk.ECDSAPrivateKey](rawKey)
	if err != nil {
		return nil, fmt.Errorf("import private key as jwk: %w", err)
	}

	// Tag the key with algorithm and a stable key ID.
	if err := privKey.Set(jwk.AlgorithmKey, jwa.ES256()); err != nil {
		return nil, fmt.Errorf("set algorithm on private key: %w", err)
	}

	if err := jwk.AssignKeyID(privKey); err != nil {
		return nil, fmt.Errorf("assign key id: %w", err)
	}

	// Derive the public key and build a JWKS.
	pubKey, err := privKey.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("derive public key: %w", err)
	}

	pubSet := jwk.NewSet()
	if err := pubSet.AddKey(pubKey); err != nil {
		return nil, fmt.Errorf("add public key to set: %w", err)
	}

	return &KeyStore{
		PrivateKey: privKey,
		PublicSet:  pubSet,
	}, nil
}

func handleJWKs(keys jwk.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		bytes, err := json.Marshal(keys)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_, _ = w.Write(bytes)
	}
}
