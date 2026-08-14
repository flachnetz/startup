package boff

import (
	"context"
	"strings"

	"github.com/flachnetz/startup/v2/lib/jwt"
)

// RenderContext carries everything a Block needs while it renders - currently the
// viewing identity to gate against. It is passed to every Block.Render, including
// nested ones: a container block hands its own rc straight to its children, so
// gating reaches any depth.
//
// It is a struct (not a bare *jwt.Identity) so more render-time values can be
// propagated later - a base path for URL building, a nonce, feature flags -
// without churning the Block interface again.
type RenderContext struct {
	// Viewer is the identity gated blocks filter themselves against. Nil means
	// fail closed: May reports false for everything gated.
	Viewer *jwt.Identity
}

// May reports whether the context's viewer satisfies required, in the notation
// of Action.RequiredRole ("write", "admin", or "audience:role"). An empty
// required is ungated and always allowed; a nil viewer denies everything gated.
//
// This is the one check both Go blocks and the "may" template function share, so
// a block's Render and its template agree on who may see what. Reuse it when you
// write your own block instead of reaching for jwt directly.
func (rc RenderContext) May(required Role) bool {
	return MayPerform(rc.Viewer, required)
}

// MayPerform reports whether viewer satisfies required. An empty required is
// ungated; no viewer denies everything gated.
//
// ponytail: a role qualified with a foreign audience is denied unless the viewer
// was issued for that audience. Cross-audience display via the advisory
// Actor-Roles header is only worth building when a page actually mixes services.
func MayPerform(viewer *jwt.Identity, required Role) bool {
	if required == "" {
		return true
	}

	if viewer == nil {
		return false
	}

	audience, role := "", string(required)
	if before, after, ok := strings.Cut(string(required), ":"); ok {
		audience, role = before, after
	}

	if audience != "" && audience != viewer.Audience {
		return false
	}

	return viewer.HasRole(role)
}

// ViewerOf returns the identity used for gating: the explicit override, else the
// verified identity of the request, else nil.
func ViewerOf(ctx context.Context, override *jwt.Identity) *jwt.Identity {
	if override != nil {
		return override
	}

	if identity, ok := jwt.IdentityFrom(ctx); ok {
		return &identity
	}

	return nil
}
