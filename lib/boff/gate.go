package boff

import (
	"context"
	"strings"

	"github.com/flachnetz/startup/v2/lib/jwt"
)

// Action is one row in the actions table on a backoffice page. When StatusText
// is non-empty the row renders a static label instead of a button (e.g.
// "Cancelled"); otherwise a plain HTML form that POSTs to Endpoint.
//
// The page ships no JavaScript: the form is a form, and a ConfirmMessage renders
// a Bootstrap modal driven by Bootstrap's own JS. Endpoint must therefore be a
// URL the browser can resolve - a service behind backoffice builds it from the
// base path backoffice sends with the fragment request, not from its own
// internal path.
type Action struct {
	Description    string // e.g. "Cancel item Sword-Pack"
	ButtonText     string // e.g. "Cancel"
	Endpoint       string // e.g. "/orders/backoffice/v1/orders/123/items/1/cancel"
	ConfirmMessage string // optional confirmation prompt; empty = submit immediately
	StatusText     string // non-empty = show label instead of button

	// true renders a link to Endpoint instead of a form, for an action that only
	// navigates. Ignores ConfirmMessage.
	Link bool

	// RequiredRole gates the whole action: "write" or "admin" means that role on
	// this service's own audience, "payment-service:admin" names another
	// audience. Empty means always shown. A viewer who may not perform the action
	// does not see it at all - a disabled button still tells a read-only user
	// which endpoint to curl.
	RequiredRole string
}

// SummaryItem is one label/value row shown above the page content, describing
// the current state of the tracked object. Ordered slice (not a map) so the page
// renders stably.
type SummaryItem struct {
	Label string
	Value string
	// Link, when non-empty, renders Value as an <a href> to this URL instead of
	// plain text - e.g. a cross-service backoffice link to the page that owns the
	// referenced entity (a payment or draw history page behind the api-gateway).
	Link string

	// RequiredRole gates the Link only, in the same notation as
	// Action.RequiredRole. A denied item still shows Label and Value, just not as
	// an anchor: the value itself is not the secret, the page behind it is.
	RequiredRole string
}

// mayPerform reports whether viewer satisfies required. An empty required is
// ungated; no viewer denies everything gated.
//
// ponytail: a role qualified with a foreign audience is denied unless the viewer
// was issued for that audience. Cross-audience display via the advisory
// Actor-Roles header is only worth building when a page actually mixes services.
func mayPerform(viewer *jwt.Identity, required string) bool {
	if required == "" {
		return true
	}

	if viewer == nil {
		return false
	}

	audience, role := "", required
	if before, after, ok := strings.Cut(required, ":"); ok {
		audience, role = before, after
	}

	if audience != "" && audience != viewer.Audience {
		return false
	}

	return viewer.HasRole(role)
}

// Gate drops the actions the viewer may not perform and demotes denied summary
// links to plain values.
func Gate(viewer *jwt.Identity, summary []SummaryItem, actions []Action) ([]SummaryItem, []Action) {
	gatedSummary := make([]SummaryItem, 0, len(summary))
	for _, item := range summary {
		if item.Link != "" && !mayPerform(viewer, item.RequiredRole) {
			item.Link = ""
		}
		gatedSummary = append(gatedSummary, item)
	}

	gatedActions := make([]Action, 0, len(actions))
	for _, action := range actions {
		if mayPerform(viewer, action.RequiredRole) {
			gatedActions = append(gatedActions, action)
		}
	}

	if len(summary) == 0 {
		gatedSummary = nil
	}

	if len(gatedActions) == 0 {
		gatedActions = nil
	}

	return gatedSummary, gatedActions
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
