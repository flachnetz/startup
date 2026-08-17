package boff

import "html/template"

// Role names the permission a gated element requires, in the notation the whole
// package shares: "write" or "admin" means that role on this service's own
// audience; "payment-service:admin" names a role on a foreign audience. The
// empty Role is ungated - always shown.
type Role string

const (
	RoleAdmin = "admin"
	RoleWrite = "write"
	RoleRead  = "read"
)

func RoleOf(audience, role string) Role {
	return Role(audience + ":" + role)
}

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
	RequiredRole Role
}

// GatingRole implements HasRequiredRole.
func (a Action) GatingRole() Role { return a.RequiredRole }

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
	RequiredRole Role

	// JSON, when non-empty, renders Value as a collapsible <details> whose body is
	// this payload, highlighted like a ledger record - for a row that summarises a
	// structure the operator sometimes needs verbatim (an order item as stored).
	JSON string
}

// JSONHTML is SummaryItem.JSON colourised, same treatment as a record payload.
func (i SummaryItem) JSONHTML() template.HTML {
	return template.HTML(HighlightJSON(i.JSON)) //nolint:gosec // HighlightJSON escapes its input
}

// HasRequiredRole is implemented by anything a viewer can be gated against - an
// Action, a NavLink. GateSlice uses it to drop the entries a viewer may not see,
// so a new gated element type only has to report its role, not its own filter.
type HasRequiredRole interface {
	GatingRole() Role
}

// GateSlice returns the entries rc's viewer may see, dropping the rest. Returns
// nil when nothing survives, so an empty block renders nothing.
func GateSlice[T HasRequiredRole](rc RenderContext, values []T) []T {
	gated := make([]T, 0, len(values))
	for _, v := range values {
		if rc.May(v.GatingRole()) {
			gated = append(gated, v)
		}
	}

	if len(gated) == 0 {
		return nil
	}

	return gated
}

// demoteLinks returns a copy of summary with the links rc's viewer may not follow
// blanked out - the label and value survive, only the anchor is dropped. Unlike
// gateSlice, a denied item is kept, since its value is not the secret; only the
// page behind the link is.
func demoteLinks(rc RenderContext, summary []SummaryItem) []SummaryItem {
	demoted := make([]SummaryItem, len(summary))
	for i, item := range summary {
		if item.Link != "" && !rc.May(item.RequiredRole) {
			item.Link = ""
		}
		demoted[i] = item
	}

	return demoted
}
