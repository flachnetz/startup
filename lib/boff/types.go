package boff

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
}

// GateActions returns the actions rc's viewer may perform, dropping the rest.
// Exposed so a custom block that renders its own actions gates them the same way
// ActionsBlock does.
func GateActions(rc RenderContext, actions []Action) []Action {
	gated := make([]Action, 0, len(actions))
	for _, action := range actions {
		if rc.May(action.RequiredRole) {
			gated = append(gated, action)
		}
	}

	if len(gated) == 0 {
		return nil
	}

	return gated
}

// DemoteLinks returns a copy of summary with the links rc's viewer may not follow
// blanked out - the label and value survive, only the anchor is dropped. Exposed
// so a custom block that renders summary-like rows gates its links the same way
// SummaryBlock does.
func DemoteLinks(rc RenderContext, summary []SummaryItem) []SummaryItem {
	demoted := make([]SummaryItem, len(summary))
	for i, item := range summary {
		if item.Link != "" && !rc.May(item.RequiredRole) {
			item.Link = ""
		}
		demoted[i] = item
	}

	return demoted
}
