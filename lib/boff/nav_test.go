package boff

import (
	"bytes"
	"strings"
	"testing"

	"github.com/flachnetz/startup/v2/lib/jwt"
)

func navLinks() NavBlock {
	return NavBlock{
		{Label: "Orders", Href: "/orders", Active: true},
		{Label: "Payments", Href: "/payments"},
		{Label: "Admin", Href: "/admin", RequiredRole: jwt.RoleAdmin},
	}
}

func renderNav(t *testing.T, viewer *jwt.Identity) string {
	t.Helper()

	var buf bytes.Buffer
	if err := Render(&buf, RenderConfig{Title: "t", Viewer: viewer, Blocks: []Block{navLinks()}}); err != nil {
		t.Fatalf("execute template: %v", err)
	}

	return buf.String()
}

// The nav renders as a <nav> and marks the active entry.
func TestNavBlockRendersNavElementWithActive(t *testing.T) {
	out := renderNav(t, identity("order-service", jwt.RoleAdmin))

	if !strings.Contains(out, "<nav") {
		t.Errorf("nav did not render a <nav> element:\n%s", out)
	}
	if !strings.Contains(out, `<a class="nav-link active" href="/orders" aria-current="page">Orders</a>`) {
		t.Errorf("active entry not marked:\n%s", out)
	}
	if !strings.Contains(out, `href="/payments"`) {
		t.Errorf("ungated entry missing:\n%s", out)
	}
}

// A viewer without the role does not see the gated entry.
func TestNavBlockGatesLinks(t *testing.T) {
	out := renderNav(t, identity("order-service", jwt.RoleRead))
	if strings.Contains(out, "/admin") {
		t.Errorf("read viewer saw an admin-gated nav link:\n%s", out)
	}
	if !strings.Contains(out, "/orders") {
		t.Errorf("ungated nav link was hidden:\n%s", out)
	}
}
