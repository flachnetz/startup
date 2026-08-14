package boff

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/flachnetz/startup/v2/lib/jwt"
)

// gateConfig is the subset of a page config the gate tests exercise.
type gateConfig struct {
	Viewer  *jwt.Identity
	Summary []SummaryItem
	Actions []Action
}

// renderGated renders the summary and actions blocks; each gates itself against
// the context's viewer.
func renderGated(t *testing.T, ctx context.Context, cfg gateConfig) string {
	t.Helper()

	var buf bytes.Buffer
	if err := Render(&buf, RenderConfig{Title: "t", Viewer: ViewerOf(ctx, cfg.Viewer), Blocks: []Block{
		SummaryBlock(cfg.Summary),
		ActionsBlock(cfg.Actions),
	}}); err != nil {
		t.Fatalf("execute template: %v", err)
	}

	return buf.String()
}

func gatedActions() []Action {
	return []Action{
		{Description: "Show", ButtonText: "Show", Endpoint: "/show"},
		{Description: "Cancel item", ButtonText: "Cancel", Endpoint: "/cancel", RequiredRole: jwt.RoleWrite},
		{Description: "Force paid", ButtonText: "Force", Endpoint: "/force", RequiredRole: jwt.RoleAdmin},
	}
}

func identity(audience string, roles ...string) *jwt.Identity {
	return &jwt.Identity{Subject: "staff-1", Audience: audience, Roles: roles}
}

func TestGate_ReadViewerDoesNotSeeWriteAction(t *testing.T) {
	out := renderGated(t, context.Background(), gateConfig{
		Viewer:  identity("order-service", jwt.RoleRead),
		Actions: gatedActions(),
	})

	if !strings.Contains(out, "/show") {
		t.Error("ungated action was hidden from a read viewer")
	}
	if strings.Contains(out, "/cancel") || strings.Contains(out, "/force") {
		t.Errorf("read viewer saw a gated action:\n%s", out)
	}
}

func TestGate_AdminViewerSeesWriteAndAdminActions(t *testing.T) {
	out := renderGated(t, context.Background(), gateConfig{
		Viewer:  identity("order-service", jwt.RoleAdmin),
		Actions: gatedActions(),
	})

	for _, endpoint := range []string{"/show", "/cancel", "/force"} {
		if !strings.Contains(out, endpoint) {
			t.Errorf("admin viewer did not see %s:\n%s", endpoint, out)
		}
	}
}

func TestGate_ViewerFromRequestContext(t *testing.T) {
	ctx := jwt.WithIdentity(context.Background(), *identity("order-service", jwt.RoleWrite))

	out := renderGated(t, ctx, gateConfig{Actions: gatedActions()})

	if !strings.Contains(out, "/cancel") {
		t.Errorf("write viewer from context did not see the write action:\n%s", out)
	}
	if strings.Contains(out, "/force") {
		t.Errorf("write viewer saw the admin action:\n%s", out)
	}
}

func TestGate_NoViewerHidesEverythingGated(t *testing.T) {
	out := renderGated(t, context.Background(), gateConfig{Actions: gatedActions()})

	if !strings.Contains(out, "/show") {
		t.Error("ungated action was hidden without a viewer")
	}
	if strings.Contains(out, "/cancel") || strings.Contains(out, "/force") {
		t.Errorf("gated action rendered without a viewer, must fail closed:\n%s", out)
	}
}

func TestGate_ForeignAudienceRoleIsDenied(t *testing.T) {
	actions := []Action{{Description: "Refund", ButtonText: "Refund", Endpoint: "/refund", RequiredRole: "payment-service:admin"}}

	own := renderGated(t, context.Background(), gateConfig{
		Viewer:  identity("payment-service", jwt.RoleAdmin),
		Actions: actions,
	})
	if !strings.Contains(own, "/refund") {
		t.Errorf("qualified role denied to a viewer of that audience:\n%s", own)
	}

	foreign := renderGated(t, context.Background(), gateConfig{
		Viewer:  identity("order-service", jwt.RoleAdmin),
		Actions: actions,
	})
	if strings.Contains(foreign, "/refund") {
		t.Errorf("admin on another audience saw a payment-service action:\n%s", foreign)
	}
}

func TestGate_DeniedSummaryLinkRendersPlainValue(t *testing.T) {
	summary := []SummaryItem{
		{Label: "Payment", Value: "pay_1", Link: "/payments/pay_1/history", RequiredRole: jwt.RoleWrite},
	}

	denied := renderGated(t, context.Background(), gateConfig{
		Viewer:  identity("order-service", jwt.RoleRead),
		Summary: summary,
	})
	if strings.Contains(denied, "<a href=") {
		t.Errorf("denied summary link rendered an anchor:\n%s", denied)
	}
	if !strings.Contains(denied, "pay_1") {
		t.Errorf("denied summary item lost its value:\n%s", denied)
	}

	allowed := renderGated(t, context.Background(), gateConfig{
		Viewer:  identity("order-service", jwt.RoleWrite),
		Summary: summary,
	})
	if !strings.Contains(allowed, `<a href="/payments/pay_1/history">pay_1</a>`) {
		t.Errorf("write viewer did not get the summary link:\n%s", allowed)
	}
}

// The Gate block hides a whole wrapped block from a viewer who lacks the role,
// and shows it whole to one who has it.
func TestGateBlock_HidesWholeBlockFromDeniedViewer(t *testing.T) {
	secret := HTMLBlock("<p>top secret</p>")

	render := func(viewer *jwt.Identity) string {
		var buf bytes.Buffer
		if err := Render(&buf, RenderConfig{Title: "t", Viewer: viewer, Blocks: []Block{
			Gate(jwt.RoleAdmin, secret),
		}}); err != nil {
			t.Fatalf("execute template: %v", err)
		}
		return buf.String()
	}

	if out := render(identity("order-service", jwt.RoleRead)); strings.Contains(out, "top secret") {
		t.Errorf("read viewer saw an admin-gated block:\n%s", out)
	}
	if out := render(nil); strings.Contains(out, "top secret") {
		t.Errorf("gated block rendered without a viewer, must fail closed:\n%s", out)
	}
	if out := render(identity("order-service", jwt.RoleAdmin)); !strings.Contains(out, "top secret") {
		t.Errorf("admin viewer did not see the gated block:\n%s", out)
	}
}
