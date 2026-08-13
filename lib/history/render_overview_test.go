package history

import (
	"bytes"
	"strings"
	"testing"
)

// The filter form only exists when the caller passes fields, and applied values
// come back into the inputs so a filtered list stays filtered after a reload.
func TestRenderOverviewFilterFormIsStickyAndOptional(t *testing.T) {
	var withFilters bytes.Buffer
	err := RenderOverviewWithConfig(&withFilters, OverviewConfig{
		Title:   "Recent Orders",
		Headers: []string{"Created", "Order ID"},
		Filters: []OverviewFilter{{Label: "Player ID", Name: "player_id", Value: "player-1"}},
		Rows:    []OverviewRow{{Link: "/orders/backoffice/v1/orders/o1/history", Cells: []string{"now", "o1"}}},
	})
	if err != nil {
		t.Fatalf("render overview: %v", err)
	}
	out := withFilters.String()

	if !strings.Contains(out, `method="GET"`) {
		t.Errorf("filter form is not a GET form:\n%s", out)
	}
	if !strings.Contains(out, `name="player_id"`) || !strings.Contains(out, `value="player-1"`) {
		t.Errorf("applied filter value was not echoed back:\n%s", out)
	}

	var noFilters bytes.Buffer
	if err := RenderOverview(&noFilters, "Recent Orders", []string{"Created"}, nil); err != nil {
		t.Fatalf("render overview: %v", err)
	}
	if strings.Contains(noFilters.String(), `method="GET"`) {
		t.Errorf("form rendered without filter fields:\n%s", noFilters.String())
	}
}

// A row navigates with anchors, not with an inline handler: the page must work as
// an embedded fragment without anybody rewriting its markup.
func TestRenderOverviewRowsNavigateWithPlainAnchors(t *testing.T) {
	var buf bytes.Buffer
	err := RenderOverview(&buf, "t", []string{"ID", "Player"},
		[]OverviewRow{{Link: "/orders/backoffice/x/history", Cells: []string{"o1", "player-1"}}})
	if err != nil {
		t.Fatalf("render overview: %v", err)
	}
	out := buf.String()

	// Every cell is clickable, so the whole row behaves like a link.
	if strings.Count(out, `href="/orders/backoffice/x/history"`) != 2 {
		t.Errorf("expected one anchor per cell:\n%s", out)
	}
	if strings.Contains(out, "onclick") || strings.Contains(out, "data-href") {
		t.Errorf("overview still needs JavaScript or rewriting:\n%s", out)
	}
}

// A row without a link renders plain cells rather than empty anchors.
func TestRenderOverviewRowWithoutLinkHasNoAnchor(t *testing.T) {
	var buf bytes.Buffer
	if err := RenderOverview(&buf, "t", []string{"ID"}, []OverviewRow{{Cells: []string{"o1"}}}); err != nil {
		t.Fatalf("render overview: %v", err)
	}
	if strings.Contains(buf.String(), "<a ") {
		t.Errorf("unlinked row rendered an anchor:\n%s", buf.String())
	}
}

// The scope the rows were loaded with is shown, so a viewer does not read an
// operator-scoped list as the whole world.
func TestRenderOverviewShowsScopeNote(t *testing.T) {
	var buf bytes.Buffer
	err := RenderOverviewWithConfig(&buf, OverviewConfig{
		Title: "t", Headers: []string{"ID"}, ScopeNote: "operator bmh-audio-pt, all shops",
	})
	if err != nil {
		t.Fatalf("render overview: %v", err)
	}
	if !strings.Contains(buf.String(), "operator bmh-audio-pt, all shops") {
		t.Errorf("scope note missing:\n%s", buf.String())
	}
}

// An action is a plain form POST. No fetch, no inline handler, nothing for the
// embedding shell to repair.
func TestRenderPageActionIsAPlainForm(t *testing.T) {
	var buf bytes.Buffer
	err := pageTemplate.Execute(&buf, PageModel{
		Title:   "t",
		GroupId: "order:1",
		Actions: []Action{{
			Description: "Cancel order", ButtonText: "Cancel",
			Endpoint: "/orders/backoffice/v1/orders/o1/cancel",
		}},
	})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, `<form method="POST" action="/orders/backoffice/v1/orders/o1/cancel">`) {
		t.Errorf("action is not a plain form POST:\n%s", out)
	}
	// The body is what gets embedded as a fragment (the <head> is dropped), so it
	// is the body that must be script-free.
	_, body, _ := strings.Cut(out, "<body>")
	if strings.Contains(body, "onclick") || strings.Contains(body, "data-endpoint") || strings.Contains(body, "<script") {
		t.Errorf("action page still needs JavaScript of its own:\n%s", body)
	}
}

// A confirmation is a Bootstrap modal wrapping the same form: the dialog is
// driven by Bootstrap's own JS, which the shell already loads.
func TestRenderPageConfirmationWrapsTheFormInAModal(t *testing.T) {
	var buf bytes.Buffer
	err := pageTemplate.Execute(&buf, PageModel{
		Title:   "t",
		GroupId: "order:1",
		Actions: []Action{{
			Description: "Cancel order", ButtonText: "Cancel",
			Endpoint: "/orders/backoffice/v1/orders/o1/cancel", ConfirmMessage: "Cancel order o1?",
		}},
	})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		`data-bs-toggle="modal"`,
		`data-bs-target="#action-confirm-0"`,
		`id="action-confirm-0"`,
		`Cancel order o1?`,
		`<form class="modal-content" method="POST" action="/orders/backoffice/v1/orders/o1/cancel">`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("confirmation modal missing %s:\n%s", want, out)
		}
	}
}
