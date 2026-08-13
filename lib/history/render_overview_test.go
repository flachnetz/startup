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
		Rows:    []OverviewRow{{Link: "/internal/backoffice/v1/orders/o1/history", Cells: []string{"now", "o1"}}},
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

// Rows keep the inline location assignment: backoffice rewrites it into a
// CSP-safe data-href when it extracts the fragment, so removing it here would
// make every overview row dead.
func TestRenderOverviewRowsStayClickable(t *testing.T) {
	var buf bytes.Buffer
	err := RenderOverview(&buf, "t", []string{"ID"}, []OverviewRow{{Link: "/x/history", Cells: []string{"o1"}}})
	if err != nil {
		t.Fatalf("render overview: %v", err)
	}
	if !strings.Contains(buf.String(), `onclick="location=`) {
		t.Errorf("row lost its navigation:\n%s", buf.String())
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

// Action buttons must carry their endpoint as data attributes and must NOT carry
// an inline onclick: the fragment runs under a CSP that forbids inline handlers,
// so behaviour lives in backoffice's global JS.
func TestRenderPageActionButtonsAreCSPSafe(t *testing.T) {
	var buf bytes.Buffer
	err := pageTemplate.Execute(&buf, PageModel{
		Title:   "t",
		GroupId: "order:1",
		Actions: []Action{{
			Description: "Cancel order", ButtonText: "Cancel", Method: "POST",
			Endpoint: "/internal/backoffice/v1/orders/o1/cancel", ConfirmMessage: "Cancel order o1?",
		}},
	})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		`data-method="POST"`,
		`data-endpoint="/internal/backoffice/v1/orders/o1/cancel"`,
		`data-confirm="Cancel order o1?"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("action button missing %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, "onclick") {
		t.Errorf("action page still uses an inline onclick handler:\n%s", out)
	}
	if strings.Contains(out, "<script") {
		t.Errorf("action page still ships an inline script:\n%s", out)
	}
}
