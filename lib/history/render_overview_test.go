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

// A dropdown filter renders a select with the applied value preselected, a date
// filter uses the browser's own date input, and a hidden filter keeps its value
// without offering a control.
func TestRenderOverviewFilterKinds(t *testing.T) {
	var buf bytes.Buffer
	err := RenderOverviewWithConfig(&buf, OverviewConfig{
		Title:   "t",
		Headers: []string{"ID"},
		Filters: []OverviewFilter{
			{Label: "Status", Name: "status", Value: "PAID", Options: []FilterOption{
				{Value: "", Label: "any"}, {Value: "PAID", Label: "PAID"}, {Value: "REFUNDED", Label: "REFUNDED"},
			}},
			{Label: "From", Name: "from", Value: "2026-08-01", Type: "date"},
			{Label: "Player ID", Name: "player_id", Value: "player-1", Hidden: true},
		},
	})
	if err != nil {
		t.Fatalf("render overview: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		`<select class="form-select form-select-sm" id="filter-status" name="status">`,
		`<option value="PAID" selected>PAID</option>`,
		`type="date" id="filter-from"`,
		`<input type="hidden" name="player_id" value="player-1">`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, `for="filter-player_id"`) {
		t.Errorf("hidden filter rendered a control:\n%s", out)
	}
}

// Paging carries every applied filter, including hidden ones, so page 2 of a
// filtered list is not page 2 of everything.
func TestRenderOverviewPaginationKeepsFilters(t *testing.T) {
	filters := []OverviewFilter{
		{Name: "status", Value: "PAID"},
		{Name: "player_id", Value: "player-1", Hidden: true},
		{Name: "order_id", Value: ""},
	}

	var buf bytes.Buffer
	err := RenderOverviewWithConfig(&buf, OverviewConfig{
		Title: "t", Headers: []string{"ID"}, Filters: filters, Page: 2, HasNext: true,
	})
	if err != nil {
		t.Fatalf("render overview: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, `href="?player_id=player-1&amp;status=PAID"`) {
		t.Errorf("previous link lost filters or page:\n%s", out)
	}
	if !strings.Contains(out, `href="?page=3&amp;player_id=player-1&amp;status=PAID"`) {
		t.Errorf("next link lost filters or page:\n%s", out)
	}
	if strings.Contains(out, "order_id=") {
		t.Errorf("empty filter leaked into the page links:\n%s", out)
	}

	// First page and no further rows: no pager links at all.
	var single bytes.Buffer
	if err := RenderOverviewWithConfig(&single, OverviewConfig{Title: "t", Headers: []string{"ID"}}); err != nil {
		t.Fatalf("render overview: %v", err)
	}
	if strings.Contains(single.String(), "Previous") {
		t.Errorf("pager rendered for a single page:\n%s", single.String())
	}
}

// The payload is coloured server-side: a fragment loses the <head>, so a
// client-side highlighter would never run when the page is embedded.
func TestHighlightJSONColoursTokensAndEscapes(t *testing.T) {
	out := highlightJSON(`{
  "name": "<b>x</b>",
  "count": -12.5,
  "ok": true,
  "gone": null
}`)

	for _, want := range []string{
		`<span class="text-primary">"name":</span>`,
		`<span class="text-success">"&lt;b&gt;x&lt;/b&gt;"</span>`,
		`<span class="text-danger">-12.5</span>`,
		`<span class="text-body-secondary fst-italic">true</span>`,
		`<span class="text-body-secondary fst-italic">null</span>`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, "<b>x</b>") {
		t.Errorf("payload markup was not escaped:\n%s", out)
	}
}
