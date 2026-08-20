package boff

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

// A cell can carry its own link, so a table with several targets does not have to
// make the whole row navigate to one of them. A cell link is a visible link.
// A cell with a tone renders as a badge; a row link still wraps it, and a cell
// link wins over the tone.
func TestRenderOverviewTonesIndividualCells(t *testing.T) {
	var buf bytes.Buffer
	err := RenderOverview(&buf, "t", []string{"Status", "Order ID"},
		[]OverviewRow{{
			Cells:     []string{"PAID", "o1"},
			CellLinks: []string{"", "/orders/o1/history"},
			CellTones: []string{"success", "danger"},
		}})
	if err != nil {
		t.Fatalf("render overview: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, `<span class="badge text-bg-success">PAID</span>`) {
		t.Errorf("toned cell did not render a badge:\n%s", out)
	}
	if !strings.Contains(out, `<a href="/orders/o1/history">o1</a>`) || strings.Contains(out, "text-bg-danger") {
		t.Errorf("cell link did not win over the tone:\n%s", out)
	}
}

func TestRenderOverviewLinksIndividualCells(t *testing.T) {
	var buf bytes.Buffer
	err := RenderOverview(&buf, "t", []string{"Order ID", "Player", "Payment ID"},
		[]OverviewRow{{
			Cells:     []string{"o1", "player-1", "p1"},
			CellLinks: []string{"/orders/backoffice/v1/orders/o1/history", "", "/payments/backoffice/v1/payments/p1/history"},
		}})
	if err != nil {
		t.Fatalf("render overview: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		`<a href="/orders/backoffice/v1/orders/o1/history">o1</a>`,
		`<a href="/payments/backoffice/v1/payments/p1/history">p1</a>`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s:\n%s", want, out)
		}
	}
	// The unlinked cell stays text, and nothing makes the row itself clickable.
	if strings.Contains(out, `>player-1</a>`) || strings.Contains(out, "text-reset") {
		t.Errorf("row was made clickable:\n%s", out)
	}
}

// The pager sits above and below the table and can jump to the first page. The
// last page is only offered when the caller counted the result set.
func TestRenderOverviewPagerJumpsToFirstAndLast(t *testing.T) {
	render := func(cfg OverviewConfig) string {
		var buf bytes.Buffer
		if err := RenderOverviewWithConfig(&buf, cfg); err != nil {
			t.Fatalf("render overview: %v", err)
		}

		return buf.String()
	}

	filters := []OverviewFilter{{Name: "status", Value: "PAID"}}
	out := render(OverviewConfig{
		Title: "t", Headers: []string{"ID"}, Filters: filters, Page: 7, HasNext: true, TotalPages: 9,
	})

	if strings.Count(out, "<nav") != 2 {
		t.Errorf("pager is not rendered above and below the table:\n%s", out)
	}
	if !strings.Contains(out, `href="?status=PAID"`) {
		t.Errorf("first-page jump missing or lost its filters:\n%s", out)
	}
	if !strings.Contains(out, `href="?page=9&amp;status=PAID"`) {
		t.Errorf("last-page jump missing:\n%s", out)
	}
	if !strings.Contains(out, "Page 7 of 9") {
		t.Errorf("page position missing:\n%s", out)
	}

	// Without a known total there is no last page to jump to.
	unknown := render(OverviewConfig{Title: "t", Headers: []string{"ID"}, Page: 2, HasNext: true})
	if strings.Contains(unknown, "Last") {
		t.Errorf("last-page jump offered without a total:\n%s", unknown)
	}
}
