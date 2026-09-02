package boff

import (
	"bytes"
	"strings"
	"testing"
)

// A SummaryItem with a Link renders as an anchor; without one it stays plain
// text. Exercises the template branch added for cross-service backoffice links.
func TestSummaryItemLinkRendersAnchor(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{SummaryBlock([]SummaryItem{
		{Label: "Payment ID", Value: "pay_1", Link: "/payments/backoffice/v1/payments/pay_1/history"},
		{Label: "Status", Value: "PAID"},
	})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, `<a href="/payments/backoffice/v1/payments/pay_1/history" class="text-decoration-none">pay_1</a>`) {
		t.Errorf("linked summary item did not render an anchor:\n%s", out)
	}
	// The unlinked row must not be wrapped in an anchor.
	if strings.Contains(out, `<a href="">PAID</a>`) || strings.Contains(out, `>PAID</a>`) {
		t.Errorf("unlinked summary item was wrapped in an anchor:\n%s", out)
	}
}

// A SummaryItem with a Tone renders its value with a severity pip; without one
// it stays plain text.
//
// Superseded assertion: this used to expect a filled `badge text-bg-*`. The
// console redesign replaced filled status badges with one pip vocabulary
// (.pip/.status) across every backoffice page, so the contract changed on
// purpose - a tone is still a tone, it is only drawn differently.
func TestSummaryItemToneRendersBadge(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{SummaryBlock([]SummaryItem{
		{Label: "Status", Value: "PAID", Tone: "success"},
		{Label: "Order ID", Value: "o1"},
	})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, `<span class="status status-ok"><span class="pip pip-ok"></span>PAID</span>`) {
		t.Errorf("toned summary item did not render a status pip:\n%s", out)
	}
	if strings.Contains(out, "text-bg-") {
		t.Errorf("summary still renders a filled badge:\n%s", out)
	}
	if strings.Contains(out, `<span class="status"`) {
		t.Errorf("untoned summary item was wrapped in a status:\n%s", out)
	}
}

// A tone outside the severity vocabulary renders a neutral pip rather than
// guessing a severity.
func TestSummaryItemUnknownToneRendersNeutralPip(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{SummaryBlock([]SummaryItem{
		{Label: "Status", Value: "REFUNDED", Tone: "info"},
	})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}

	if out := buf.String(); !strings.Contains(out, `<span class="status"><span class="pip"></span>REFUNDED</span>`) {
		t.Errorf("unmapped tone did not render a neutral pip:\n%s", out)
	}
}

// A SummaryItem with JSON renders a collapsible payload, highlighted like a
// ledger record, instead of a plain value.
//
// Superseded assertion: this used to expect a <details>/<summary> pair. The
// redesign gives every payload on the page the same widget - a chip showing the
// field count plus a panel toggled through aria-expanded/hidden - so the
// contract is now that widget, not <details>.
func TestSummaryItemJSONRendersCollapsiblePayload(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{SummaryBlock([]SummaryItem{
		{Label: "Item 0", Value: "coins x1", JSON: "{\n  \"offerId\": \"o1\"\n}"},
	})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		`aria-controls="summary-row-0"`,
		`aria-expanded="false"`,
		`{ } 1`,
		`<pre class="payload payload-inline" id="summary-row-0" hidden>`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("JSON summary item is missing %s:\n%s", want, out)
		}
	}
	if !strings.Contains(out, `<span class="text-primary">"offerId":</span>`) {
		t.Errorf("JSON payload was not highlighted:\n%s", out)
	}
}

// The summary card is a fact grid: a small uppercase label above its value,
// amounts monospaced, a zero amount muted, an id shown in full and copyable.
func TestSummaryRendersFactGrid(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{SummaryBlock([]SummaryItem{
		{Label: "Shop", Value: "showmethemoney-pt"},
		{Label: "Total charged", Value: "2.50 EUR"},
		{Label: "Discount", Value: "0.00 EUR"},
		{Label: "Request ID", Value: "01M1ETRDSW0QHC9Y6GAVX23ZQW"},
	})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		`<dl class="fact-grid mb-0">`,
		`<dt class="lbl">Shop</dt>`,
		// A shop name is prose: not monospaced, not copyable.
		`showmethemoney-pt`,
		`class="text-break font-mono">`,
		`class="text-break font-mono zero">`,
		// An id is shown in full and is copyable.
		`data-copy="01M1ETRDSW0QHC9Y6GAVX23ZQW"`,
		`<span class="id-text">01M1ETRDSW0QHC9Y6GAVX23ZQW</span>`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("fact grid is missing %s:\n%s", want, out)
		}
	}

	if strings.Contains(out, "row-cols-md-2") {
		t.Errorf("summary still renders the old two-column list:\n%s", out)
	}
	// Prose is not turned into a copy button.
	if strings.Contains(out, `data-copy="showmethemoney-pt"`) {
		t.Errorf("a shop name was rendered as a copyable id:\n%s", out)
	}
}

// A linked id renders in full inside the anchor and gets its own copy control
// next to it, since the anchor itself navigates rather than copying.
func TestSummaryLinkedIdRendersInFullWithACopyControl(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{SummaryBlock([]SummaryItem{
		{Label: "Player", Value: "01KXJMFRY454B6BH7Y3TYNWEXR", Link: "/players/backoffice/player/01KXJMFRY454B6BH7Y3TYNWEXR"},
	})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, `>01KXJMFRY454B6BH7Y3TYNWEXR</a>`) {
		t.Errorf("linked id was not rendered in full:\n%s", out)
	}
	if !strings.Contains(out, `title="Copy 01KXJMFRY454B6BH7Y3TYNWEXR"`) || !strings.Contains(out, `class="id-copy"`) {
		t.Errorf("linked id has no copy control of its own:\n%s", out)
	}
}

// An item carrying a SummaryRow leaves the grid and renders as a full-width two
// line row: pip, title, subtitle, tags, state, and a meta line.
func TestSummaryRowRendersItemRow(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{SummaryBlock([]SummaryItem{
		{Label: "Shop", Value: "showmethemoney-pt"},
		{Label: "Item 0", JSON: `{"offerId":"o1"}`, Row: &SummaryRow{
			Tone:     "success",
			Title:    "M\u00e1quina de Dinheiro (1)",
			Subtitle: "\u00d71 \u00b7 2.50 EUR",
			Tags:     []string{"competition"},
			State:    "ACTIVE", StateTone: "success",
			Meta: []MetaPart{
				{Text: "item 0"},
				{Text: "fulfilled by draw-service 15:53:43"},
				{Text: "draw history", Link: "/draws/backoffice/draw/d1"},
			},
		}},
	})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		`<div class="item-row">`,
		`<span class="pip pip-ok"></span>`,
		`<span class="ev-name">M\u00e1quina de Dinheiro (1)</span>`,
		`competition</span>`,
		`ACTIVE</span>`,
		`<a href="/draws/backoffice/draw/d1">draw history</a>`,
		`aria-controls="summary-row-0"`,
		`<pre class="payload payload-inline" id="summary-row-0" hidden>`,
	} {
		want = strings.ReplaceAll(want, `\u00e1`, "\u00e1")
		if !strings.Contains(out, want) {
			t.Errorf("item row is missing %s:\n%s", want, out)
		}
	}

	// The row left the grid; only the plain fact is still a grid cell.
	if strings.Count(out, `<div class="fact mw0">`) != 1 {
		t.Errorf("item row was rendered as a grid fact:\n%s", out)
	}
}

// Two summary cards on one page must not give their payload panels the same id.
func TestSummaryCardsUseDistinctPayloadIds(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{
		SummaryCard("Account", []SummaryItem{{Label: "Raw", Value: "v", JSON: `{"a":1}`}}),
		SummaryCard("Personal Data", []SummaryItem{{Label: "Raw", Value: "v", JSON: `{"b":2}`}}),
	}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}

	out := buf.String()
	for _, want := range []string{`id="summary-account-row-0"`, `id="summary-personal-data-row-0"`} {
		if !strings.Contains(out, want) {
			t.Errorf("payload panel id %s missing:\n%s", want, out)
		}
	}
}

// An item carrying a JSON payload but no SummaryRow still leaves the grid: a
// payload rendered inside a 164px grid column is clipped, and an item with a
// payload describes a record rather than a single value. It keeps the label,
// the value stays as the caller wrote it, and the payload panel spans the card.
func TestSummaryJSONItemWithoutRowStillLeavesTheGrid(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{SummaryBlock([]SummaryItem{
		{Label: "Shop", Value: "showmethemoney-pt"},
		{Label: "Item 0", Value: "Maquina de Dinheiro x1 = 1.85 EUR [competition] - CANCELLED", JSON: `{"id":0,"status":"CANCELLED"}`},
	})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		`<div class="item-row">`,
		`<span class="lbl">Item 0</span>`,
		`<span class="item-text">Maquina de Dinheiro x1 = 1.85 EUR [competition] - CANCELLED</span>`,
		`<pre class="payload payload-inline" id="summary-row-0" hidden>`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("JSON item did not render as a full-width row, missing %s:\n%s", want, out)
		}
	}

	// Only the plain fact is left in the grid.
	if strings.Count(out, `<div class="fact mw0">`) != 1 {
		t.Errorf("JSON item was still rendered as a grid fact:\n%s", out)
	}
}

// Several items stack as one row each, in the order they were given, each with
// its own payload panel.
func TestSummaryStacksSeveralItemRows(t *testing.T) {
	item := func(name string, json string) SummaryItem {
		return SummaryItem{Label: name, JSON: json, Row: &SummaryRow{Tone: "success", Title: name, Subtitle: "x1"}}
	}

	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{SummaryBlock([]SummaryItem{
		{Label: "Shop", Value: "showmethemoney-pt"},
		item("Item 0", `{"a":1}`),
		item("Item 1", `{"b":2}`),
		item("Item 2", `{"c":3}`),
	})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	if got := strings.Count(out, `<div class="item-row">`); got != 3 {
		t.Errorf("expected 3 item rows, got %d:\n%s", got, out)
	}
	// Each row keeps its own payload id, so expanding one does not open another.
	for i, id := range []string{"summary-row-0", "summary-row-1", "summary-row-2"} {
		if !strings.Contains(out, `id="`+id+`"`) {
			t.Errorf("item %d has no payload panel of its own:\n%s", i, out)
		}
	}
	// Order is the order given.
	first, second, third := strings.Index(out, "Item 0"), strings.Index(out, "Item 1"), strings.Index(out, "Item 2")
	if first >= second || second >= third {
		t.Errorf("item rows are out of order:\n%s", out)
	}
}

// A copyable id carries the copy glyph, never wraps, and hands the full value
// to the clipboard - the shortened text is only what is shown.
func TestSummaryIdCarriesACopyControl(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{SummaryBlock([]SummaryItem{
		{Label: "Request ID", Value: "01M1ETRDSW0QHC9Y6GAVX23ZQW"},
	})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		`data-copy="01M1ETRDSW0QHC9Y6GAVX23ZQW"`,
		`aria-label="Copy 01M1ETRDSW0QHC9Y6GAVX23ZQW"`,
		`<span class="id-text">01M1ETRDSW0QHC9Y6GAVX23ZQW</span>`,
		`class="id-copy"`,
		// The glyph is decorative: the button already has an accessible name.
		`aria-hidden="true"`,
		// A full id is wider than a fact column, so it may break anywhere.
		".id-text{overflow-wrap:anywhere",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("copyable id is missing %s:\n%s", want, out)
		}
	}
}

// A label is rendered as the caller wrote it - no truncation, no title
// attribute of its own.
func TestSummaryLabelIsRenderedVerbatim(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{SummaryBlock([]SummaryItem{
		{Label: "Draw", Value: "01M1DTGTM47SC9XFPEBAJRRD65", Link: "/draws/d1"},
		{Label: "Payment method", Value: "card"},
	})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		`<dt class="lbl">Draw</dt>`,
		`<dt class="lbl">Payment method</dt>`,
		// The id itself is the linked, copyable value.
		`>01M1DTGTM47SC9XFPEBAJRRD65</a>`,
		`title="Copy 01M1DTGTM47SC9XFPEBAJRRD65"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("summary is missing %s:\n%s", want, out)
		}
	}
}
