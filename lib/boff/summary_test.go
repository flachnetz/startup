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

// A SummaryItem with a Tone renders its value as a bootstrap badge; without one
// it stays plain text.
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

	if !strings.Contains(out, `<span class="badge text-bg-success">PAID</span>`) {
		t.Errorf("toned summary item did not render a badge:\n%s", out)
	}
	if strings.Contains(out, `>o1</span>`) {
		t.Errorf("untoned summary item was wrapped in a badge:\n%s", out)
	}
}

// A SummaryItem with JSON renders a collapsible payload, highlighted like a
// ledger record, instead of a plain value.
func TestSummaryItemJSONRendersCollapsiblePayload(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{SummaryBlock([]SummaryItem{
		{Label: "Item 0", Value: "coins x1", JSON: "{\n  \"offerId\": \"o1\"\n}"},
	})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, "<details>") || !strings.Contains(out, "<summary>coins x1</summary>") {
		t.Errorf("JSON summary item did not render a collapsible row:\n%s", out)
	}
	if !strings.Contains(out, `<span class="text-primary">"offerId":</span>`) {
		t.Errorf("JSON payload was not highlighted:\n%s", out)
	}
}
