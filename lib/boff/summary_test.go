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
	err := Render(&buf, Shell, RenderConfig{Title: "t", Subtitle: "order:1", Blocks: []Block{SummaryBlock([]SummaryItem{
		{Label: "Payment ID", Value: "pay_1", Link: "/payments/backoffice/v1/payments/pay_1/history"},
		{Label: "Status", Value: "PAID"},
	})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, `<a href="/payments/backoffice/v1/payments/pay_1/history">pay_1</a>`) {
		t.Errorf("linked summary item did not render an anchor:\n%s", out)
	}
	// The unlinked row must not be wrapped in an anchor.
	if strings.Contains(out, `<a href="">PAID</a>`) || strings.Contains(out, `>PAID</a>`) {
		t.Errorf("unlinked summary item was wrapped in an anchor:\n%s", out)
	}
}
