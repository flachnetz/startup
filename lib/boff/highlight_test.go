package boff

import (
	"strings"
	"testing"
)

// The payload is coloured server-side: a fragment loses the <head>, so a
// client-side highlighter would never run when the page is embedded.
func TestHighlightJSONColoursTokensAndEscapes(t *testing.T) {
	out := HighlightJSON(`{
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
