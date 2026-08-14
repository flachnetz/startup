package boff

import (
	"bytes"
	"strings"
	"testing"
)

// A card wraps a title, subtitle and a body block; the body renders inside the
// card body.
func TestCardBlockWrapsBody(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{CardBlock{
		Title:    "Balance",
		Subtitle: "current",
		Body:     HTMLBlock(`<p class="card-text">42.00 EUR</p>`),
	}}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		`<div class="card mb-4">`,
		`<h5 class="card-title">Balance</h5>`,
		`<h6 class="card-subtitle mb-2 text-body-secondary">current</h6>`,
		`<p class="card-text">42.00 EUR</p>`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("card missing %s:\n%s", want, out)
		}
	}
}

// A card without a subtitle omits the subtitle element.
func TestCardBlockOmitsEmptySubtitle(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{CardBlock{
		Title: "Just a title",
		Body:  HTMLBlock("body"),
	}}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}

	if strings.Contains(buf.String(), "card-subtitle") {
		t.Errorf("empty subtitle still rendered:\n%s", buf.String())
	}
}

// Raised adds the shadow-sm utility; a plain card does not.
func TestCardBlockRaisedAddsShadow(t *testing.T) {
	render := func(raised bool) string {
		var buf bytes.Buffer
		if err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{CardBlock{
			Title: "x", Body: HTMLBlock("body"), Raised: raised,
		}}}); err != nil {
			t.Fatalf("execute template: %v", err)
		}
		return buf.String()
	}

	if !strings.Contains(render(true), "shadow-sm") {
		t.Error("raised card missing shadow-sm")
	}
	if strings.Contains(render(false), "shadow-sm") {
		t.Error("plain card should not have shadow-sm")
	}
}
