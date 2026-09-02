package boff

import (
	"bytes"
	"strings"
	"testing"
)

// The fragment contract: backoffice keeps only #body and drops the <head>, so
// anything the page needs to work must sit inside #body. It embeds the markup
// server-side into its own shell, so a <script> there is parsed and runs - the
// console script therefore lives in the body, not the head.
func TestConsoleScriptLivesInTheEmbeddedBody(t *testing.T) {
	var buf bytes.Buffer

	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{
		ActionsBlock([]Action{{
			Description: "Cancel order", ButtonText: "Cancel",
			Endpoint: "/orders/backoffice/v1/orders/o1/cancel",
		}}),
	}})
	if err != nil {
		t.Fatalf("render shell: %v", err)
	}

	out := buf.String()

	head, body, found := strings.Cut(out, `id="body"`)
	if !found {
		t.Fatalf("shell has no #body:\n%s", out)
	}

	if strings.Contains(head, "btn-payload") {
		t.Errorf("console script is in the head, which the embedder drops:\n%s", head)
	}

	if !strings.Contains(body, "btn-payload") {
		t.Errorf("console script is not inside #body:\n%s", body)
	}

	// Per-element handlers would be lost for markup the shell re-renders; the
	// script wires itself on document instead.
	if strings.Contains(body, "onclick") {
		t.Errorf("fragment body carries inline event handlers:\n%s", body)
	}
}

// Copying must work where these pages are actually served: backoffice is
// reached over plain http, where navigator.clipboard does not exist. The script
// therefore carries the execCommand fallback and confirms only a copy that
// succeeded - the confirmation is CSS, so the icon-only control next to a linked
// id confirms like any other.
func TestConsoleScriptCopiesWithoutTheClipboardAPI(t *testing.T) {
	var buf bytes.Buffer

	if err := Render(&buf, RenderConfig{Title: "t"}); err != nil {
		t.Fatalf("render shell: %v", err)
	}

	out := buf.String()

	for _, want := range []string{
		`document.execCommand("copy")`,
		"function legacyCopy(text)",
		`id.classList.add(ok ? "copied" : "copy-failed")`,
		`.id.copied::after{content:"\2713"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("console script is missing %s:\n%s", want, out)
		}
	}

	// The old path claimed success without checking, and swapped the id text -
	// which an icon-only control has none of.
	if strings.Contains(out, `text.textContent = "copied`) {
		t.Errorf("copy still confirms by swapping the id text:\n%s", out)
	}
}
