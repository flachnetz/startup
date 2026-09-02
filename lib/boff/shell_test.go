package boff

import (
	"bytes"
	"strings"
	"testing"
)

// The fragment contract: backoffice embeds the page body verbatim and drops the
// <head>, so everything below <body> must work with no JavaScript of its own.
// The console script therefore sits in the <head> - an innerHTML-inserted
// <script> would never execute anyway.
func TestRenderKeepsTheBodyFreeOfScripts(t *testing.T) {
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

	head, body, found := strings.Cut(out, "<body>")
	if !found {
		t.Fatalf("shell has no body:\n%s", out)
	}

	if !strings.Contains(head, "btn-payload") {
		t.Errorf("console script is not in the head:\n%s", head)
	}

	if strings.Contains(body, "<script") || strings.Contains(body, "onclick") {
		t.Errorf("fragment body carries JavaScript of its own:\n%s", body)
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
