package boff

import (
	"bytes"
	"strings"
	"testing"
)

// An action is a plain form POST. No fetch, no inline handler, nothing for the
// embedding shell to repair.
//
// The assertion is scoped to the block, not the page: the shell now ships one
// shared console script (payload toggles, copy-to-clipboard) inside #body. That
// script belongs to the page chrome; an action must still work without any
// script of its own, which is what this checks.
func TestActionsBlockActionIsAPlainForm(t *testing.T) {
	html, err := ActionsBlock([]Action{{
		Description: "Cancel order", ButtonText: "Cancel",
		Endpoint: "/orders/backoffice/v1/orders/o1/cancel",
	}}).Render(RenderContext{})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := string(html)

	if !strings.Contains(out, `<form method="POST" action="/orders/backoffice/v1/orders/o1/cancel">`) {
		t.Errorf("action is not a plain form POST:\n%s", out)
	}
	if strings.Contains(out, "onclick") || strings.Contains(out, "data-endpoint") || strings.Contains(out, "<script") {
		t.Errorf("action block still needs JavaScript of its own:\n%s", out)
	}
}

// A confirmation is a Bootstrap modal wrapping the same form: the dialog is
// driven by Bootstrap's own JS, which the shell already loads.
func TestActionsBlockConfirmationWrapsTheFormInAModal(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{ActionsBlock([]Action{{
		Description: "Cancel order", ButtonText: "Cancel",
		Endpoint: "/orders/backoffice/v1/orders/o1/cancel", ConfirmMessage: "Cancel order o1?",
	}})}})
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
		// Confirm defaults to "OK", not the action label: two "Cancel" buttons in
		// one footer read as a choice between cancelling twice.
		`type="submit">OK</button>`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("confirmation modal missing %s:\n%s", want, out)
		}
	}
}

func TestActionsBlockConfirmTextOverridesTheConfirmButtonLabel(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{ActionsBlock([]Action{{
		Description: "Cancel order", ButtonText: "Cancel",
		Endpoint: "/cancel", ConfirmMessage: "Cancel order o1?", ConfirmText: "Yes, cancel",
	}})}})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	if out := buf.String(); !strings.Contains(out, `type="submit">Yes, cancel</button>`) {
		t.Errorf("ConfirmText not used:\n%s", out)
	}
}
