package boff

import (
	"bytes"
	"strings"
	"testing"
)

// An action is a plain form POST. No fetch, no inline handler, nothing for the
// embedding shell to repair.
func TestActionsBlockActionIsAPlainForm(t *testing.T) {
	var buf bytes.Buffer
	err := Render(&buf, RenderConfig{Title: "t", Blocks: []Block{ActionsBlock([]Action{{
		Description: "Cancel order", ButtonText: "Cancel",
		Endpoint: "/orders/backoffice/v1/orders/o1/cancel",
	}})}})
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
