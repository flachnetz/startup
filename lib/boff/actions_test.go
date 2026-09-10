package boff

import (
	"bytes"
	"strings"
	"testing"
)

// An action is a plain form POST. No fetch, no inline handler, nothing for the
// embedding shell to repair.
//
// The assertion is scoped to the block, not the page: the shell ships one shared
// console script (payload toggles, copy-to-clipboard) at the end of #body. That
// script is page chrome with delegated listeners; an action must still work as a
// plain form POST with no script of its own, which is what this checks.
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

// An action whose server-side rule refuses an empty reason renders the field
// that collects one. Without it the operator learns of the rule from an error
// page, which is what the parked-message exclusion did.
func TestActionsBlockPromptRendersTheFieldInsideTheForm(t *testing.T) {
	html, err := ActionsBlock([]Action{{
		Description: "Exclude", ButtonText: "Exclude",
		Endpoint:       "/finance/backoffice/parked/3/disposition?disposition=EXCLUDED",
		ConfirmMessage: "Exclude this message permanently?",
		Prompt:         &ActionPrompt{Name: "reason", Label: "Why is it excluded?", Required: true},
	}}).Render(RenderContext{})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := string(html)

	for _, want := range []string{
		`<form class="modal-content" method="POST"`,
		`name="reason"`,
		` required`,
		`Why is it excluded?`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("prompt missing %s:\n%s", want, out)
		}
	}
}

// A prompt alone opens the dialog: the field cannot be filled in by a form that
// submits on click.
func TestActionsBlockPromptImpliesTheDialog(t *testing.T) {
	html, err := ActionsBlock([]Action{{
		ButtonText: "Exclude", Endpoint: "/x",
		Prompt: &ActionPrompt{Name: "reason", Label: "Reason", Required: true},
	}}).Render(RenderContext{})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}

	if !strings.Contains(string(html), `data-bs-toggle="modal"`) {
		t.Errorf("a prompt without a confirm message did not open a dialog:\n%s", html)
	}
}

// Row actions get dialog ids of their own, or the second row's button opens the
// first row's modal.
func TestTableBlockRowActionsGetRowScopedDialogIds(t *testing.T) {
	rows := []OverviewRow{
		{Cells: []string{"a", ""}, Actions: []Action{{
			ButtonText: "Mark replayed", Endpoint: "/parked/1/disposition", ConfirmMessage: "Replayed?",
		}}},
		{Cells: []string{"b", ""}, Actions: []Action{{
			ButtonText: "Mark replayed", Endpoint: "/parked/2/disposition", ConfirmMessage: "Replayed?",
		}}},
	}

	html, err := TableBlock([]string{"Thing", "Actions"}, rows).Render(RenderContext{})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := string(html)

	for _, want := range []string{`id="action-confirm-row0-0"`, `id="action-confirm-row1-0"`} {
		if !strings.Contains(out, want) {
			t.Errorf("row action dialog id missing %s:\n%s", want, out)
		}
	}
}

// The actions column keeps its cell for a viewer who may perform none of them,
// so the row does not shift left under the headers.
func TestTableBlockActionsColumnSurvivesGating(t *testing.T) {
	rows := []OverviewRow{{Cells: []string{"a"}, Actions: []Action{{
		ButtonText: "Mark replayed", Endpoint: "/parked/1/disposition", RequiredRole: RoleWrite,
	}}}}

	html, err := TableBlock([]string{"Thing", "Actions"}, rows).Render(RenderContext{})
	if err != nil {
		t.Fatalf("execute template: %v", err)
	}
	out := string(html)

	if strings.Contains(out, "Mark replayed") {
		t.Errorf("a viewer without the write role was offered the action:\n%s", out)
	}
	if got := strings.Count(out, "<td"); got != 2 {
		t.Errorf("row rendered %d cells, want 2 (the empty actions cell must stay):\n%s", got, out)
	}
}
