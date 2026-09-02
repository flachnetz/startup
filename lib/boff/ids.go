package boff

import (
	"encoding/json"
	"html/template"
	"regexp"
	"strings"
)

// idPattern matches a value that is an identifier rather than prose: one
// unbroken run of id characters, at least as long as the shortest id shape in
// use (a 26 character ULID, a 32 character trace id, a 36 character UUID). An
// optional "type:" prefix is part of the match (a GroupId, a prefixed request
// id).
//
// The length floor is what keeps a slug out: "showmethemoney-pt" is a shop
// name an operator reads, not an id they copy.
var idPattern = regexp.MustCompile(`^(?:[A-Za-z][A-Za-z0-9_-]*:)?[A-Za-z0-9_-]{20,}$`)

// IsId reports whether value looks like an identifier - used to decide whether a
// displayed value renders monospaced and copyable rather than as prose. An id is
// always shown in full: an operator compares it character by character against a
// support ticket, and a truncated ULID or UUID cannot be compared at all.
// Deliberately conservative: anything with whitespace or punctuation, and
// anything short enough to be a human-readable name, is prose.
func IsId(value string) bool {
	return idPattern.MatchString(value)
}

// amountPattern matches a rendered money amount ("2.50 EUR", "-12,34 EUR"), the
// values that must line up in a column and therefore render monospaced with
// tabular figures.
var amountPattern = regexp.MustCompile(`^-?\d+[.,]\d{2}(?: [A-Za-z]{3})?$`)

// IsAmount reports whether value is a rendered money amount.
func IsAmount(value string) bool {
	return amountPattern.MatchString(value)
}

// zeroAmountPattern matches an amount of zero in any currency.
var zeroAmountPattern = regexp.MustCompile(`^0[.,]00(?: [A-Za-z]{3})?$`)

// IsZeroAmount reports whether value is a zero amount, which the page mutes: a
// zero discount is information an operator scans past, not one they read.
func IsZeroAmount(value string) bool {
	return zeroAmountPattern.MatchString(value)
}

// JSONFieldCount is how many fields a payload carries at its top level, the
// number the payload chip shows ("{ } 5"). An array counts its elements. A
// payload that is neither (a bare string, invalid JSON, or empty) counts zero,
// which is what suppresses the chip.
func JSONFieldCount(payload string) int {
	if strings.TrimSpace(payload) == "" {
		return 0
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(payload), &fields); err == nil {
		return len(fields)
	}

	var elements []json.RawMessage
	if err := json.Unmarshal([]byte(payload), &elements); err == nil {
		return len(elements)
	}

	return 0
}

// payloadModel is one collapsible JSON payload: the chip that toggles it and
// the panel it toggles share this model, so their id and field count cannot
// drift apart. Inline marks a payload rendered inside a card body rather than
// under a ledger row, where it needs no rail-width indent.
type payloadModel struct {
	Id     string
	Fields int
	HTML   template.HTML
	Inline bool
}

// newPayloadModel is the "payload" template func, so a template can build the
// shared model for a chip and its panel in one call.
func newPayloadModel(id string, fields int, html template.HTML, inline bool) payloadModel {
	return payloadModel{Id: id, Fields: fields, HTML: html, Inline: inline}
}

// slugPattern matches everything that must not appear in an HTML id.
var slugPattern = regexp.MustCompile(`[^a-z0-9]+`)

// slug turns a title into an id fragment, so two cards on one page give their
// collapsible panels distinct ids.
func slug(title string) string {
	return strings.Trim(slugPattern.ReplaceAllString(strings.ToLower(title), "-"), "-")
}

// pipTone maps a Bootstrap tone name onto the page's severity vocabulary: the
// pip suffix used by .pip-* and .status-*. An unmapped tone (or none) is
// neutral - grey, informative, no claim about severity.
// It accepts the severity names themselves too ("ok", "warn", "err"), so a
// caller that already thinks in severities - a ledger classifying its records -
// does not have to translate them into Bootstrap tone names first.
func pipTone(tone string) string {
	switch tone {
	case "success", "ok":
		return "ok"
	case "danger", "error", "err":
		return "err"
	case "warning", "warn":
		return "warn"
	default:
		return ""
	}
}

// pipClass is the full class attribute of a severity pip, e.g. "pip pip-ok".
func pipClass(tone string) string {
	if suffix := pipTone(tone); suffix != "" {
		return "pip pip-" + suffix
	}

	return "pip"
}

// statusClass is the full class attribute of a pip-plus-label status, e.g.
// "status status-ok".
func statusClass(tone string) string {
	if suffix := pipTone(tone); suffix != "" {
		return "status status-" + suffix
	}

	return "status"
}
