package boff

import (
	"regexp"
	"strings"
)

// jsonToken matches one JSON string (optionally an object key, i.e. followed by
// a colon), literal or number in already-indented JSON.
var jsonToken = regexp.MustCompile(`"(?:\\.|[^"\\])*"\s*:?|\b(?:true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?`)

// payloadEscaper escapes exactly what is unsafe in element text. Quotes stay
// intact so the highlighter can still see JSON strings.
var payloadEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// HighlightJSON colourises an already-indented JSON payload with Bootstrap
// text-colour utility classes, escaping it first. Those classes come from the
// stylesheet the page already has - server-side because a client-side
// highlighter would be dropped together with the <head> when the page is
// embedded as a backoffice fragment. Shared by every block that shows a JSON
// payload verbatim (a ledger record, a summary row) so the rendering stays
// consistent across backoffice pages.
func HighlightJSON(payload string) string {
	escaped := payloadEscaper.Replace(payload)

	return jsonToken.ReplaceAllStringFunc(escaped, func(token string) string {
		var class string
		switch {
		case strings.HasSuffix(token, ":"):
			class = "text-primary"
		case strings.HasPrefix(token, `"`):
			class = "text-success"
		case token == "true" || token == "false" || token == "null":
			class = "text-body-secondary fst-italic"
		default:
			class = "text-danger"
		}

		return `<span class="` + class + `">` + token + `</span>`
	})
}
