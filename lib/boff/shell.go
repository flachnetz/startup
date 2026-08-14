package boff

import (
	"embed"
	"fmt"
	"html/template"
	"io"

	"github.com/flachnetz/startup/v2/lib/jwt"
)

//go:embed templates/shell.gohtml templates/blocks.gohtml
var shellFS embed.FS

// Shell is the parsed page shell, a full HTML document that renders a title, an
// optional subtitle and error line, and then concatenates the page's blocks. It
// also carries the built-in block sub-templates (block/summary, block/actions,
// overview/*), so a page package that clones Shell has them available. A page
// package parses its own extra block sub-templates into a clone of this shell,
// then renders with Render.
//
// The shell is exposed as a template rather than a render function so callers
// keep control of Funcs and of parsing their own block definitions alongside it.
var Shell = template.Must(template.New("boff").ParseFS(shellFS, "templates/*.gohtml"))

// RenderConfig is the caller-supplied config for the shell. Subtitle and
// ErrorMessage are optional. Blocks are rendered into the shell in order.
//
// Viewer is the identity the page's gated blocks filter themselves against. It
// is put into the RenderContext each block renders with. Leave it nil to fail
// closed - every gated element is then hidden. Use ViewerOf to derive it from
// the request context.
type RenderConfig struct {
	Title        string
	Subtitle     string
	ErrorMessage string
	Viewer       *jwt.Identity
	Blocks       []Block
}

// shellData is what the shell template actually executes against: the config
// plus the rendered blocks concatenated into one HTML fragment.
type shellData struct {
	RenderConfig
	Blocks template.HTML
}

// Render renders cfg.Blocks and writes the full page shell to w. Each block
// renders with a RenderContext carrying cfg.Viewer, so blocks gate themselves.
// tpl provides the "boff/shell" definition (parse your block templates into a
// clone of Shell); blocks resolve their own sub-templates via the template they
// were built with.
func Render(w io.Writer, tpl *template.Template, cfg RenderConfig) error {
	rc := RenderContext{Viewer: cfg.Viewer}

	blocks, err := Blocks(cfg.Blocks...).Render(rc)
	if err != nil {
		return err
	}

	if err := tpl.ExecuteTemplate(w, "boff/shell", shellData{RenderConfig: cfg, Blocks: blocks}); err != nil {
		return fmt.Errorf("render shell: %w", err)
	}

	return nil
}
