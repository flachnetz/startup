package boff

import (
	"embed"
	"fmt"
	"html/template"
	"io"
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
type RenderConfig struct {
	Title        string
	Subtitle     string
	ErrorMessage string
	Blocks       []Block
}

// shellData is what the shell template actually executes against: the config
// plus the rendered blocks.
type shellData struct {
	RenderConfig
	Blocks []RenderedBlock
}

// Render renders cfg.Blocks against tpl and writes the full page shell to
// w. tpl must contain the "boff/shell" definition (parse your block templates
// into a clone of Shell) and the sub-templates any TemplateBlock refers to.
func Render(w io.Writer, tpl *template.Template, cfg RenderConfig) error {
	rendered, err := RenderBlocks(tpl, cfg.Blocks)
	if err != nil {
		return err
	}

	if err := tpl.ExecuteTemplate(w, "boff/shell", shellData{RenderConfig: cfg, Blocks: rendered}); err != nil {
		return fmt.Errorf("render shell: %w", err)
	}

	return nil
}
