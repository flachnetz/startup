package boff

import (
	"embed"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"reflect"
	"strings"
	"time"

	"github.com/flachnetz/startup/v2/lib/jwt"
)

//go:embed templates
var shellFS embed.FS

// shell is the pristine base template: the page shell plus the built-in block
// sub-templates (block/summary, block/actions, overview/*), parsed from shellFS.
// It is never executed or mutated directly - a template that has executed can no
// longer be cloned - only cloned (by RenderTemplate, per render). It is
// unexported so no caller can execute it by accident.
var shell = MustTemplatesFromFS(shellFS)

// defaultFuncs returns a fresh empty template named "boff" carrying the funcs
// every boff page template shares, so a page's own block templates can use them
// without re-declaring: formatTime for timestamps, add for index arithmetic in
// ranges, formatMoney for amounts, pipClass/statusClass for the shared
// console vocabulary, and placeholder render and allowed funcs
// (rebound per call by RenderTemplate) so templates using {{ . | render }} or
// {{ if allowed .RequiredRole }} still parse.
func defaultFuncs() *template.Template {
	return template.New("boff").Funcs(template.FuncMap{
		"formatTime":  func(t time.Time) string { return t.Format("2006-01-02 15:04:05.000") },
		"add":         func(a, b int) int { return a + b },
		"formatMoney": formatMoney,
		"pipClass":    pipClass,
		"statusClass": statusClass,
		"payload":     newPayloadModel,
		"render": func(Block) (template.HTML, error) {
			return "", fmt.Errorf("render: template executed without RenderTemplate - the render func was never bound to a RenderContext")
		},
		"allowed": func(Role) (bool, error) {
			return false, fmt.Errorf("allowed: template executed without RenderTemplate - the allowed func was never bound to a RenderContext")
		},
	})
}

// formatMoney renders an integer amount in minor units (cents) with its currency
// as "0.00 EUR": formatMoney 1234 "EUR" is "12.34 EUR". A negative amount keeps
// its sign before the digits ("-12.34 EUR"). minor is taken via reflection so a
// template can pass any integer kind (int, int32, int64).
func formatMoney(minor any, currency string) (string, error) {
	v := reflect.ValueOf(minor)
	if !v.CanInt() {
		return "", fmt.Errorf("formatMoney: amount must be an integer, got %T", minor)
	}

	n := v.Int()

	sign := ""
	if n < 0 {
		sign, n = "-", -n
	}

	return fmt.Sprintf("%s%d.%02d %s", sign, n/100, n%100, currency), nil
}

// Templates returns a fresh page template carrying the shared default funcs
// (formatTime, add, formatMoney, pipClass, statusClass, payload,
// render) and the shared console partials (boff/id, boff/payload-chip,
// boff/payload-panel), so a page's own blocks render a copyable id or a
// collapsible payload exactly like the built-in ones. Parse your own block
// sub-templates into it, then pass it to RenderWithShell. It is a fresh template
// every call, so callers never share (or accidentally execute) a common base.
func Templates() *template.Template {
	tpl, err := defaultFuncs().ParseFS(shellFS, "templates/console.gohtml")
	if err != nil {
		// An embedded template that does not parse is a build-time bug.
		panic(fmt.Errorf("parse console partials: %w", err))
	}

	return tpl
}

// TemplatesFromFS is Templates with every .gohtml file in fsys parsed in, for a
// page that keeps its block templates in an embedded FS. It walks fsys, so the
// files may sit at any depth (a templates/ subdirectory, say) without the caller
// naming a glob. The parsed definitions and the shared funcs are all available
// to RenderWithShell.
func TemplatesFromFS(fsys fs.FS) (*template.Template, error) {
	tpl := Templates()

	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !strings.HasSuffix(path, ".gohtml") {
			return nil
		}

		if _, err := tpl.ParseFS(fsys, path); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("parse page templates: %w", err)
	}

	return tpl, nil
}

// MustTemplatesFromFS is TemplatesFromFS for a package-level var: it panics on a
// parse error, since an embedded template that fails to parse is a build-time
// bug, not a runtime condition.
func MustTemplatesFromFS(fsys fs.FS) *template.Template {
	tpl, err := TemplatesFromFS(fsys)
	if err != nil {
		panic(err)
	}

	return tpl
}

// RenderConfig is the caller-supplied config for the shell. Title feeds the
// document <title>; the visible page heading is a HeaderBlock a page adds to
// Blocks. Blocks are rendered into the shell body in order.
//
// Viewer is the identity the page's gated blocks filter themselves against. It
// is put into the RenderContext each block renders with. Leave it nil to fail
// closed - every gated element is then hidden. Use ViewerOf to derive it from
// the request context.
type RenderConfig struct {
	Title  string
	Viewer *jwt.Identity
	Blocks []Block
}

// shellData is what the shell template actually executes against: the config
// plus the rendered blocks concatenated into one HTML fragment.
type shellData struct {
	RenderConfig
	Blocks template.HTML
}

// Render renders cfg.Blocks and writes the full page shell to w, using the
// built-in shell. Each block renders with a RenderContext carrying cfg.Viewer,
// so blocks gate themselves. Use RenderWithShell to supply your own shell
// template (from Templates or TemplatesFromFS) carrying extra funcs or
// sub-templates.
func Render(w io.Writer, cfg RenderConfig) error {
	return RenderWithShell(w, shell, cfg)
}

// RenderWithShell is Render with an explicit shell template, from Templates or
// TemplatesFromFS. tpl is never executed directly - RenderTemplate executes a
// render-bound clone so tpl stays pristine and reusable.
func RenderWithShell(w io.Writer, tpl *template.Template, cfg RenderConfig) error {
	rc := RenderContext{Viewer: cfg.Viewer}

	blocks, err := Blocks(cfg.Blocks...).Render(rc)
	if err != nil {
		return err
	}

	html, err := RenderTemplate(rc, tpl, "boff/shell", shellData{RenderConfig: cfg, Blocks: blocks})
	if err != nil {
		return fmt.Errorf("render shell: %w", err)
	}

	if _, err := io.WriteString(w, string(html)); err != nil {
		return fmt.Errorf("write shell: %w", err)
	}

	return nil
}
