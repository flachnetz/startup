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

// Shell is the parsed page shell together with the built-in block sub-templates
// (block/summary, block/actions, overview/*). The built-in blocks render against
// it, so it stays the home of those definitions. Page packages do not parse into
// Shell; they build their own template with Templates or TemplatesFromFS.
var Shell = MustTemplatesFromFS(shellFS)

// defaultFuncs registers the funcs every boff page template shares, so a page's
// own block templates can use them without re-declaring: formatTime for
// timestamps, add for index arithmetic in ranges, formatMoney for amounts.
func defaultFuncs(t *template.Template) *template.Template {
	return t.Funcs(template.FuncMap{
		"formatTime":  func(t time.Time) string { return t.Format("2006-01-02 15:04:05.000") },
		"add":         func(a, b int) int { return a + b },
		"formatMoney": formatMoney,
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
// (formatTime, add). Parse your own block sub-templates into it, then pass it to
// Render. The built-in blocks resolve their own sub-templates against Shell, so
// this template needs only whatever your custom blocks define.
func Templates() *template.Template {
	return defaultFuncs(template.New("boff"))
}

// TemplatesFromFS is Templates with every .gohtml file in fsys parsed in, for a
// page that keeps its block templates in an embedded FS. It walks fsys, so the
// files may sit at any depth (a templates/ subdirectory, say) without the caller
// naming a glob. The parsed definitions and the shared funcs are all available
// to Render.
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
// built-in Shell. Each block renders with a RenderContext carrying cfg.Viewer,
// so blocks gate themselves; blocks resolve their own sub-templates via the
// template they were built with. Use RenderWithShell to supply your own shell
// template (a clone of Shell carrying extra funcs or sub-templates).
func Render(w io.Writer, cfg RenderConfig) error {
	return RenderWithShell(w, Shell, cfg)
}

// RenderWithShell is Render with an explicit shell template. tpl provides the
// "boff/shell" definition (parse your block templates into a clone of Shell).
func RenderWithShell(w io.Writer, tpl *template.Template, cfg RenderConfig) error {
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
