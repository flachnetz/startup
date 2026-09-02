// Package boff holds the block-based rendering machinery used by backoffice
// pages. A page is a slice of Block; each block renders to a fragment of HTML,
// and the blocks are concatenated top to bottom into a page shell. This is the
// generic core, free of any particular page's models, so it can back a history
// ledger, an overview list, or any other backoffice page.
package boff

import (
	"bytes"
	"fmt"
	"html/template"
)

// Block is one renderable section of a page. Blocks are rendered top to bottom
// in the order given, so a page is just a slice of them. Providing your own
// Block is how you extend a page beyond its built-in sections without touching
// the page shell.
//
// Render receives a RenderContext carrying the viewing identity and the shell
// template. A block gates itself here - it consults rc.May and emits only what
// the viewer may see. Because the context is passed on every call, a container
// block hands the same rc to its children, so gating and template resolution
// compose to any
// nesting depth.
//
// A block is free to produce any HTML it likes. An empty block returns no bytes
// and is simply skipped.
type Block interface {
	Render(rc RenderContext) (template.HTML, error)
}

// TemplateBlock renders a named (sub-)template with a model. It is the common
// shape of the built-in blocks and the easiest way to add your own: give it the
// Name of a template and a Model to execute it with.
//
// Template must contain a definition for Name. The built-in blocks set it to the
// package shell, so they resolve the sub-templates defined alongside the shell.
// Set Skip to render nothing, which is how a section vanishes when it has no
// content.
type TemplateBlock struct {
	Name     string
	Model    any
	Skip     bool
	Template *template.Template
}

func (b TemplateBlock) Render(rc RenderContext) (template.HTML, error) {
	if b.Skip {
		return "", nil
	}

	if b.Template == nil {
		return "", fmt.Errorf("render block %q: no template", b.Name)
	}

	html, err := RenderTemplate(rc, b.Template, b.Name, b.Model)
	if err != nil {
		return "", fmt.Errorf("render block %q: %w", b.Name, err)
	}

	return html, nil
}

// RenderTemplate executes the named (sub-)template of tmpl with data, bound to
// rc, and returns its HTML. It is the plumbing behind TemplateBlock and the way
// to render a template from your own Block: the template can render a child Block
// inline via {{ . | render }}, which executes against the same rc. tmpl is
// cloned per call so the binding never leaks into a shared template, which also
// means tmpl itself is never executed and stays reusable.
func RenderTemplate(rc RenderContext, tmpl *template.Template, name string, data any) (template.HTML, error) {
	var buf bytes.Buffer
	if err := withRenderContext(tmpl, rc).ExecuteTemplate(&buf, name, data); err != nil {
		return "", err
	}

	return template.HTML(buf.String()), nil //nolint:gosec // template output is already escaped
}

// withRenderContext clones tmpl and injects the funcs bound to rc: "render", so a
// template can render a child Block inline ({{ .Body | render }} executes the
// block against the same render context, returning its HTML), and "allowed", so
// a template can gate a fragment inline ({{ if allowed .RequiredRole }}...{{ end }})
// using the same rc.May check a Go block uses.
//
// tmpl is cloned, so the injected funcs never leak into the shared template; a
// clone failure is a programmer error (a template already executed), so it
// panics.
func withRenderContext(tmpl *template.Template, rc RenderContext) *template.Template {
	return template.Must(tmpl.Clone()).Funcs(template.FuncMap{
		"render": func(block Block) (template.HTML, error) {
			if block == nil {
				return "", nil
			}

			return block.Render(rc)
		},
		"allowed": func(required Role) (bool, error) {
			return rc.May(required), nil
		},
	})
}

// HTMLBlock is a block of pre-rendered HTML, for callers that want to drop in
// arbitrary markup without a template.
type HTMLBlock template.HTML

func (b HTMLBlock) Render(RenderContext) (template.HTML, error) {
	return template.HTML(b), nil //nolint:gosec // HTMLBlock is pre-rendered markup by contract
}

// BlockFunc adapts a plain function to a Block, so a one-off block needs no
// named type.
type BlockFunc func(rc RenderContext) (template.HTML, error)

func (f BlockFunc) Render(rc RenderContext) (template.HTML, error) { return f(rc) }

// Gate wraps a block so it renders only when the viewer satisfies required, in
// the notation of Action.RequiredRole. A denied viewer sees nothing - the whole
// wrapped block vanishes. This is the coarse counterpart to the fine-grained,
// per-item gating SummaryBlock and ActionsBlock do: reach for it to hide an
// entire section (a card, a whole custom block) behind one role.
func Gate(required Role, block Block) Block {
	return BlockFunc(func(rc RenderContext) (template.HTML, error) {
		if !rc.May(required) {
			return "", nil
		}

		return block.Render(rc)
	})
}

// Blocks renders a sequence of child blocks and concatenates their output into
// one HTML fragment, so a container block can hold other blocks - and so a page
// turns its whole block slice into markup. Empty children (those that render to
// nothing) are skipped, and the same rc is passed to each, so gating reaches
// nested blocks unchanged.
func Blocks(children ...Block) Block {
	return BlockFunc(func(rc RenderContext) (template.HTML, error) {
		var buf bytes.Buffer
		for _, child := range children {
			html, err := child.Render(rc)
			if err != nil {
				return "", err
			}

			buf.WriteString(string(html))
		}

		return template.HTML(buf.String()), nil //nolint:gosec // children already escaped
	})
}

// HeaderBlock is the page heading: an h1 title, an optional monospace subtitle,
// and an optional error alert. It was the shell's built-in header; as a block a
// page places it wherever it likes (or drops it), for full control of the page
// layout.
type HeaderBlock struct {
	Title        string
	Subtitle     string
	ErrorMessage string
}

func (b HeaderBlock) Render(rc RenderContext) (template.HTML, error) {
	return TemplateBlock{Name: "block/header", Model: b, Template: shell}.Render(rc)
}

// CardBlock wraps a body block in a Bootstrap card with a title and optional
// subtitle. The body is itself a Block, rendered inline by the card template via
// the render func (RenderTemplate), with the same rc - so gating and nested
// blocks compose inside a card like anywhere else.
type CardBlock struct {
	Title    string
	Subtitle string
	Body     Block
	// Raised adds a small drop shadow (Bootstrap shadow-sm) to lift the card off
	// the page.
	Raised bool
}

func (b CardBlock) Render(rc RenderContext) (template.HTML, error) {
	return TemplateBlock{Name: "block/card", Model: b, Template: shell}.Render(rc)
}

// PanelBlock wraps child blocks in a bare Bootstrap card frame, one that adds
// no card-body padding of its own - unlike CardBlock, whose Body always sits in
// a padded card-body. Reach for PanelBlock when a card holds sections with
// different padding needs (e.g. a padded filter form above a table that must
// sit flush against the card's edges); each child renders itself and is
// responsible for its own spacing. A child that renders to nothing (Skip, or
// an empty Blocks) simply contributes nothing.
type PanelBlock struct {
	Children []Block
	// Raised adds a small drop shadow (Bootstrap shadow-sm) to lift the card off
	// the page.
	Raised bool
}

func (b PanelBlock) Render(rc RenderContext) (template.HTML, error) {
	return TemplateBlock{Name: "block/panel", Model: b, Template: shell}.Render(rc)
}

// summaryCardModel is what the block/summary template renders: the label/value
// pairs as grid facts, the item-shaped entries as full-width rows below them,
// plus an optional card title (empty renders no heading), so a page with more
// than one summary section can label each one. Id is a per-card prefix for the
// ids of the collapsible payload panels, so two summary cards on one page
// cannot collide.
type summaryCardModel struct {
	Title string
	Id    string
	Facts []SummaryItem
	Rows  []SummaryItem
}

// summaryModelOf splits items into the grid facts and the full-width rows. An
// item gets its own row when it describes a whole small record rather than one
// value: either explicitly (it carries a SummaryRow) or implicitly (it carries
// a JSON payload). A payload has no business in a 164px grid column - it would
// be clipped by the column it sits in - and a caller that attaches one is
// describing a record either way.
func summaryModelOf(title, id string, items []SummaryItem) summaryCardModel {
	model := summaryCardModel{Title: title, Id: id}
	for _, item := range items {
		if item.Row != nil || item.JSON != "" {
			model.Rows = append(model.Rows, item)
			continue
		}

		model.Facts = append(model.Facts, item)
	}

	return model
}

// SummaryBlock is a SummaryItem list rendered as the current-state summary card.
// It gates itself at render time: the links a viewer may not follow are demoted
// to plain values. Renders nothing when empty.
//
// The slice is the block: boff.SummaryBlock{...} or a boff.SummaryBlock(items)
// conversion both give you a Block. Use SummaryCard instead for a titled card.
type SummaryBlock []SummaryItem

func (b SummaryBlock) Render(rc RenderContext) (template.HTML, error) {
	items := demoteLinks(rc, b)

	return TemplateBlock{
		Name:     "block/summary",
		Model:    summaryModelOf("", "summary", items),
		Skip:     len(items) == 0,
		Template: shell,
	}.Render(rc)
}

// SummaryCard is SummaryBlock under a card title, for a page that shows more
// than one summary section (e.g. account data next to profile data) and needs
// each one labeled. It gates and renders nothing exactly like SummaryBlock.
func SummaryCard(title string, items []SummaryItem) Block {
	return BlockFunc(func(rc RenderContext) (template.HTML, error) {
		gated := demoteLinks(rc, items)

		return TemplateBlock{
			Name:     "block/summary",
			Model:    summaryModelOf(title, "summary-"+slug(title), gated),
			Skip:     len(gated) == 0,
			Template: shell,
		}.Render(rc)
	})
}

// ActionsBlock is an Action list rendered as the actions table. It gates itself
// at render time: the actions a viewer may not perform are dropped. Renders
// nothing when empty.
//
// The slice is the block: boff.ActionsBlock{...} or a boff.ActionsBlock(actions)
// conversion both give you a Block.
type ActionsBlock []Action

func (b ActionsBlock) Render(rc RenderContext) (template.HTML, error) {
	actions := GateSlice(rc, b)

	return TemplateBlock{Name: "block/actions", Model: actions, Skip: len(actions) == 0, Template: shell}.Render(rc)
}

// PagerModel is the pagination state a PagerBlock renders. Both pagers (above
// and below the table) render the same model. The Link fields are empty when
// there is no such page.
type PagerModel struct {
	Page       int
	TotalPages int
	FirstLink  string
	PrevLink   string
	NextLink   string
	LastLink   string
}

// tableModel is what the table sub-template reads.
type tableModel struct {
	Headers []string
	Rows    []OverviewRow
}

// FiltersBlock renders the GET filter form on its own, outside any card. Reach
// for FilterableTableBlock instead when the filters belong above a results
// table - the common case, and the default overview layout. Renders nothing
// when filters is empty.
func FiltersBlock(filters []OverviewFilter) Block {
	return TemplateBlock{Name: "overview/filters", Model: filters, Skip: len(filters) == 0, Template: shell}
}

// ScopeNoteBlock renders the one-line scope note. Renders nothing when empty.
func ScopeNoteBlock(note string) Block {
	return TemplateBlock{Name: "overview/scopenote", Model: note, Skip: note == "", Template: shell}
}

// PagerBlock renders the pagination nav. Renders nothing when there is no
// previous or next page.
func PagerBlock(m PagerModel) Block {
	return TemplateBlock{Name: "overview/pager", Model: m, Skip: m.PrevLink == "" && m.NextLink == "", Template: shell}
}

// tableRowsBlock is the table itself, flush against its container (no card, no
// padding) - the shared piece TableBlock and FilterableTableBlock both nest
// inside a PanelBlock. Always renders (shows a "No records." row when rows is
// empty).
func tableRowsBlock(headers []string, rows []OverviewRow) Block {
	return TemplateBlock{Name: "overview/table-flush", Model: tableModel{Headers: headers, Rows: rows}, Template: shell}
}

// filtersCardBodyBlock is the filter form padded as one card-body section,
// bordered off from whatever follows it in the same PanelBlock. Renders
// nothing when filters is empty.
func filtersCardBodyBlock(filters []OverviewFilter) Block {
	return TemplateBlock{Name: "overview/filters-card-body", Model: filters, Skip: len(filters) == 0, Template: shell}
}

// TableBlock renders the clickable rows table in its own card. Always renders
// (shows a "No records." row when rows is empty). Use this for a table with no
// filter form (e.g. a fixed recent-N list); use FilterableTableBlock when the
// table has a filter form above it, so both sit in one card.
func TableBlock(headers []string, rows []OverviewRow) Block {
	return PanelBlock{Raised: true, Children: []Block{tableRowsBlock(headers, rows)}}
}

// FilterableTableBlock renders the filter form and the results table together
// in one card, the form separated from the table by a divider. This is what
// RenderOverviewWithConfig uses by default. Renders the table even when
// filters is empty (the form section is simply omitted).
func FilterableTableBlock(filters []OverviewFilter, headers []string, rows []OverviewRow) Block {
	return PanelBlock{Raised: true, Children: []Block{
		filtersCardBodyBlock(filters),
		tableRowsBlock(headers, rows),
	}}
}
