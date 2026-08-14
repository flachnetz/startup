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
// template. A block gates itself here - it consults rc.May (or the exposed
// rc.GateActions / rc.DemoteLinks helpers) and emits only what the viewer may
// see. Because the context is passed on every call, a container block hands the
// same rc to its children, so gating and template resolution compose to any
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
// Template must contain a definition for Name. The built-in blocks set it to
// Shell (or a clone), so they resolve the sub-templates defined alongside the
// shell. Set Empty to render nothing, which is how a section vanishes when it
// has no content.
type TemplateBlock struct {
	Name     string
	Model    any
	Empty    bool
	Template *template.Template
}

func (b TemplateBlock) Render(RenderContext) (template.HTML, error) {
	if b.Empty {
		return "", nil
	}

	if b.Template == nil {
		return "", fmt.Errorf("render block %q: no template", b.Name)
	}

	var buf bytes.Buffer
	if err := b.Template.ExecuteTemplate(&buf, b.Name, b.Model); err != nil {
		return "", fmt.Errorf("render block %q: %w", b.Name, err)
	}

	return template.HTML(buf.String()), nil //nolint:gosec // template output is already escaped
}

// HTMLBlock is a block of pre-rendered HTML, for callers that want to drop in
// arbitrary markup without a template.
type HTMLBlock template.HTML

func (b HTMLBlock) Render(RenderContext) (template.HTML, error) {
	return template.HTML(b), nil
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

// SummaryBlock is a SummaryItem list rendered as the current-state summary card.
// It gates itself at render time: the links a viewer may not follow are demoted
// to plain values. Renders nothing when empty.
//
// The slice is the block: boff.SummaryBlock{...} or a boff.SummaryBlock(items)
// conversion both give you a Block.
type SummaryBlock []SummaryItem

func (b SummaryBlock) Render(rc RenderContext) (template.HTML, error) {
	items := DemoteLinks(rc, b)

	return TemplateBlock{Name: "block/summary", Model: items, Empty: len(items) == 0, Template: Shell}.Render(rc)
}

// ActionsBlock is an Action list rendered as the actions table. It gates itself
// at render time: the actions a viewer may not perform are dropped. Renders
// nothing when empty.
//
// The slice is the block: boff.ActionsBlock{...} or a boff.ActionsBlock(actions)
// conversion both give you a Block.
type ActionsBlock []Action

func (b ActionsBlock) Render(rc RenderContext) (template.HTML, error) {
	actions := GateActions(rc, b)

	return TemplateBlock{Name: "block/actions", Model: actions, Empty: len(actions) == 0, Template: Shell}.Render(rc)
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

// FiltersBlock renders the GET filter form. Renders nothing when filters is
// empty.
func FiltersBlock(filters []OverviewFilter) Block {
	return TemplateBlock{Name: "overview/filters", Model: filters, Empty: len(filters) == 0, Template: Shell}
}

// ScopeNoteBlock renders the one-line scope note. Renders nothing when empty.
func ScopeNoteBlock(note string) Block {
	return TemplateBlock{Name: "overview/scopenote", Model: note, Empty: note == "", Template: Shell}
}

// PagerBlock renders the pagination nav. Renders nothing when there is no
// previous or next page.
func PagerBlock(m PagerModel) Block {
	return TemplateBlock{Name: "overview/pager", Model: m, Empty: m.PrevLink == "" && m.NextLink == "", Template: Shell}
}

// TableBlock renders the clickable rows table. Always renders (shows a "No
// records." row when rows is empty).
func TableBlock(headers []string, rows []OverviewRow) Block {
	return TemplateBlock{Name: "overview/table", Model: tableModel{Headers: headers, Rows: rows}, Template: Shell}
}
