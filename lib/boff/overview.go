package boff

import (
	"io"
	"net/url"
	"strconv"
)

// OverviewFilter is one field of the overview filter form. Value is the
// currently applied value, echoed back so the form stays sticky.
type OverviewFilter struct {
	Label       string
	Name        string
	Value       string
	Placeholder string
	// Type is the HTML input type; empty means "text". "date" gets the browser's
	// own date picker, no JavaScript needed.
	Type string
	// Options, when non-empty, renders a <select> instead of an input. The empty
	// value must be part of the list to allow "no filter".
	Options []FilterOption
	// Hidden keeps the value in the form and in the pagination links without
	// showing a control - for a filter the page receives from elsewhere (a deep
	// link carrying a player id) rather than one the viewer types.
	Hidden bool
}

// FilterOption is one entry of an OverviewFilter dropdown.
type FilterOption struct {
	Value string
	Label string
}

// InputType is the type attribute of a text-ish filter input.
func (f OverviewFilter) InputType() string {
	if f.Type == "" {
		return "text"
	}

	return f.Type
}

// OverviewConfig bundles everything the overview page renders.
type OverviewConfig struct {
	Title     string
	Headers   []string
	Rows      []OverviewRow
	Filters   []OverviewFilter
	ScopeNote string

	// Page is the 1-based page number shown; 0 and 1 both mean the first page.
	// HasNext tells the pager that another page exists - the caller knows this by
	// loading one row more than it displays, so no count query is needed.
	Page    int
	HasNext bool
	// TotalPages enables the jump to the last page. Leave it 0 when counting the
	// whole result set is not worth a second query; the pager then only walks.
	TotalPages int

	// Blocks overrides the sections rendered on the page. When nil the page uses
	// its default layout: FiltersBlock, ScopeNoteBlock, a PagerBlock, TableBlock
	// and a trailing PagerBlock built from the fields above. When set, the
	// callback receives those default blocks and returns the blocks to render in
	// order - so a caller can reorder them, drop one, or splice its own Block in.
	Blocks func(defaults DefaultOverviewBlocks) []Block
}

// DefaultOverviewBlocks are the built-in blocks an overview page renders out of
// the box, handed to an OverviewConfig.Blocks callback so custom layouts can
// reuse them. Pager is shared by the top and bottom pager.
type DefaultOverviewBlocks struct {
	Filters   Block
	ScopeNote Block
	Pager     Block
	Table     Block
}

// All returns the default blocks in their default order (filters, scope note,
// pager, table, pager).
func (d DefaultOverviewBlocks) All() []Block {
	return []Block{d.Filters, d.ScopeNote, d.Pager, d.Table, d.Pager}
}

// PageParam is the query parameter the overview pager pages with.
const PageParam = "page"

// pageLink builds the URL of another page of the same filtered list. Filters are
// carried over, so paging never silently widens the list.
func pageLink(filters []OverviewFilter, page int) string {
	query := url.Values{}
	for _, f := range filters {
		if f.Value != "" {
			query.Set(f.Name, f.Value)
		}
	}

	if page > 1 {
		query.Set(PageParam, strconv.Itoa(page))
	}

	if len(query) == 0 {
		return "?"
	}

	return "?" + query.Encode()
}

// OverviewRow is one list entry; Cells aligns with the overview Headers.
//
// Link makes the whole row navigate to one detail page. CellLinks instead links
// individual cells (index-aligned with Cells, empty entry = plain text), which is
// what a table with several targets needs - an id column pointing at this
// service's detail page, a foreign id pointing at the owning service. A cell link
// is rendered as a visible link; a row link stays inconspicuous.
type OverviewRow struct {
	Link      string
	Cells     []string
	CellLinks []string
}

// CellAt pairs a cell with its own link, so the template does not have to index
// two slices in parallel.
func (r OverviewRow) CellAt(i int) OverviewCell {
	cell := OverviewCell{Text: r.Cells[i]}
	if i < len(r.CellLinks) {
		cell.Link = r.CellLinks[i]
	}

	return cell
}

// OverviewCell is one table cell with its optional own link.
type OverviewCell struct {
	Text string
	Link string
}

// RenderOverview writes a standalone clickable table; each row links to Link.
func RenderOverview(w io.Writer, title string, headers []string, rows []OverviewRow) error {
	return RenderOverviewWithConfig(w, OverviewConfig{Title: title, Headers: headers, Rows: rows})
}

// RenderOverviewWithConfig is RenderOverview with the optional display elements
// (filter form, scope note).
func RenderOverviewWithConfig(w io.Writer, cfg OverviewConfig) error {
	page := max(cfg.Page, 1)

	pager := PagerModel{Page: page, TotalPages: cfg.TotalPages}

	if page > 1 {
		pager.FirstLink = pageLink(cfg.Filters, 1)
		pager.PrevLink = pageLink(cfg.Filters, page-1)
	}

	if cfg.HasNext {
		pager.NextLink = pageLink(cfg.Filters, page+1)
	}

	// Walking back from page 7 to page 1 is one click; walking forward to the end
	// only when the caller knows where the end is.
	if cfg.TotalPages > page {
		pager.LastLink = pageLink(cfg.Filters, cfg.TotalPages)
	}

	defaults := DefaultOverviewBlocks{
		Filters:   FiltersBlock(cfg.Filters),
		ScopeNote: ScopeNoteBlock(cfg.ScopeNote),
		Pager:     PagerBlock(pager),
		Table:     TableBlock(cfg.Headers, cfg.Rows),
	}

	blocks := defaults.All()
	if cfg.Blocks != nil {
		blocks = cfg.Blocks(defaults)
	}

	return Render(w, Shell, RenderConfig{Title: cfg.Title, Blocks: blocks})
}
