package history

import (
	"github.com/flachnetz/startup/v2/lib/boff"
)

// SummaryBlock renders the current-state summary card above the ledger. Renders
// nothing when items is empty.
func SummaryBlock(items []SummaryItem) boff.Block {
	return boff.TemplateBlock{Name: "block/summary", Model: items, Empty: len(items) == 0}
}

// ActionsBlock renders the actions table. Renders nothing when actions is empty.
func ActionsBlock(actions []Action) boff.Block {
	return boff.TemplateBlock{Name: "block/actions", Model: actions, Empty: len(actions) == 0}
}

// HistoryItemsBlock renders the record ledger. Always renders (shows a "No
// history records." note when views is empty).
func HistoryItemsBlock(views []RecordView) boff.Block {
	return boff.TemplateBlock{Name: "block/records", Model: views}
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
func FiltersBlock(filters []OverviewFilter) boff.Block {
	return boff.TemplateBlock{Name: "overview/filters", Model: filters, Empty: len(filters) == 0}
}

// ScopeNoteBlock renders the one-line scope note. Renders nothing when empty.
func ScopeNoteBlock(note string) boff.Block {
	return boff.TemplateBlock{Name: "overview/scopenote", Model: note, Empty: note == ""}
}

// PagerBlock renders the pagination nav. Renders nothing when there is no
// previous or next page.
func PagerBlock(m PagerModel) boff.Block {
	return boff.TemplateBlock{Name: "overview/pager", Model: m, Empty: m.PrevLink == "" && m.NextLink == ""}
}

// TableBlock renders the clickable rows table. Always renders (shows a "No
// records." row when rows is empty).
func TableBlock(headers []string, rows []OverviewRow) boff.Block {
	return boff.TemplateBlock{Name: "overview/table", Model: tableModel{Headers: headers, Rows: rows}}
}
