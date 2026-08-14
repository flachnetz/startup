package history

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"sort"
	"time"

	"github.com/flachnetz/startup/v2/lib/boff"
	"github.com/flachnetz/startup/v2/lib/jwt"
	"github.com/flachnetz/startup/v2/lib/ql"
)

//go:embed templates/history.gohtml
var templateFS embed.FS

var pageTemplate = template.Must(template.Must(boff.Shell.Clone()).
	Funcs(template.FuncMap{
		"formatTime": func(t time.Time) string { return t.Format("2006-01-02 15:04:05.000") },
		"add":        func(a, b int) int { return a + b },
	}).
	ParseFS(templateFS, "templates/history.gohtml"))

// RecordView wraps a Record with its pretty-printed JSON payload for rendering.
type RecordView struct {
	Record
	// JSON is the indented payload.
	JSON string
	// ShowSeparator is true when this record starts a new RequestTraceId group.
	ShowSeparator bool
}

// JSONHTML is the payload colourised with Bootstrap text-colour utilities. Those
// classes come from the stylesheet the page already has - server-side because a
// client-side highlighter would be dropped together with the <head> when the
// page is embedded as a backoffice fragment.
func (v RecordView) JSONHTML() template.HTML {
	return template.HTML(boff.HighlightJSON(v.JSON)) //nolint:gosec // HighlightJSON escapes its input
}

// HistoryItemsBlock renders the record ledger. Always renders (shows a "No
// history records." note when views is empty).
func HistoryItemsBlock(views []RecordView) boff.Block {
	return boff.TemplateBlock{Name: "block/records", Model: views}
}

// PageConfig bundles all optional display elements for a history detail page.
// Use with RenderPageWithConfig to avoid the combinatorial explosion of
// RenderPage* method variants.
type PageConfig struct {
	Summary   []boff.SummaryItem
	Actions   []boff.Action
	CreatedAt time.Time // zero = local-only, non-zero = Athena fallback

	// Viewer overrides the identity used for RequiredRole gating. Normally the
	// identity comes from the request context; set this when rendering outside a
	// request (a test, a report). No viewer at all means every gated element is
	// omitted - fail closed.
	Viewer *jwt.Identity

	// Blocks overrides the sections rendered on the page. When nil the page uses
	// its default layout: SummaryBlock, ActionsBlock and HistoryItemsBlock built
	// from the fields above. When set, the callback receives those default blocks
	// (already gated) plus the loaded record views, and returns the blocks to
	// render in order - so a caller can reorder them, drop one, or splice its own
	// Block in between.
	Blocks func(defaults DefaultBlocks) []boff.Block
}

// DefaultBlocks are the built-in blocks a page renders out of the box, handed to
// a PageConfig.Blocks callback so custom layouts can reuse them.
type DefaultBlocks struct {
	Summary boff.Block
	Actions boff.Block
	Records boff.Block
}

// All returns the default blocks in their default order.
func (d DefaultBlocks) All() []boff.Block {
	return []boff.Block{d.Summary, d.Actions, d.Records}
}

// RenderPage writes a standalone HTML history page for groupId to w. Records are
// loaded in a new read transaction, sorted by Timestamp (Service.Records is
// unordered), and each RequestTraceId group is rendered in its own card.
//
// ponytail: payload is rendered as pretty JSON only; add a key/value table when
// an item needs structured display.
func (h *Service) RenderPage(ctx context.Context, w io.Writer, groupId GroupId, title string) error {
	return h.RenderPageSummary(ctx, w, groupId, title, nil)
}

// RenderPageAt is RenderPage with an Athena fallback: records are loaded via
// RecordsAt using createdTime to decide between the local table and Athena.
func (h *Service) RenderPageAt(ctx context.Context, w io.Writer, groupId GroupId, title string, createdTime time.Time) error {
	return h.renderPage(ctx, w, groupId, title, PageConfig{CreatedAt: createdTime})
}

// RenderPageSummary is RenderPage with an extra current-state summary rendered
// above the ledger.
func (h *Service) RenderPageSummary(ctx context.Context, w io.Writer, groupId GroupId, title string, summary []boff.SummaryItem) error {
	// zero time: RecordsAt always reads the local table.
	return h.renderPage(ctx, w, groupId, title, PageConfig{Summary: summary})
}

// RenderPageSummaryAt is RenderPageSummary with the Athena fallback (see RenderPageAt).
func (h *Service) RenderPageSummaryAt(ctx context.Context, w io.Writer, groupId GroupId, title string, summary []boff.SummaryItem, createdTime time.Time) error {
	return h.renderPage(ctx, w, groupId, title, PageConfig{Summary: summary, CreatedAt: createdTime})
}

// RenderPageWithConfig renders the history page using PageConfig for all
// optional display elements (summary, actions, Athena fallback).
func (h *Service) RenderPageWithConfig(ctx context.Context, w io.Writer, groupId GroupId, title string, cfg PageConfig) error {
	return h.renderPage(ctx, w, groupId, title, cfg)
}

func (h *Service) renderPage(ctx context.Context, w io.Writer, groupId GroupId, title string, cfg PageConfig) error {
	summary, actions := boff.Gate(boff.ViewerOf(ctx, cfg.Viewer), cfg.Summary, cfg.Actions)

	records, err := ql.InNewTransactionWithResult(ctx, h.txStarter, func(ctx ql.TxContext) ([]Record, error) {
		return h.RecordsAt(ctx, groupId, cfg.CreatedAt)
	})
	if err != nil {
		return fmt.Errorf("load records: %w", err)
	}

	sort.SliceStable(records, func(i, j int) bool {
		return records[i].Timestamp.Before(records[j].Timestamp)
	})

	views := make([]RecordView, len(records))
	for i, rec := range records {
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, rec.Payload, "", "  "); err != nil {
			// keep the raw payload if it is not valid JSON.
			pretty.Reset()
			pretty.Write(rec.Payload)
		}

		views[i] = RecordView{
			Record:        rec,
			JSON:          pretty.String(),
			ShowSeparator: i > 0 && rec.RequestTraceId.String() != records[i-1].RequestTraceId.String(),
		}
	}

	defaults := DefaultBlocks{
		Summary: boff.SummaryBlock(summary),
		Actions: boff.ActionsBlock(actions),
		Records: HistoryItemsBlock(views),
	}

	blocks := defaults.All()
	if cfg.Blocks != nil {
		blocks = cfg.Blocks(defaults)
	}

	return boff.Render(w, pageTemplate, boff.RenderConfig{
		Title:    title,
		Subtitle: "GroupId: " + groupId.String(),
		Blocks:   blocks,
	})
}
