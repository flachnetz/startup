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

	"github.com/flachnetz/startup/v2/lib/ql"
)

//go:embed templates/history.gohtml
var templateFS embed.FS

//go:embed templates/overview.gohtml
var overviewFS embed.FS

var pageTemplate = template.Must(template.New("history.gohtml").
	Funcs(template.FuncMap{
		"formatTime": func(t time.Time) string { return t.Format("2006-01-02 15:04:05.000") },
	}).
	ParseFS(templateFS, "templates/history.gohtml"))

var overviewTemplate = template.Must(template.New("overview.gohtml").
	ParseFS(overviewFS, "templates/overview.gohtml"))

// OverviewModel is the template model for a clickable list page linking to
// per-item detail pages.
type OverviewModel struct {
	Title   string
	Headers []string
	Rows    []OverviewRow
}

// OverviewRow is one list entry; Cells aligns with OverviewModel.Headers and
// Link is the detail-page URL the row navigates to.
type OverviewRow struct {
	Link  string
	Cells []string
}

// RenderOverview writes a standalone clickable table; each row links to Link.
func RenderOverview(w io.Writer, title string, headers []string, rows []OverviewRow) error {
	if err := overviewTemplate.Execute(w, OverviewModel{Title: title, Headers: headers, Rows: rows}); err != nil {
		return fmt.Errorf("render overview: %w", err)
	}
	return nil
}

// RecordView wraps a Record with its pretty-printed JSON payload for rendering.
type RecordView struct {
	Record
	// JSON is the indented payload.
	JSON string
	// ShowSeparator is true when this record starts a new RequestTraceId group.
	ShowSeparator bool
}

// Action is one row in the actions table on the history detail page. When
// StatusText is non-empty the row renders a static label instead of a button
// (e.g. "Cancelled"); otherwise a clickable button that fires an HTTP request
// to Endpoint is shown.
type Action struct {
	Description    string // e.g. "Cancel item Sword-Pack"
	ButtonText     string // e.g. "Cancel"
	Method         string // HTTP method, default POST
	Endpoint       string // e.g. "/internal/v1/orders/123/items/1/cancel"
	ConfirmMessage string // optional confirm() prompt; empty = no confirmation
	StatusText     string // non-empty = show label instead of button
}

// PageConfig bundles all optional display elements for a history detail page.
// Use with RenderPageWithConfig to avoid the combinatorial explosion of
// RenderPage* method variants.
type PageConfig struct {
	Summary   []SummaryItem
	Actions   []Action
	CreatedAt time.Time // zero = local-only, non-zero = Athena fallback
}

// PageModel is the template model for the generic history page.
type PageModel struct {
	Title        string
	GroupType    string
	GroupId      string
	ErrorMessage string
	Summary      []SummaryItem
	Actions      []Action
	Records      []RecordView
}

// SummaryItem is one label/value row shown above the ledger, describing the
// current state of the tracked object. Ordered slice (not a map) so the page
// renders stably.
type SummaryItem struct {
	Label string
	Value string
}

// RenderPage writes a standalone HTML history page for groupId to w. Records are
// loaded in a new read transaction, sorted by Timestamp (Service.Records is
// unordered), and an <hr> separates consecutive RequestTraceId groups.
//
// ponytail: payload is rendered as pretty JSON only; add a key/value table when
// an item needs structured display.
func (h *Service) RenderPage(ctx context.Context, w io.Writer, groupId GroupId, title string) error {
	return h.RenderPageSummary(ctx, w, groupId, title, nil)
}

// RenderPageAt is RenderPage with an Athena fallback: records are loaded via
// RecordsAt using createdTime to decide between the local table and Athena.
func (h *Service) RenderPageAt(ctx context.Context, w io.Writer, groupId GroupId, title string, createdTime time.Time) error {
	return h.renderPage(ctx, w, groupId, title, nil, nil, createdTime)
}

// RenderPageSummary is RenderPage with an extra current-state summary rendered
// above the ledger.
func (h *Service) RenderPageSummary(ctx context.Context, w io.Writer, groupId GroupId, title string, summary []SummaryItem) error {
	// zero time: RecordsAt always reads the local table.
	return h.renderPage(ctx, w, groupId, title, summary, nil, time.Time{})
}

// RenderPageSummaryAt is RenderPageSummary with the Athena fallback (see RenderPageAt).
func (h *Service) RenderPageSummaryAt(ctx context.Context, w io.Writer, groupId GroupId, title string, summary []SummaryItem, createdTime time.Time) error {
	return h.renderPage(ctx, w, groupId, title, summary, nil, createdTime)
}

// RenderPageWithConfig renders the history page using PageConfig for all
// optional display elements (summary, actions, Athena fallback).
func (h *Service) RenderPageWithConfig(ctx context.Context, w io.Writer, groupId GroupId, title string, cfg PageConfig) error {
	return h.renderPage(ctx, w, groupId, title, cfg.Summary, cfg.Actions, cfg.CreatedAt)
}

func (h *Service) renderPage(ctx context.Context, w io.Writer, groupId GroupId, title string, summary []SummaryItem, actions []Action, createdTime time.Time) error {
	records, err := ql.InNewTransactionWithResult(ctx, h.txStarter, func(ctx ql.TxContext) ([]Record, error) {
		return h.RecordsAt(ctx, groupId, createdTime)
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

	return pageTemplate.Execute(w, PageModel{
		Title:     title,
		GroupType: groupId.Type,
		GroupId:   groupId.String(),
		Summary:   summary,
		Actions:   actions,
		Records:   views,
	})
}
