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

// PageModel is the template model for the generic history page.
type PageModel struct {
	Title        string
	GroupId      string
	ErrorMessage string
	Summary      []SummaryItem
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

// RenderPageSummary is RenderPage with an extra current-state summary rendered
// above the ledger.
func (h *Service) RenderPageSummary(ctx context.Context, w io.Writer, groupId GroupId, title string, summary []SummaryItem) error {
	records, err := ql.InNewTransactionWithResult(ctx, h.txStarter, func(ctx ql.TxContext) ([]Record, error) {
		return h.Records(ctx, groupId)
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
		Title:   title,
		GroupId: groupId.String(),
		Summary: summary,
		Records: views,
	})
}
