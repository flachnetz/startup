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
	"strings"
	"time"

	"github.com/flachnetz/startup/v2/lib/boff"
	"github.com/flachnetz/startup/v2/lib/jwt"
	"github.com/flachnetz/startup/v2/lib/ql"
)

//go:embed templates/history.gohtml
var templateFS embed.FS

var pageTemplate = boff.MustTemplatesFromFS(templateFS)

// Severity levels a PageConfig.Level classifier may return. Anything else -
// including the empty string every unclassified record carries - renders a
// neutral grey pip: informative, no claim about severity.
const (
	LevelOk    = "ok"
	LevelWarn  = "warn"
	LevelError = "err"
)

// RecordView wraps a Record with its pretty-printed JSON payload for rendering.
type RecordView struct {
	Record
	// JSON is the indented payload.
	JSON string
	// Level is the severity of this record, one of LevelOk, LevelWarn,
	// LevelError, or empty for neutral. It comes from PageConfig.Level; without a
	// classifier every record is neutral.
	Level string
	// Key identifies this record's payload panel on the page. It is derived from
	// the trace and the position inside it - a Record carries no id of its own -
	// and is stable across a refetch, which is what lets an expanded payload
	// survive one.
	Key string
}

// TimeOfDay is the record's time without its date, which the trace header shows
// once for the whole block.
func (v RecordView) TimeOfDay() string { return v.Timestamp.Format("15:04:05") }

// Millis is the fractional part of the record's time, rendered faint next to
// TimeOfDay so a burst of records within one second stays readable.
func (v RecordView) Millis() string { return v.Timestamp.Format(".000") }

// HTTPSource is the "POST /public/v1/checkout" segment of a record's second
// line, empty for a record that did not arrive over HTTP (a consumer, a
// scheduler) - such a row simply omits the segment instead of showing an empty
// one.
func (v RecordView) HTTPSource() string {
	if v.Trigger.Source != "http" {
		return ""
	}

	return v.Trigger.Detail
}

// SourceRef is the id of the request or message this record came from, e.g. a
// request id. Empty when the trigger carries none.
func (v RecordView) SourceRef() string { return v.Trigger.Ref }

// SourceRefLabel names the kind of SourceRef for the row's title attribute.
func (v RecordView) SourceRefLabel() string {
	if v.Trigger.RefType == "" {
		return "source"
	}

	return v.Trigger.RefType
}

// ActorId is who initiated the work, empty for an anonymous or system-triggered
// record.
func (v RecordView) ActorId() string { return v.Trigger.Actor.Id }

// ActorType is the kind of principal behind ActorId (player, staff, service).
func (v RecordView) ActorType() string { return string(v.Trigger.Actor.Type) }

// FieldCount is how many top-level fields the payload carries - what the
// payload chip shows. Zero means no chip at all.
func (v RecordView) FieldCount() int { return boff.JSONFieldCount(v.JSON) }

// JSONHTML is the payload colourised with Bootstrap text-colour utilities. Those
// classes come from the stylesheet the page already has - server-side because a
// client-side highlighter would be dropped together with the <head> when the
// page is embedded as a backoffice fragment.
func (v RecordView) JSONHTML() template.HTML {
	return template.HTML(boff.HighlightJSON(v.JSON)) //nolint:gosec // HighlightJSON escapes its input
}

// HasPayload reports whether the record carries a payload worth displaying -
// i.e. it is not empty and not just "{}", which many events carry as a
// placeholder body.
func (v RecordView) HasPayload() bool {
	switch strings.TrimSpace(v.JSON) {
	case "", "{}":
		return false
	default:
		return true
	}
}

// TraceView is one request trace: every record written while handling the same
// incoming request, in the order they happened. The ledger renders one block
// per trace, so a page reads as "what did this request do" rather than as one
// long undifferentiated list.
type TraceView struct {
	// Id is the request trace id shared by every record in the block.
	Id string
	// Events are the records of this trace, oldest first - a trace block reads
	// top to bottom as the request unfolded.
	Events []RecordView
}

// Start is when the trace's first record was written, whichever end of the
// block that record now sits at.
func (t TraceView) Start() time.Time {
	if len(t.Events) == 0 {
		return time.Time{}
	}

	start := t.Events[0].Timestamp
	for _, event := range t.Events[1:] {
		if event.Timestamp.Before(start) {
			start = event.Timestamp
		}
	}

	return start
}

// End is when the trace's last record was written.
func (t TraceView) End() time.Time {
	if len(t.Events) == 0 {
		return time.Time{}
	}

	end := t.Events[0].Timestamp
	for _, event := range t.Events[1:] {
		if event.Timestamp.After(end) {
			end = event.Timestamp
		}
	}

	return end
}

// Date is the day the trace happened, shown once in the trace header so the
// rows themselves can show a time only.
func (t TraceView) Date() string {
	if len(t.Events) == 0 {
		return ""
	}

	return t.Start().Format("2006-01-02")
}

// Count is how many records the trace holds.
func (t TraceView) Count() int { return len(t.Events) }

// Duration is how long the trace took, from its first record to its last -
// independent of the direction the block is rendered in.
func (t TraceView) Duration() string {
	if len(t.Events) == 0 {
		return ""
	}

	return formatDuration(t.End().Sub(t.Start()))
}

// formatDuration renders a span the way an operator scans it: sub-second spans
// in milliseconds, anything longer in seconds with two decimals.
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%d ms", d.Milliseconds())
	}

	return fmt.Sprintf("%.2f s", d.Seconds())
}

// TraceOrder is the reading direction of the ledger. Its zero value is the
// default: everything oldest first, so the page reads as one timeline from top
// to bottom - the tracked object is created at the top and reaches its current
// state at the bottom.
//
// The two axes are independent on purpose. "Newest trace at the top, but each
// trace still read forwards" is a real way to work a busy ledger: the most
// recent request is what an operator came for, and inside it they still want
// cause before effect.
type TraceOrder struct {
	// NewestTracesFirst puts the most recent trace block at the top.
	NewestTracesFirst bool

	// NewestEventsFirst reverses the records inside every trace block.
	NewestEventsFirst bool
}

// tracesOf groups records into trace blocks and orders both the blocks and the
// records inside them according to order (see TraceOrder for the default).
// Sorting is stable, so records sharing a timestamp keep the order they arrived
// in whichever direction is chosen.
func tracesOf(views []RecordView, order TraceOrder) []TraceView {
	var traces []TraceView

	index := make(map[string]int, len(views))
	for _, view := range views {
		id := view.RequestTraceId.String()
		at, ok := index[id]
		if !ok {
			index[id] = len(traces)
			traces = append(traces, TraceView{Id: id})
			at = len(traces) - 1
		}

		traces[at].Events = append(traces[at].Events, view)
	}

	for _, trace := range traces {
		sort.SliceStable(trace.Events, func(i, j int) bool {
			return earlier(trace.Events[i].Timestamp, trace.Events[j].Timestamp, order.NewestEventsFirst)
		})
	}

	// Blocks are ordered by when the trace started, which is now the first record
	// of the block only in the default direction - hence Start, not Events[0].
	sort.SliceStable(traces, func(i, j int) bool {
		return earlier(traces[i].Start(), traces[j].Start(), order.NewestTracesFirst)
	})

	return traces
}

// earlier is the less function of both sorts: a is before b, or after it when
// reversed.
func earlier(a, b time.Time, reversed bool) bool {
	if reversed {
		return a.After(b)
	}

	return a.Before(b)
}

// recordsModel is the template model for "block/records": the trace blocks plus
// the totals and controls of the section heading above them.
type recordsModel struct {
	Traces []TraceView
	// Events is the total number of records across every trace.
	Events int
	// Duration spans the whole ledger, from the oldest record to the newest.
	Duration string
	// HasLevels is true when at least one record was classified, i.e. when the
	// severity filter has something to filter on. Without it the control is not
	// rendered - a filter that can only ever match everything is noise.
	HasLevels bool
}

// Traces is how many trace blocks the ledger holds.
func (m recordsModel) TraceCount() int { return len(m.Traces) }

// recordsModelOf builds the ledger model from the views, in render order.
func recordsModelOf(views []RecordView, order TraceOrder) recordsModel {
	model := recordsModel{Traces: tracesOf(views, order), Events: len(views)}

	for _, view := range views {
		if view.Level != "" {
			model.HasLevels = true
			break
		}
	}

	if len(model.Traces) > 0 {
		// The span of the whole ledger, from the earliest record to the latest,
		// whichever direction the blocks are rendered in.
		first, last := model.Traces[0].Start(), model.Traces[0].End()
		for _, trace := range model.Traces[1:] {
			if start := trace.Start(); start.Before(first) {
				first = start
			}
			if end := trace.End(); end.After(last) {
				last = end
			}
		}

		model.Duration = formatDuration(last.Sub(first))
	}

	return model
}

// HistoryItemsBlock renders the record ledger: one block per request trace,
// oldest trace first, each block a top-to-bottom timeline of its records - the
// whole page reads as one timeline, oldest at the top. Every
// payload starts collapsed behind a chip showing its field count. Always
// renders (shows a "No history records." note when views is empty).
//
// Use HistoryItemsBlockOrdered to read the ledger in the other direction.
func HistoryItemsBlock(views []RecordView) boff.Block {
	return HistoryItemsBlockOrdered(views, TraceOrder{})
}

// HistoryItemsBlockOrdered is HistoryItemsBlock with an explicit reading
// direction; the zero TraceOrder is the default (everything oldest first).
func HistoryItemsBlockOrdered(views []RecordView, order TraceOrder) boff.Block {
	return boff.TemplateBlock{
		Name:     "block/records",
		Model:    recordsModelOf(views, order),
		Template: pageTemplate,
	}
}

// HistoryItemsBlockCollapsed is HistoryItemsBlock.
//
// Deprecated: payloads are always collapsed now, so there is nothing left to
// choose. It stays for callers that named it explicitly.
func HistoryItemsBlockCollapsed(views []RecordView) boff.Block {
	return HistoryItemsBlock(views)
}

// PageConfig bundles all optional display elements for a history detail page.
// Use with RenderPageWithConfig to avoid the combinatorial explosion of
// RenderPage* method variants.
type PageConfig struct {
	Summary   []boff.SummaryItem
	Actions   []boff.Action
	CreatedAt time.Time // zero = local-only, non-zero = Athena fallback

	// CollapseAllPayloads has no effect: every payload is collapsed behind its
	// own chip now.
	//
	// Deprecated: kept so existing callers still compile.
	CollapseAllPayloads bool

	// Level classifies a record's severity for the pip on its ledger row:
	// LevelOk, LevelWarn, LevelError, or "" for neutral. Nil means every record
	// is neutral - this package does not guess a severity from a step name, since
	// only the service writing the ledger knows which of its steps are bad news.
	// A page with no classifier also renders no severity filter.
	Level func(Record) string

	// Order is the reading direction of the ledger. The zero value reads oldest
	// first, both across trace blocks and inside them.
	Order TraceOrder

	// Viewer overrides the identity used for RequiredRole gating. Normally the
	// identity comes from the request context; set this when rendering outside a
	// request (a test, a report). No viewer at all means every gated element is
	// omitted - fail closed.
	Viewer *jwt.Identity

	// Blocks overrides the sections rendered on the page. When nil the page uses
	// its default layout: a HeaderBlock, then SummaryBlock, ActionsBlock and
	// HistoryItemsBlock built from the fields above. When set, the callback
	// receives those default blocks (already gated) plus the loaded record views,
	// and returns the blocks to render in order - so a caller can reorder them,
	// drop one, or splice its own Block in between.
	Blocks func(defaults DefaultBlocks) []boff.Block
}

// DefaultBlocks are the built-in blocks a page renders out of the box, handed to
// a PageConfig.Blocks callback so custom layouts can reuse them.
type DefaultBlocks struct {
	Header  boff.Block
	Summary boff.Block
	Actions boff.Block
	Records boff.Block
}

// All returns the default blocks in their default order.
func (d DefaultBlocks) All() []boff.Block {
	return []boff.Block{d.Header, d.Summary, d.Actions, d.Records}
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

		level := ""
		if cfg.Level != nil {
			level = cfg.Level(rec)
		}

		views[i] = RecordView{
			Record: rec,
			JSON:   pretty.String(),
			Level:  level,
			// A Record has no id of its own, so the panel is keyed by its trace and
			// its position in the ledger - stable across a refetch of the same page.
			Key: fmt.Sprintf("ev-%s-%d", rec.RequestTraceId.String(), i),
		}
	}

	recordsBlock := HistoryItemsBlockOrdered(views, cfg.Order)

	defaults := DefaultBlocks{
		Header:  boff.HeaderBlock{Title: title, Subtitle: "GroupId: " + groupId.String()},
		Summary: boff.SummaryBlock(cfg.Summary),
		Actions: boff.ActionsBlock(cfg.Actions),
		Records: recordsBlock,
	}

	blocks := defaults.All()
	if cfg.Blocks != nil {
		blocks = cfg.Blocks(defaults)
	}

	return boff.Render(w, boff.RenderConfig{
		Title:  title,
		Viewer: boff.ViewerOf(ctx, cfg.Viewer),
		Blocks: blocks,
	})
}
