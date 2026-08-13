package history

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/flachnetz/startup/v2/lib/jwt"
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
	// Filters renders a GET form above the table; empty means no form. Only
	// free filters belong here (a player id, a request id). Tenant scope
	// (operator, shop) travels as a header set by backoffice, never as a form
	// field a viewer can retype.
	Filters []OverviewFilter
	// ScopeNote is one line naming the scope the rows were loaded with (e.g.
	// "operator bmh-audio-pt, all shops"), so the viewer sees why the list is
	// short.
	ScopeNote string

	// Page is the 1-based page currently shown; PrevLink/NextLink are empty when
	// there is no such page.
	Page     int
	PrevLink string
	NextLink string
}

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

// OverviewConfig bundles everything the overview page renders. Mirrors
// PageConfig, so a new display element does not grow the argument list of
// RenderOverview.
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

// OverviewRow is one list entry; Cells aligns with OverviewModel.Headers and
// Link is the detail-page URL the row navigates to.
type OverviewRow struct {
	Link  string
	Cells []string
}

// RenderOverview writes a standalone clickable table; each row links to Link.
func RenderOverview(w io.Writer, title string, headers []string, rows []OverviewRow) error {
	return RenderOverviewWithConfig(w, OverviewConfig{Title: title, Headers: headers, Rows: rows})
}

// RenderOverviewWithConfig is RenderOverview with the optional display elements
// (filter form, scope note).
func RenderOverviewWithConfig(w io.Writer, cfg OverviewConfig) error {
	page := max(cfg.Page, 1)

	model := OverviewModel{
		Title:     cfg.Title,
		Headers:   cfg.Headers,
		Rows:      cfg.Rows,
		Filters:   cfg.Filters,
		ScopeNote: cfg.ScopeNote,
		Page:      page,
	}

	if page > 1 {
		model.PrevLink = pageLink(cfg.Filters, page-1)
	}

	if cfg.HasNext {
		model.NextLink = pageLink(cfg.Filters, page+1)
	}
	if err := overviewTemplate.Execute(w, model); err != nil {
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

// jsonToken matches one JSON string (optionally an object key, i.e. followed by
// a colon), literal or number in already-indented JSON.
var jsonToken = regexp.MustCompile(`"(?:\\.|[^"\\])*"\s*:?|\b(?:true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?`)

// payloadEscaper escapes exactly what is unsafe in element text. Quotes stay
// intact so the highlighter can still see JSON strings.
var payloadEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// JSONHTML is the payload colourised with Bootstrap text-colour utilities. Those
// classes come from the stylesheet the page already has - server-side because a
// client-side highlighter would be dropped together with the <head> when the
// page is embedded as a backoffice fragment.
func (v RecordView) JSONHTML() template.HTML {
	return template.HTML(highlightJSON(v.JSON)) //nolint:gosec // highlightJSON escapes its input
}

func highlightJSON(payload string) string {
	escaped := payloadEscaper.Replace(payload)

	return jsonToken.ReplaceAllStringFunc(escaped, func(token string) string {
		var class string
		switch {
		case strings.HasSuffix(token, ":"):
			class = "text-primary"
		case strings.HasPrefix(token, `"`):
			class = "text-success"
		case token == "true" || token == "false" || token == "null":
			class = "text-body-secondary fst-italic"
		default:
			class = "text-danger"
		}

		return `<span class="` + class + `">` + token + `</span>`
	})
}

// Action is one row in the actions table on the history detail page. When
// StatusText is non-empty the row renders a static label instead of a button
// (e.g. "Cancelled"); otherwise a plain HTML form that POSTs to Endpoint.
//
// The page ships no JavaScript: the form is a form, and a ConfirmMessage renders
// a Bootstrap modal driven by Bootstrap's own JS. Endpoint must therefore be a
// URL the browser can resolve - a service behind backoffice builds it from the
// base path backoffice sends with the fragment request, not from its own
// internal path.
type Action struct {
	Description    string // e.g. "Cancel item Sword-Pack"
	ButtonText     string // e.g. "Cancel"
	Endpoint       string // e.g. "/orders/backoffice/v1/orders/123/items/1/cancel"
	ConfirmMessage string // optional confirmation prompt; empty = submit immediately
	StatusText     string // non-empty = show label instead of button

	// true renders a link to Endpoint instead of a form, for an action that only
	// navigates. Ignores ConfirmMessage.
	Link bool

	// RequiredRole gates the whole action: "write" or "admin" means that role on
	// this service's own audience, "payment-service:admin" names another
	// audience. Empty means always shown. A viewer who may not perform the action
	// does not see it at all - a disabled button still tells a read-only user
	// which endpoint to curl.
	RequiredRole string
}

// PageConfig bundles all optional display elements for a history detail page.
// Use with RenderPageWithConfig to avoid the combinatorial explosion of
// RenderPage* method variants.
type PageConfig struct {
	Summary   []SummaryItem
	Actions   []Action
	CreatedAt time.Time // zero = local-only, non-zero = Athena fallback

	// Viewer overrides the identity used for RequiredRole gating. Normally the
	// identity comes from the request context; set this when rendering outside a
	// request (a test, a report). No viewer at all means every gated element is
	// omitted - fail closed.
	Viewer *jwt.Identity
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
	// Link, when non-empty, renders Value as an <a href> to this URL instead of
	// plain text - e.g. a cross-service backoffice link to the page that owns the
	// referenced entity (a payment or draw history page behind the api-gateway).
	Link string

	// RequiredRole gates the Link only, in the same notation as
	// Action.RequiredRole. A denied item still shows Label and Value, just not as
	// an anchor: the value itself is not the secret, the page behind it is.
	RequiredRole string
}

// mayPerform reports whether viewer satisfies required. An empty required is
// ungated; no viewer denies everything gated.
//
// ponytail: a role qualified with a foreign audience is denied unless the viewer
// was issued for that audience. Cross-audience display via the advisory
// Actor-Roles header is only worth building when a page actually mixes services.
func mayPerform(viewer *jwt.Identity, required string) bool {
	if required == "" {
		return true
	}

	if viewer == nil {
		return false
	}

	audience, role := "", required
	if idx := strings.IndexByte(required, ':'); idx >= 0 {
		audience, role = required[:idx], required[idx+1:]
	}

	if audience != "" && audience != viewer.Audience {
		return false
	}

	return viewer.HasRole(role)
}

// gate drops the actions the viewer may not perform and demotes denied summary
// links to plain values.
func gate(viewer *jwt.Identity, summary []SummaryItem, actions []Action) ([]SummaryItem, []Action) {
	gatedSummary := make([]SummaryItem, 0, len(summary))
	for _, item := range summary {
		if item.Link != "" && !mayPerform(viewer, item.RequiredRole) {
			item.Link = ""
		}
		gatedSummary = append(gatedSummary, item)
	}

	gatedActions := make([]Action, 0, len(actions))
	for _, action := range actions {
		if mayPerform(viewer, action.RequiredRole) {
			gatedActions = append(gatedActions, action)
		}
	}

	if len(summary) == 0 {
		gatedSummary = nil
	}

	if len(gatedActions) == 0 {
		gatedActions = nil
	}

	return gatedSummary, gatedActions
}

// viewerOf returns the identity used for gating: the explicit override, else the
// verified identity of the request, else nil.
func viewerOf(ctx context.Context, override *jwt.Identity) *jwt.Identity {
	if override != nil {
		return override
	}

	if identity, ok := jwt.IdentityFrom(ctx); ok {
		return &identity
	}

	return nil
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
	return h.renderPage(ctx, w, groupId, title, PageConfig{CreatedAt: createdTime})
}

// RenderPageSummary is RenderPage with an extra current-state summary rendered
// above the ledger.
func (h *Service) RenderPageSummary(ctx context.Context, w io.Writer, groupId GroupId, title string, summary []SummaryItem) error {
	// zero time: RecordsAt always reads the local table.
	return h.renderPage(ctx, w, groupId, title, PageConfig{Summary: summary})
}

// RenderPageSummaryAt is RenderPageSummary with the Athena fallback (see RenderPageAt).
func (h *Service) RenderPageSummaryAt(ctx context.Context, w io.Writer, groupId GroupId, title string, summary []SummaryItem, createdTime time.Time) error {
	return h.renderPage(ctx, w, groupId, title, PageConfig{Summary: summary, CreatedAt: createdTime})
}

// RenderPageWithConfig renders the history page using PageConfig for all
// optional display elements (summary, actions, Athena fallback).
func (h *Service) RenderPageWithConfig(ctx context.Context, w io.Writer, groupId GroupId, title string, cfg PageConfig) error {
	return h.renderPage(ctx, w, groupId, title, cfg)
}

func (h *Service) renderPage(ctx context.Context, w io.Writer, groupId GroupId, title string, cfg PageConfig) error {
	summary, actions := gate(viewerOf(ctx, cfg.Viewer), cfg.Summary, cfg.Actions)

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

	return pageTemplate.Execute(w, PageModel{
		Title:     title,
		GroupType: groupId.Type,
		GroupId:   groupId.String(),
		Summary:   summary,
		Actions:   actions,
		Records:   views,
	})
}
