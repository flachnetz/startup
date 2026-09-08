package history

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/flachnetz/startup/v2/lib/actor"
	"github.com/flachnetz/startup/v2/lib/boff"
)

// at builds a timestamp on a fixed day, so a test can talk in seconds and
// milliseconds without carrying a full date around.
func at(second int, millis int) time.Time {
	return time.Date(2026, 9, 1, 15, 52, second, millis*int(time.Millisecond), time.UTC)
}

// view is a RecordView as renderPage builds one, minus the database.
func view(traceId string, ts time.Time, step, description, payload, level string) RecordView {
	return RecordView{
		Timestamp:   ts,
		Step:        step,
		Description: description,
		Payload:     json.RawMessage(payload),
		EventSender: "order_service",
		JSON:        payload,
		Level:       level,
		Key:         "ev-" + traceId + "-" + step,
	}
}

// withTrace tags a view with a trace id. RequestTraceId only decodes from its
// hex form, which is exactly what a stored record carries.
func withTrace(v RecordView, hexTraceId string) RecordView {
	var id RequestTraceId
	if err := id.Scan(hexTraceId); err != nil {
		panic(err)
	}
	v.RequestTraceId = id

	return v
}

const (
	traceOld = "005ad836695a73d91e08b44f966e7961"
	traceNew = "7c2f19ab4d8e41f2ba03c65d17e9a844"
)

func render(t *testing.T, views []RecordView) string {
	t.Helper()

	html, err := HistoryItemsBlock(views).Render(boff.RenderContext{})
	if err != nil {
		t.Fatalf("render ledger: %v", err)
	}

	return string(html)
}

// The whole ledger is one timeline: trace blocks oldest first, and the records
// inside a block oldest first too, so the page reads top to bottom in the order
// things happened.
func TestLedgerOrdersTracesAndEventsOldestFirst(t *testing.T) {
	// Input arrives in insertion order, oldest record first, as renderPage sorts
	// it. The second trace is deliberately fed first to prove the blocks are
	// sorted by when they started, not by the order they were encountered.
	out := render(t, []RecordView{
		withTrace(view(traceNew, at(43, 120), "WebhookReceived", "payment_paid", "{}", ""), traceNew),
		withTrace(view(traceOld, at(41, 835), "SessionRequested", "Checkout request received", "{}", ""), traceOld),
		withTrace(view(traceOld, at(41, 902), "PaymentCreated", "mbway", "{}", ""), traceOld),
		withTrace(view(traceNew, at(43, 221), "OrderStatusChanged", "PENDING_PAYMENT to PAID", "{}", ""), traceNew),
	})

	order := []string{"SessionRequested", "PaymentCreated", "WebhookReceived", "OrderStatusChanged"}
	previous := -1
	for _, step := range order {
		at := strings.Index(out, ">"+step+"<")
		if at < 0 {
			t.Fatalf("step %s not rendered:\n%s", step, out)
		}
		if at < previous {
			t.Errorf("step %s rendered out of order (expected %v):\n%s", step, order, out)
		}
		previous = at
	}
}

// The trace header carries the shortened id, the record count, the span of the
// trace and its date - the date appears once per block, never on a row.
func TestLedgerTraceHeaderSummarisesTheBlock(t *testing.T) {
	out := render(t, []RecordView{
		withTrace(view(traceOld, at(41, 835), "SessionRequested", "received", "{}", ""), traceOld),
		withTrace(view(traceOld, at(43, 305), "OrderFulfilled", "fulfilled", "{}", ""), traceOld),
	})

	for _, want := range []string{
		`<span class="lbl">Trace</span>`,
		`title="` + traceOld + " \u2014 click to copy\"",
		">005ad836695a73d91e08b44f966e7961</span>",
		// The trace id is a thing to copy, so it carries the copy control.
		`class="id-copy"`,
		"2 events",
		"1.47 s",
		"2026-09-01",
		// Rows show a time only, with the milliseconds faint.
		`<span class="ev-t">15:52:41<span class="opacity-50">.835</span></span>`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("trace header missing %s:\n%s", want, out)
		}
	}
}

// A sub-second trace is measured in milliseconds; seconds would read as "0.15 s".
func TestLedgerShortTraceIsMeasuredInMilliseconds(t *testing.T) {
	out := render(t, []RecordView{
		withTrace(view(traceOld, at(41, 835), "A", "a", "{}", ""), traceOld),
		withTrace(view(traceOld, at(41, 902), "B", "b", "{}", ""), traceOld),
	})

	if !strings.Contains(out, "67 ms") {
		t.Errorf("sub-second trace not rendered in milliseconds:\n%s", out)
	}
}

// Only a row that has a payload can be opened, so only that row is a <details>
// whose summary is the row itself. A row without one stays a plain div, so it
// never lights up under the pointer promising a detail view that is not there.
func TestLedgerOnlyRowsWithPayloadAreOpenable(t *testing.T) {
	out := render(t, []RecordView{
		withTrace(view(traceOld, at(41, 835), "OrderCreated", "created", `{"orderId":"o1"}`, ""), traceOld),
		withTrace(view(traceOld, at(41, 902), "OrderFulfilled", "fulfilled", "{}", ""), traceOld),
	})

	if got := strings.Count(out, `<details class="ev-p"`); got != 1 {
		t.Errorf("%d rows render as details, want 1:\n%s", got, out)
	}
	if got := strings.Count(out, `<summary class="ev">`); got != 1 {
		t.Errorf("%d rows render as a summary, want 1:\n%s", got, out)
	}
	// The severity filter hides whole rows, so data-level sits on the outer
	// element of both shapes - hiding a details takes its payload with it.
	if got := strings.Count(out, `<div class="ev" data-level="">`); got != 1 {
		t.Errorf("%d plain rows, want 1 (the record without a payload):\n%s", got, out)
	}
}

// A payload sits inside a closed <details>, so it starts collapsed, opens
// without any script, and find-in-page can reach into it. The chip is a label on
// the row, not a second control nested inside the summary.
func TestLedgerPayloadStartsCollapsed(t *testing.T) {
	out := render(t, []RecordView{
		withTrace(view(traceOld, at(41, 835), "OrderCreated", "created",
			`{"orderId":"o1","status":"VERIFIED","totalMinor":250}`, ""), traceOld),
	})

	for _, want := range []string{
		// No open attribute, so the row starts closed.
		`<details class="ev-p" data-level="" data-payload="ev-` + traceOld + `-OrderCreated">`,
		`<summary class="ev">`,
		`<span class="btn-payload">{ } 3</span>`,
		`<pre class="payload">`,
		`<span class="text-primary">"orderId":</span>`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("payload row missing %s:\n%s", want, out)
		}
	}

	// The summary is the control, so none of the aria bookkeeping a scripted
	// disclosure needs is left to keep in sync.
	for _, unwanted := range []string{"aria-expanded", "aria-controls", " hidden>"} {
		if strings.Contains(out, unwanted) {
			t.Errorf("payload row still carries %s:\n%s", unwanted, out)
		}
	}
}

// An empty payload gets no chip at all - a chip reading "{ } 0" is a control
// that does nothing.
func TestLedgerEmptyPayloadHasNoChip(t *testing.T) {
	out := render(t, []RecordView{
		withTrace(view(traceOld, at(41, 835), "OrderFulfilled", "fulfilled", "{}", ""), traceOld),
	})

	if strings.Contains(out, "btn-payload") {
		t.Errorf("empty payload still rendered a chip:\n%s", out)
	}
}

// A record that did not arrive over HTTP omits the verb/path segment instead of
// rendering an empty one; an HTTP record shows it.
func TestLedgerHTTPSourceOnlyForHTTPTriggers(t *testing.T) {
	consumed := withTrace(view(traceOld, at(41, 835), "PaymentCaptured", "captured", "{}", ""), traceOld)
	consumed.Trigger = Trigger{Source: "message-broker", Detail: "topic payment_captured", RefType: "kafkaEventId", Ref: "01M1ETRDQ8FJ4W2K7ZP0X5NVB1"}

	served := withTrace(view(traceOld, at(41, 902), "OrderCreated", "created", "{}", ""), traceOld)
	served.Trigger = Trigger{
		Source: "http", Detail: "POST /public/v1/checkout",
		RefType: "requestId", Ref: "01M1ETRD12CEXT501ZYQZ3EHY6",
		Actor: actor.Actor{Type: "player", Id: "01KXJMFRY454B6BH7Y3TYNWEXR"},
	}

	out := render(t, []RecordView{consumed, served})

	if !strings.Contains(out, "POST /public/v1/checkout") {
		t.Errorf("http source not rendered:\n%s", out)
	}
	if strings.Contains(out, "topic payment_captured") {
		t.Errorf("non-http trigger detail was rendered as a verb/path segment:\n%s", out)
	}
	// Ids on the second line render in full, with the kind of id in the title.
	if !strings.Contains(out, `title="requestId 01M1ETRD12CEXT501ZYQZ3EHY6">01M1ETRD12CEXT501ZYQZ3EHY6<`) {
		t.Errorf("source ref not rendered in full:\n%s", out)
	}
	if !strings.Contains(out, `title="player 01KXJMFRY454B6BH7Y3TYNWEXR"`) {
		t.Errorf("actor not rendered:\n%s", out)
	}
}

// Without a Level classifier every pip is neutral and no severity filter is
// offered - a filter that can only match everything is noise.
func TestLedgerWithoutLevelsIsNeutralAndOffersNoFilter(t *testing.T) {
	out := render(t, []RecordView{
		withTrace(view(traceOld, at(41, 835), "OrderCreated", "created", "{}", ""), traceOld),
	})

	if !strings.Contains(out, `<span class="pip"></span>`) {
		t.Errorf("unclassified record did not render a neutral pip:\n%s", out)
	}
	if strings.Contains(out, "data-level-filter") {
		t.Errorf("severity filter offered without any classified record:\n%s", out)
	}
}

// A classified ledger renders the severity on the row (as a pip and as the
// filter's data attribute) and offers the filter.
func TestLedgerLevelsRenderPipsAndFilter(t *testing.T) {
	out := render(t, []RecordView{
		withTrace(view(traceOld, at(41, 835), "OrderCreated", "created", "{}", LevelOk), traceOld),
		withTrace(view(traceOld, at(41, 902), "WebhookIgnored", "duplicate", "{}", LevelWarn), traceOld),
		withTrace(view(traceOld, at(42, 100), "PaymentDeclined", "declined", "{}", LevelError), traceOld),
	})

	for _, want := range []string{
		`data-level="ok"`, `data-level="warn"`, `data-level="err"`,
		`<span class="pip pip-ok"></span>`, `<span class="pip pip-warn"></span>`, `<span class="pip pip-err"></span>`,
		"data-level-filter",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("classified ledger missing %s:\n%s", want, out)
		}
	}
}

// An empty ledger says so instead of rendering an empty card.
func TestLedgerEmptyRendersNote(t *testing.T) {
	out := render(t, nil)

	if !strings.Contains(out, "No history records.") {
		t.Errorf("empty ledger did not render its note:\n%s", out)
	}
	if strings.Contains(out, "data-expand-all") {
		t.Errorf("empty ledger still offered the payload controls:\n%s", out)
	}
}

// The section heading counts traces and events and spans the whole ledger.
func TestLedgerHeadingCountsTracesAndEvents(t *testing.T) {
	out := render(t, []RecordView{
		withTrace(view(traceOld, at(41, 835), "A", "a", "{}", ""), traceOld),
		withTrace(view(traceOld, at(41, 902), "B", "b", "{}", ""), traceOld),
		withTrace(view(traceNew, at(43, 305), "C", "c", "{}", ""), traceNew),
	})

	if !strings.Contains(out, "2 traces &middot; 3 events &middot; 1.47 s") {
		t.Errorf("ledger heading did not summarise the page:\n%s", out)
	}
}

func TestFormatDuration(t *testing.T) {
	cases := map[time.Duration]string{
		67 * time.Millisecond:   "67 ms",
		999 * time.Millisecond:  "999 ms",
		time.Second:             "1.00 s",
		1470 * time.Millisecond: "1.47 s",
		0:                       "0 ms",
	}

	for d, want := range cases {
		if got := formatDuration(d); got != want {
			t.Errorf("formatDuration(%s) = %q, want %q", d, got, want)
		}
	}
}

// renderOrdered is render with an explicit reading direction.
func renderOrdered(t *testing.T, views []RecordView, order TraceOrder) string {
	t.Helper()

	html, err := HistoryItemsBlockOrdered(views, order).Render(boff.RenderContext{})
	if err != nil {
		t.Fatalf("render ledger: %v", err)
	}

	return string(html)
}

// stepOrder is the steps in the order they appear on the page.
func stepOrder(t *testing.T, out string) []string {
	t.Helper()

	var steps []string
	for rest := out; ; {
		at := strings.Index(rest, `<span class="ev-name">`)
		if at < 0 {
			return steps
		}
		rest = rest[at+len(`<span class="ev-name">`):]
		end := strings.Index(rest, "<")
		steps = append(steps, rest[:end])
	}
}

// The two axes reverse independently, and the zero TraceOrder is the current
// default: everything oldest first.
func TestLedgerOrderIsConfigurablePerAxis(t *testing.T) {
	// Two traces, two records each: trace "old" ran first.
	views := []RecordView{
		withTrace(view(traceOld, at(41, 835), "A1", "a1", "{}", ""), traceOld),
		withTrace(view(traceOld, at(41, 902), "A2", "a2", "{}", ""), traceOld),
		withTrace(view(traceNew, at(43, 120), "B1", "b1", "{}", ""), traceNew),
		withTrace(view(traceNew, at(43, 221), "B2", "b2", "{}", ""), traceNew),
	}

	cases := []struct {
		name  string
		order TraceOrder
		want  []string
	}{
		{"default is oldest first on both axes", TraceOrder{}, []string{"A1", "A2", "B1", "B2"}},
		{"newest trace first keeps each trace reading forwards", TraceOrder{NewestTracesFirst: true}, []string{"B1", "B2", "A1", "A2"}},
		{"newest events first keeps the blocks in order", TraceOrder{NewestEventsFirst: true}, []string{"A2", "A1", "B2", "B1"}},
		{"both reversed reads the whole ledger backwards", TraceOrder{NewestTracesFirst: true, NewestEventsFirst: true}, []string{"B2", "B1", "A2", "A1"}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := stepOrder(t, renderOrdered(t, views, c.order))
			if strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Errorf("order = %v, want %v", got, c.want)
			}
		})
	}
}

// A trace that comes back after another trace wrote in between opens a second
// block. Pulling those later records back up next to the earlier ones would put
// the page out of chronological order, which is the one thing the ledger is for.
func TestLedgerSplitsInterleavedTracesIntoRuns(t *testing.T) {
	views := []RecordView{
		withTrace(view(traceOld, at(41, 835), "A1", "a1", "{}", ""), traceOld),
		withTrace(view(traceNew, at(42, 100), "B1", "b1", "{}", ""), traceNew),
		// The first trace resumes - a retry, a callback, a background task.
		withTrace(view(traceOld, at(43, 305), "A2", "a2", "{}", ""), traceOld),
	}

	got := stepOrder(t, renderOrdered(t, views, TraceOrder{}))
	if want := "A1,B1,A2"; strings.Join(got, ",") != want {
		t.Errorf("order = %v, want %s", got, want)
	}

	// Three runs, not two traces: the resumed trace is its own block.
	out := renderOrdered(t, views, TraceOrder{})
	if !strings.Contains(out, "3 traces &middot; 3 events") {
		t.Errorf("resumed trace was not counted as its own block:\n%s", out)
	}
	if blocks := strings.Count(out, `<div class="trace-head">`); blocks != 3 {
		t.Errorf("rendered %d trace blocks, want 3:\n%s", blocks, out)
	}
}

// Records arriving out of order are put back on the timeline before the runs are
// cut, so an unsorted input renders the same page as a sorted one.
func TestLedgerSortsBeforeGrouping(t *testing.T) {
	shuffled := []RecordView{
		withTrace(view(traceOld, at(43, 305), "A2", "a2", "{}", ""), traceOld),
		withTrace(view(traceOld, at(41, 835), "A1", "a1", "{}", ""), traceOld),
		withTrace(view(traceNew, at(42, 100), "B1", "b1", "{}", ""), traceNew),
	}

	got := stepOrder(t, renderOrdered(t, shuffled, TraceOrder{}))
	if want := "A1,B1,A2"; strings.Join(got, ",") != want {
		t.Errorf("order = %v, want %s", got, want)
	}
}

// Reversing a run of an interleaved ledger flips the records inside each block
// and the blocks themselves, without merging the two runs of the same trace.
func TestLedgerReversedInterleavedTracesKeepTheirRuns(t *testing.T) {
	views := []RecordView{
		withTrace(view(traceOld, at(41, 835), "A1", "a1", "{}", ""), traceOld),
		withTrace(view(traceOld, at(41, 902), "A2", "a2", "{}", ""), traceOld),
		withTrace(view(traceNew, at(42, 100), "B1", "b1", "{}", ""), traceNew),
		withTrace(view(traceOld, at(43, 305), "A3", "a3", "{}", ""), traceOld),
	}

	got := stepOrder(t, renderOrdered(t, views, TraceOrder{NewestTracesFirst: true, NewestEventsFirst: true}))
	if want := "A3,B1,A2,A1"; strings.Join(got, ",") != want {
		t.Errorf("order = %v, want %s", got, want)
	}
}

// The trace header and the ledger heading describe the trace, not the direction
// it is read in: a reversed block reports the same date and the same span.
func TestLedgerSummariesAreIndependentOfTheReadingDirection(t *testing.T) {
	views := []RecordView{
		withTrace(view(traceOld, at(41, 835), "A1", "a1", "{}", ""), traceOld),
		withTrace(view(traceOld, at(43, 305), "A2", "a2", "{}", ""), traceOld),
	}

	forwards := renderOrdered(t, views, TraceOrder{})
	backwards := renderOrdered(t, views, TraceOrder{NewestTracesFirst: true, NewestEventsFirst: true})

	for _, want := range []string{"2 events", "1.47 s", "2026-09-01"} {
		if !strings.Contains(forwards, want) || !strings.Contains(backwards, want) {
			t.Errorf("summary %q differs with the reading direction:\n%s", want, backwards)
		}
	}
}
