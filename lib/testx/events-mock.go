package testx

import (
	"context"
	"io"
	"slices"
	"sync"
	"testing"

	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// CaptureEvents replaces the global events sender with a MockEvents that records all
// sent events and returns it. The previous sender is restored on test cleanup. It
// panics if events are already being captured, since the global sender cannot be
// shared across concurrent tests.
func CaptureEvents(t *testing.T) *MockEvents {
	if _, ok := events.Sender.(*MockEvents); ok {
		panic("CaptureEvents does not support concurrent capturing")
	}

	prevSender := events.Sender
	t.Cleanup(func() { events.Sender = prevSender })

	mock := &MockEvents{Testing: t}
	events.Sender = mock

	return mock
}

// MockEvents is an events.Sender that records events instead of dispatching them.
// Events sent transactionally are recorded only after the transaction commits.
type MockEvents struct {
	Testing *testing.T
	events  []events.Event
	lock    sync.Mutex
}

// Events returns a copy of the recorded events.
func (m *MockEvents) Events() []events.Event {
	m.lock.Lock()
	defer m.lock.Unlock()
	return slices.Clone(m.events)
}

// SendAsync serializes the event (failing the test on error) and records it
// immediately.
func (m *MockEvents) SendAsync(_ context.Context, event events.Event) {
	err := event.Serialize(io.Discard)
	require.NoError(m.Testing, err)

	m.lock.Lock()
	m.events = append(m.events, event)
	m.lock.Unlock()
}

// SendInTx serializes the event (failing the test on error) and records it once the
// transaction in ctx commits.
func (m *MockEvents) SendInTx(ctx context.Context, _ sqlx.ExecerContext, event events.Event) error {
	err := event.Serialize(io.Discard)
	require.NoError(m.Testing, err)

	ql.TxContextFromContext(ctx).OnCommit(func() {
		m.lock.Lock()
		m.events = append(m.events, event)
		m.lock.Unlock()
	})

	return nil
}

// Close is a no-op that satisfies the events.Sender interface.
func (m *MockEvents) Close() error {
	return nil
}

// MockEventsGetSingle returns the single recorded event of type T matching all
// predicates, failing the test unless exactly one such event exists.
func MockEventsGetSingle[T events.Event](t *testing.T, m *MockEvents, predicates ...func(T) bool) T {
	t.Helper()
	all := MockEventsGetAll[T](t, m, predicates...)
	require.Equalf(t, 1, len(all), "expected exactly one event of type %T, got %d", *new(T), len(all))
	return all[0]
}

// MockEventsGetAll returns all recorded events of type T that match every predicate,
// in the order they were recorded.
func MockEventsGetAll[T events.Event](t *testing.T, m *MockEvents, predicates ...func(T) bool) []T {
	t.Helper()
	var res []T

	for _, ev := range m.Events() {
		e, ok := events.WithKey(ev, "").Event.(T)
		if !ok {
			continue
		}

		if matchesAll(e, predicates) {
			res = append(res, e)
		}
	}

	return res
}

// MockEventsHasNone fails the test if any recorded event of type T matches all
// predicates.
func MockEventsHasNone[T events.Event](t *testing.T, m *MockEvents, predicates ...func(T) bool) {
	t.Helper()
	all := MockEventsGetAll[T](t, m, predicates...)
	require.Emptyf(t, all, "expected no events of type %T matching predicate, got %d", *new(T), len(all))
}

// MockEventsHasSome returns the recorded events of type T matching all predicates,
// failing the test if there are none.
func MockEventsHasSome[T events.Event](t *testing.T, m *MockEvents, predicates ...func(T) bool) []T {
	t.Helper()
	all := MockEventsGetAll[T](t, m, predicates...)
	require.NotEmptyf(t, all, "expected at least one event of type %T matching predicate", *new(T))
	return all
}

func matchesAll[T any](value T, predicates []func(T) bool) bool {
	for _, p := range predicates {
		if !p(value) {
			return false
		}
	}
	return true
}
