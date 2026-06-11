package testx

import (
	"context"
	"io"
	"testing"

	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

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

type MockEvents struct {
	Testing *testing.T
	Events  []events.Event
}

func (m *MockEvents) SendAsync(_ context.Context, event events.Event) {
	err := event.Serialize(io.Discard)
	require.NoError(m.Testing, err)

	m.Events = append(m.Events, event)
}

func (m *MockEvents) SendInTx(ctx context.Context, _ sqlx.ExecerContext, event events.Event) error {
	err := event.Serialize(io.Discard)
	require.NoError(m.Testing, err)

	ql.TxContextFromContext(ctx).OnCommit(func() {
		m.Events = append(m.Events, event)
	})

	return nil
}

func (m *MockEvents) Close() error {
	return nil
}

func MockEventsGetSingle[T events.Event](t *testing.T, m *MockEvents, predicates ...func(T) bool) T {
	t.Helper()
	all := MockEventsGetAll[T](t, m, predicates...)
	require.Equalf(t, 1, len(all), "expected exactly one event of type %T, got %d", *new(T), len(all))
	return all[0]
}

func MockEventsGetAll[T events.Event](t *testing.T, m *MockEvents, predicates ...func(T) bool) []T {
	t.Helper()
	var res []T

	for _, ev := range m.Events {
		e, ok := events.ToKafkaEvent("", ev).Event.(T)
		if !ok {
			continue
		}

		if matchesAll(e, predicates) {
			res = append(res, e)
		}
	}

	return res
}

func MockEventsHasNone[T events.Event](t *testing.T, m *MockEvents, predicates ...func(T) bool) {
	t.Helper()
	all := MockEventsGetAll[T](t, m, predicates...)
	require.Emptyf(t, all, "expected no events of type %T matching predicate, got %d", *new(T), len(all))
}

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
