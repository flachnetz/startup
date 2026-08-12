package events

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/flachnetz/startup/v2/lib/actor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func actorTopics(t *testing.T) *NormalizedEventTypes {
	t.Helper()

	et := EventTopics{
		EventTypes: map[reflect.Type]Topic{
			reflect.TypeFor[testEvent](): {Name: "topic-x"},
		},
	}

	norm, err := et.Normalized()
	require.NoError(t, err)

	return norm
}

func headerMap(headers EventHeaders) map[string]string {
	result := make(map[string]string, len(headers))
	for _, header := range headers {
		result[header.Key] = header.Value
	}

	return result
}

func TestMetadataOf_ActorHeadersFromContext(t *testing.T) {
	ctx := actor.WithActor(context.Background(), actor.Actor{
		Type:  actor.TypeUser,
		Id:    "0d0e5b1a-user",
		Label: "staff@example.com",
	})

	meta, err := actorTopics(t).MetadataOf(addActorToEvent(ctx, &testEvent{}))
	require.NoError(t, err)

	assert.Equal(t, map[string]string{
		HeaderActorType:  "user",
		HeaderActorId:    "0d0e5b1a-user",
		HeaderActorLabel: "staff@example.com",
	}, headerMap(meta.Headers))

	// the wrapper must not change the event type used for topic lookup
	assert.Equal(t, reflect.TypeFor[testEvent](), meta.Type)
	assert.Equal(t, "topic-x", meta.Topic)
}

func TestMetadataOf_NoActorHeadersWithoutActor(t *testing.T) {
	meta, err := actorTopics(t).MetadataOf(addActorToEvent(context.Background(), &testEvent{}))
	require.NoError(t, err)

	assert.Empty(t, meta.Headers)
}

func TestMetadataOf_LabelOmittedWhenEmpty(t *testing.T) {
	ctx := actor.WithActor(context.Background(), actor.Actor{Type: actor.TypeService, Id: "order-service"})

	meta, err := actorTopics(t).MetadataOf(addActorToEvent(ctx, &testEvent{}))
	require.NoError(t, err)

	assert.Equal(t, map[string]string{
		HeaderActorType: "service",
		HeaderActorId:   "order-service",
	}, headerMap(meta.Headers))
}

func TestMetadataOf_ActorAndTraceContextCoexist(t *testing.T) {
	ctx := actor.WithActor(context.Background(), actor.Actor{Type: actor.TypePlayer, Id: "01J-player"})

	event := addActorToEvent(ctx, &testEvent{})
	event = &eventWithTraceContext{TraceContext: map[string]string{"traceparent": "00-abc-def-01"}, Event: event}

	meta, err := actorTopics(t).MetadataOf(event)
	require.NoError(t, err)

	assert.Equal(t, map[string]string{
		"traceparent":   "00-abc-def-01",
		HeaderActorType: "player",
		HeaderActorId:   "01J-player",
	}, headerMap(meta.Headers))
	assert.Len(t, meta.Headers, 3, "no duplicated headers")
}

// capturingExecer records the arguments WriteToOutbox passes to the database.
type capturingExecer struct {
	args []any
}

func (e *capturingExecer) ExecContext(_ context.Context, _ string, args ...any) (sql.Result, error) {
	e.args = args
	return nil, nil
}

func TestWriteToOutbox_PersistsActorHeaders(t *testing.T) {
	ctx := actor.WithActor(context.Background(), actor.Actor{Type: actor.TypeUser, Id: "sub-1", Label: "a@b.c"})

	meta, err := actorTopics(t).MetadataOf(addActorToEvent(ctx, &testEvent{}))
	require.NoError(t, err)

	execer := &capturingExecer{}
	require.NoError(t, WriteToOutbox(ctx, execer, *meta, "bo_outbox", []byte("payload")))

	// stmt args are topic, key, value, header keys, header values
	require.Len(t, execer.args, 5)
	assert.Equal(t, []string{HeaderActorType, HeaderActorId, HeaderActorLabel}, execer.args[3])
	assert.Equal(t, []string{"user", "sub-1", "a@b.c"}, execer.args[4])
}
