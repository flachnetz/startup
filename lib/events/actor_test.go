package events

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

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

func kafkaHeader(key, value string) kafka.Header {
	return kafka.Header{Key: key, Value: []byte(value)}
}

func TestActorFromKafkaHeaders(t *testing.T) {
	cases := []struct {
		name    string
		headers []kafka.Header
		want    actor.Actor
	}{
		{
			name: "staff member who caused the command upstream",
			headers: []kafka.Header{
				kafkaHeader(HeaderActorType, "user"),
				kafkaHeader(HeaderActorId, "sub-42"),
				kafkaHeader(HeaderActorLabel, "anna@example.com"),
			},
			want: actor.Actor{Type: actor.TypeUser, Id: "sub-42", Label: "anna@example.com"},
		},
		{
			name: "label is optional",
			headers: []kafka.Header{
				kafkaHeader(HeaderActorType, "service"),
				kafkaHeader(HeaderActorId, "order-service"),
			},
			want: actor.Actor{Type: actor.TypeService, Id: "order-service"},
		},
		{
			name:    "no actor headers means no actor",
			headers: []kafka.Header{kafkaHeader("traceparent", "00-abc-def-01")},
			want:    actor.Actor{},
		},
		{
			name:    "a type without an id names nobody",
			headers: []kafka.Header{kafkaHeader(HeaderActorType, "user")},
			want:    actor.Actor{},
		},
		{
			name:    "an id without a type still identifies the principal",
			headers: []kafka.Header{kafkaHeader(HeaderActorId, "sub-42")},
			want:    actor.Actor{Id: "sub-42"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ActorFromKafkaHeaders(tc.headers)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.want.Zero(), got.Zero())
		})
	}
}

// Round trip: what the producer side writes, the consumer side reads back.
func TestActorKafkaHeadersRoundTrip(t *testing.T) {
	a := actor.Actor{Type: actor.TypeUser, Id: "sub-1", Label: "staff@example.com"}

	headers := make([]kafka.Header, 0, len(actorHeaders(a)))
	for _, h := range actorHeaders(a) {
		headers = append(headers, kafkaHeader(h.Key, h.Value))
	}

	assert.Equal(t, a, ActorFromKafkaHeaders(headers))
}
