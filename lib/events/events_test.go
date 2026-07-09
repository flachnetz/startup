package events

import (
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testEvent is a minimal Event implementation for testing.
type testEvent struct {
	Name string
}

func (e *testEvent) Schema() string { return `{"type":"record","name":"testEvent","fields":[]}` }
func (e *testEvent) Serialize(w io.Writer) error {
	_, err := w.Write([]byte("test"))
	return err
}

// anotherEvent is a second event type for multi-topic tests.
type anotherEvent struct {
	Value int
}

func (e *anotherEvent) Schema() string { return `{"type":"record","name":"anotherEvent","fields":[]}` }

func (e *anotherEvent) Serialize(w io.Writer) error {
	_, err := w.Write([]byte("another"))
	return err
}

// --- EventTopics ---

func TestEventTopics_Topics(t *testing.T) {
	et := EventTopics{
		EventTypes: map[reflect.Type]Topic{
			reflect.TypeFor[testEvent]():    {Name: "topic-a"},
			reflect.TypeFor[anotherEvent](): {Name: "topic-b"},
		},
	}

	topics := et.Topics()
	assert.Len(t, topics, 2)

	names := map[string]bool{}
	for _, tp := range topics {
		names[tp.Name] = true
	}
	assert.True(t, names["topic-a"])
	assert.True(t, names["topic-b"])
}

func TestEventTopics_Normalized(t *testing.T) {
	// pointer type should be normalized to struct type
	et := EventTopics{
		EventTypes: map[reflect.Type]Topic{
			reflect.TypeFor[*testEvent](): {Name: "topic-a"},
		},
	}

	norm, err := et.Normalized()
	require.NoError(t, err)

	_, ok := norm.EventTypes[reflect.TypeFor[testEvent]()]
	assert.True(t, ok, "should be indexed by struct type, not pointer type")
}

func TestEventTopics_Normalized_RejectsNonEvent(t *testing.T) {
	et := EventTopics{
		EventTypes: map[reflect.Type]Topic{
			reflect.TypeFor[string](): {Name: "topic-a"},
		},
	}

	// derefEventType panics for types that don't implement Event
	assert.Panics(t, func() {
		_, _ = et.Normalized()
	})
}

func TestNormalizedEventTypes_TopicForType(t *testing.T) {
	et := EventTopics{
		EventTypes: map[reflect.Type]Topic{
			reflect.TypeFor[testEvent](): {Name: "my-topic"},
		},
	}

	norm, err := et.Normalized()
	require.NoError(t, err)

	topic, err := norm.TopicForType(reflect.TypeFor[testEvent]())
	require.NoError(t, err)
	assert.Equal(t, "my-topic", topic)

	// pointer type should also resolve
	topic, err = norm.TopicForType(reflect.TypeFor[*testEvent]())
	require.NoError(t, err)
	assert.Equal(t, "my-topic", topic)

	// unknown type returns error
	_, err = norm.TopicForType(reflect.TypeFor[anotherEvent]())
	assert.Error(t, err)
}

// --- KafkaEvent ---

func TestWithKey(t *testing.T) {
	ev := &testEvent{Name: "hello"}
	ke := WithKey(ev, "my-key", EventHeader{Key: "h1", Value: "v1"})

	assert.Equal(t, "my-key", ke.Key)
	assert.Equal(t, "", ke.Topic)
	assert.Equal(t, ev, ke.Unwrap())
	require.Len(t, ke.Headers, 1)
	assert.Equal(t, "h1", ke.Headers[0].Key)
}

func TestWithKeyAndTopic(t *testing.T) {
	ev := &testEvent{Name: "hello"}
	ke := WithKeyAndTopic(ev, "k", "custom-topic", EventHeader{Key: "a", Value: "b"})

	assert.Equal(t, "k", ke.Key)
	assert.Equal(t, "custom-topic", ke.Topic)
	assert.Equal(t, ev, ke.Unwrap())
}

func TestWithKey_UpdatesExistingKafkaEvent(t *testing.T) {
	ev := &testEvent{Name: "hello"}
	ke1 := WithKey(ev, "key1", EventHeader{Key: "h1", Value: "v1"})
	ke2 := WithKey(ke1, "key2", EventHeader{Key: "h2", Value: "v2"})

	// should return the same wrapper, updated in place
	assert.Same(t, ke1, ke2)
	assert.Equal(t, "key2", ke2.Key)
	assert.Len(t, ke2.Headers, 2)
}

// --- MetadataOf ---

func TestMetadataOf_PlainEvent(t *testing.T) {
	et := EventTopics{
		EventTypes: map[reflect.Type]Topic{
			reflect.TypeFor[testEvent](): {Name: "topic-x"},
		},
	}
	norm, err := et.Normalized()
	require.NoError(t, err)

	meta, err := norm.MetadataOf(&testEvent{Name: "foo"})
	require.NoError(t, err)

	assert.Equal(t, "topic-x", meta.Topic)
	assert.Equal(t, reflect.TypeFor[testEvent](), meta.Type)
	assert.Nil(t, meta.Key)
	assert.Empty(t, meta.Headers)
}

func TestMetadataOf_KafkaEvent(t *testing.T) {
	et := EventTopics{
		EventTypes: map[reflect.Type]Topic{
			reflect.TypeFor[testEvent](): {Name: "topic-x"},
		},
	}
	norm, err := et.Normalized()
	require.NoError(t, err)

	ke := WithKeyAndTopic(&testEvent{}, "the-key", "", EventHeader{Key: "hk", Value: "hv"})
	meta, err := norm.MetadataOf(ke)
	require.NoError(t, err)

	assert.Equal(t, "topic-x", meta.Topic) // no override since topic is ""
	require.NotNil(t, meta.Key)
	assert.Equal(t, "the-key", *meta.Key)
	require.Len(t, meta.Headers, 1)
}

func TestMetadataOf_KafkaEventWithTopicOverride(t *testing.T) {
	et := EventTopics{
		EventTypes: map[reflect.Type]Topic{
			reflect.TypeFor[testEvent](): {Name: "topic-x"},
		},
	}
	norm, err := et.Normalized()
	require.NoError(t, err)

	ke := WithKeyAndTopic(&testEvent{}, "k", "override-topic")
	meta, err := norm.MetadataOf(ke)
	require.NoError(t, err)

	assert.Equal(t, "override-topic", meta.Topic)
}

func TestMetadataOf_UnknownEventType(t *testing.T) {
	et := EventTopics{
		EventTypes: map[reflect.Type]Topic{
			reflect.TypeFor[testEvent](): {Name: "topic-x"},
		},
	}
	norm, err := et.Normalized()
	require.NoError(t, err)

	_, err = norm.MetadataOf(&anotherEvent{})
	assert.Error(t, err)
}

// --- EventHeaders ---

func TestEventHeaders_ToKafka(t *testing.T) {
	headers := EventHeaders{
		{Key: "k1", Value: "v1"},
		{Key: "k2", Value: "v2"},
	}

	kh := headers.ToKafka()
	require.Len(t, kh, 2)
	assert.Equal(t, "k1", kh[0].Key)
	assert.Equal(t, []byte("v1"), kh[0].Value)
	assert.Equal(t, "k2", kh[1].Key)
	assert.Equal(t, []byte("v2"), kh[1].Value)
}

func TestEventHeaders_ToKafka_Nil(t *testing.T) {
	var headers EventHeaders
	assert.Nil(t, headers.ToKafka())
}

// --- Timestamp conversion ---

func TestTimeToEventTimestamp(t *testing.T) {
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	millis := TimeToEventTimestamp(ts)
	assert.Equal(t, ts.UnixMilli(), millis)
}

func TestFromEventTimestamp(t *testing.T) {
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	millis := ts.UnixMilli()

	restored := FromEventTimestamp(millis)
	assert.True(t, ts.Equal(restored))
}

func TestTimestampRoundTrip(t *testing.T) {
	original := time.Date(2025, 3, 10, 8, 30, 45, 123000000, time.UTC)
	millis := TimeToEventTimestamp(original)
	restored := FromEventTimestamp(millis)

	// millisecond precision round-trip
	assert.Equal(t, original.UnixMilli(), restored.UnixMilli())
}

// --- NoopEventSender ---

func TestNoopEventSender_SendAsync(t *testing.T) {
	sender := &NoopEventSender{}
	sender.SendAsync(t.Context(), &testEvent{Name: "test"})
	// should not panic or error
}

func TestNoopEventSender_SendInTx(t *testing.T) {
	sender := &NoopEventSender{}
	err := sender.SendInTx(t.Context(), nil, &testEvent{Name: "test"})
	assert.NoError(t, err)
}

func TestNoopEventSender_Close(t *testing.T) {
	sender := &NoopEventSender{}
	assert.NoError(t, sender.Close())
}

// --- derefEventType ---

func TestDerefEventType_Struct(t *testing.T) {
	tp := derefEventType(reflect.TypeFor[testEvent]())
	assert.Equal(t, reflect.TypeFor[testEvent](), tp)
}

func TestDerefEventType_Pointer(t *testing.T) {
	tp := derefEventType(reflect.TypeFor[*testEvent]())
	assert.Equal(t, reflect.TypeFor[testEvent](), tp)
}

func TestDerefEventType_DoublePointer(t *testing.T) {
	tp := derefEventType(reflect.TypeFor[**testEvent]())
	assert.Equal(t, reflect.TypeFor[testEvent](), tp)
}

func TestDerefEventType_PanicsForNonEvent(t *testing.T) {
	assert.Panics(t, func() {
		derefEventType(reflect.TypeFor[string]())
	})
}

// --- byteSliceOf ---

func TestByteSliceOf_Nil(t *testing.T) {
	assert.Nil(t, byteSliceOf(nil))
}

func TestByteSliceOf_Value(t *testing.T) {
	s := "hello"
	assert.Equal(t, []byte("hello"), byteSliceOf(&s))
}
