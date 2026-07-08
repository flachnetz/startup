package events_test

import (
	"context"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/lib/testx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type integrationEvent struct {
	Message string
}

func (e *integrationEvent) Schema() string {
	return `{"type":"record","name":"integrationEvent","fields":[{"name":"message","type":"string"}]}`
}

func (e *integrationEvent) Serialize(w io.Writer) error {
	// write a simple payload (not real avro, but enough for the test)
	_, err := w.Write([]byte(e.Message))
	return err
}

func TestSendAsync_DeliversToKafka(t *testing.T) {
	const topicName = "integration-test-topic"

	kc := testx.KafkaCluster(t)
	kc.CreateTopic(topicName, 1)

	registry := testx.MockConfluentRegistry(t)

	eventTopics := events.EventTopics{
		EventTypes: map[reflect.Type]events.Topic{
			reflect.TypeFor[integrationEvent](): {Name: topicName, NumPartitions: 1, ReplicationFactor: 1},
		},
	}

	producer := kc.Producer()

	initializer, err := events.NewInitializer(
		registry.Client(),
		producer,
		nil, // no file sender
		eventTopics,
		"", // no outbox table
		64,
	)
	require.NoError(t, err)
	defer initializer.Close()

	t.Log("Initialize")
	sender, err := initializer.Initialize()
	require.NoError(t, err)

	// send event with key and headers
	ev := events.WithKey(
		&integrationEvent{Message: "hello-kafka"},
		"my-key",
		events.EventHeader{Key: "X-Source", Value: "test-suite"},
		events.EventHeader{Key: "X-Trace", Value: "abc-123"},
	)

	t.Log("Send")
	sender.SendAsync(t.Context(), ev)

	// close flushes pending messages
	require.NoError(t, sender.Close())

	t.Log("Consume")
	// consume the message back
	consumer := kc.TestConsumer(topicName)
	msg := consumer.MessageTimeout(5 * time.Second)

	// verify key
	assert.Equal(t, "my-key", string(msg.Key))

	// verify topic
	assert.Equal(t, topicName, *msg.TopicPartition.Topic)

	// verify payload contains our message (prefixed with 5-byte confluent header)
	require.True(t, len(msg.Value) > 5, "payload should have confluent header + body")
	assert.Equal(t, byte(0), msg.Value[0], "first byte should be magic zero")
	assert.Equal(t, "hello-kafka", string(msg.Value[5:]))

	// verify headers
	headerMap := map[string]string{}
	for _, h := range msg.Headers {
		headerMap[h.Key] = string(h.Value)
	}
	assert.Equal(t, "test-suite", headerMap["X-Source"])
	assert.Equal(t, "abc-123", headerMap["X-Trace"])
}

func TestSendAsync_PropagatesTraceContext(t *testing.T) {
	const topicName = "trace-context-topic"

	kc := testx.KafkaCluster(t)
	kc.CreateTopic(topicName, 1)

	registry := testx.MockConfluentRegistry(t)

	eventTopics := events.EventTopics{
		EventTypes: map[reflect.Type]events.Topic{
			reflect.TypeFor[integrationEvent](): {Name: topicName, NumPartitions: 1, ReplicationFactor: 1},
		},
	}

	producer := kc.Producer()

	initializer, err := events.NewInitializer(
		registry.Client(),
		producer,
		nil,
		eventTopics,
		"",
		64,
	)
	require.NoError(t, err)
	defer initializer.Close()

	sender, err := initializer.Initialize()
	require.NoError(t, err)

	// install W3C trace context propagator so headers get injected
	prevPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prevPropagator) })

	// create a tracer and start a span to get a valid trace context
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, span := tp.Tracer("test").Start(t.Context(), "test-span")
	traceID := span.SpanContext().TraceID().String()
	span.End()

	// send with the traced context
	sender.SendAsync(ctx, &integrationEvent{Message: "traced"})
	require.NoError(t, sender.Close())

	// consume and verify
	consumer := kc.TestConsumer(topicName)
	msg := consumer.MessageTimeout(5 * time.Second)

	headerMap := map[string]string{}
	for _, h := range msg.Headers {
		headerMap[h.Key] = string(h.Value)
	}

	// the W3C propagator injects a "traceparent" header
	traceparent, ok := headerMap["traceparent"]
	require.True(t, ok, "expected traceparent header, got headers: %v", headerMap)
	assert.Contains(t, traceparent, traceID, "traceparent should contain the trace ID")
}
