package events

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	confluent "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/flachnetz/startup/v2/lib/events/avro"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/flachnetz/startup/v2/startup_tracing"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type eventSender struct {
	EventTypes *NormalizedEventTypes

	// can be nil to not send to kafka
	KafkaSender *kafka.Producer

	// can be nil to not write to a file
	FileSender io.WriteCloser

	// async events are queued
	AsyncBufferCh chan Event

	// schema cache
	SchemaIdCache map[reflect.Type]uint32

	// set to true to disable avro encoding
	NoAvro bool

	// the table to write events to
	OutboxTable string

	// wait group to wait for pending background tasks on close
	wg sync.WaitGroup

	// guards Close so it is idempotent (close(chan) panics if called twice)
	closeOnce sync.Once
}

func NewInitializer(
	confluentClient confluent.Client,
	kafkaSender *kafka.Producer,
	fileSender io.WriteCloser,
	eventTopics EventTopics,
	outboxTable string,
	bufferSize uint,
) (EventSenderInitializer, error) {
	if bufferSize == 0 {
		// use default value for buffer size
		bufferSize = 1024
	}

	asyncBufferCh := make(chan Event, bufferSize)

	// normalize event topics to fix any issues with pointer/non pointer types
	eventTopicsNormalized, err := eventTopics.Normalized()
	if err != nil {
		return nil, fmt.Errorf("normalize event topics: %w", err)
	}

	eventSender := &eventSender{
		KafkaSender:   kafkaSender,
		FileSender:    fileSender,
		EventTypes:    eventTopicsNormalized,
		AsyncBufferCh: asyncBufferCh,
	}

	eventSender.launchAsyncTasks()

	eventSenderInitializer := &eventSenderInitializer{
		ConfluentClient: confluentClient,
		EventTopics:     eventTopicsNormalized,
		OutboxTable:     outboxTable,
		eventSender:     eventSender,
	}

	return eventSenderInitializer, nil
}

func (ev *eventSender) SendAsync(ctx context.Context, event Event) {
	event = &eventWithContext{Context: ctx, Event: event}

	if trace.SpanContextFromContext(ctx).IsValid() {
		// This is a very short trace, just for "send this event"
		_ = startup_tracing.Trace(ctx, "Send"+avro.EventTypeOf(event)+"Async", func(ctx context.Context, span trace.Span) error {
			event = addTraceContextToEvent(ctx, event)
			event = &eventWithContext{Context: ctx, Event: event}
			return nil
		})
	}

	select {
	case <-ctx.Done():
	case ev.AsyncBufferCh <- event:
	default:
		slog.WarnContext(ctx, "Async event queue is full, discarding event", slog.String("event", eventToString(event)))
	}
}

func (ev *eventSender) SendInTx(ctx context.Context, tx sqlx.ExecerContext, event Event) error {
	return startup_tracing.Trace(ctx, "Send"+avro.EventTypeOf(event), func(ctx context.Context, span trace.Span) error {
		if ev.NoAvro {
			slog.WarnContext(ctx, "Will not write event to outbox, avro is disabled", slog.Any("event", event))

			// still serialize event to ensure it is well-defined
			return event.Serialize(io.Discard)
		}

		event = addTraceContextToEvent(ctx, event)
		event = &eventWithContext{Context: ctx, Event: event}

		meta, avro, err := ev.encodeAvro(event)
		if err != nil {
			return fmt.Errorf("encode event: %w", err)
		}

		return WriteToOutbox(ctx, tx, *meta, ev.OutboxTable, avro)
	})
}

func (ev *eventSender) Close() error {
	ev.closeOnce.Do(func() {
		close(ev.AsyncBufferCh)
		ev.wg.Wait()
	})
	return nil
}

func (ev *eventSender) launchAsyncTasks() {
	ev.wg.Go(func() {
		defer func() {
			if ev.KafkaSender != nil {
				for {
					count := ev.KafkaSender.Flush(5_000)
					if count == 0 {
						break
					}

					slog.Warn("Flush says there are still queued messages to be send.", slog.Int("count", count))
				}

				ev.KafkaSender.Close()
			}
		}()

		for event := range ev.AsyncBufferCh {
			ev.doSendAsync(event)
		}
	})

	if ev.KafkaSender != nil {
		ev.wg.Go(func() {
			for e := range ev.KafkaSender.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						slog.Warn(
							"Event delivery failed",
							slog.Any("topicPartition", ev.TopicPartition),
							slog.String("key", string(ev.Key)),
							sl.Error(ev.TopicPartition.Error),
						)
					}
				}
			}
		})
	}
}

func (ev *eventSender) doSendAsync(event Event) {
	ctx := contextOf(event)

	// ignore error as we're in the process of sending an async
	if err := ev.writeToFile(event); err != nil {
		eventType := avro.EventTypeOf(event)
		slog.WarnContext(ctx, "Failed to write async event to file", slog.String("type", eventType), sl.Error(err))
	}

	if err := ev.sendToKafka(event); err != nil {
		eventType := avro.EventTypeOf(event)
		slog.WarnContext(ctx, "Failed to send async event to kafka", slog.String("type", eventType), sl.Error(err))
	}

	if ev.FileSender == nil && ev.KafkaSender == nil {
		// serialize event just to check for the error
		err := event.Serialize(io.Discard)
		if err != nil {
			eventType := avro.EventTypeOf(event)
			slog.WarnContext(ctx, "Failed to send async event to kafka", slog.String("type", eventType), sl.Error(err))
		}
	}
}

func (ev *eventSender) writeToFile(event Event) error {
	if ev.FileSender == nil {
		return nil
	}

	buf, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}

	_, err = ev.FileSender.Write(bytes.TrimSpace(buf))
	if err != nil {
		return fmt.Errorf("write to file: %w", err)
	}

	_, err = ev.FileSender.Write([]byte("\n"))
	if err != nil {
		return fmt.Errorf("write to file: %w", err)
	}

	return nil
}

func (ev *eventSender) sendToKafka(event Event) error {
	if ev.KafkaSender == nil {
		return nil
	}

	meta, avro, err := ev.encodeAvro(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &meta.Topic,
			Partition: kafka.PartitionAny,
		},
		Key:     byteSliceOf(meta.Key),
		Headers: meta.Headers.ToKafka(),
		Value:   avro,
	}

	for ev.KafkaSender.Len() > 64*1024 {
		// wait until some messages are delivered before
		// pushing more messages to kafka.
		time.Sleep(100 * time.Millisecond)
	}

	for {
		err := ev.KafkaSender.Produce(message, nil)
		if err == nil {
			break
		}

		if err, ok := err.(kafka.Error); ok {
			// if the internal queue is full, we block a moment and then try again
			if err.Code() == kafka.ErrQueueFull {
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		return fmt.Errorf("kafka produce: %w", err)
	}

	return nil
}

func (ev *eventSender) encodeAvro(event Event) (*EventMetadata, []byte, error) {
	meta, err := ev.EventTypes.MetadataOf(event)
	if err != nil {
		return nil, nil, fmt.Errorf("lookup event metadata: %w", err)
	}

	schemaId, ok := ev.SchemaIdCache[meta.Type]
	if !ok {
		return nil, nil, fmt.Errorf("no schema found for %q", meta.Type)
	}

	buf, err := avro.SerializeWithSchemaId(schemaId, event)
	if err != nil {
		return nil, nil, fmt.Errorf("serialize event: %w", err)
	}

	return meta, buf, nil
}

func eventToString(event Event) string {
	buf, _ := json.Marshal(event)
	return strings.TrimSpace(string(buf))
}

func byteSliceOf(value *string) []byte {
	if value == nil {
		return nil
	}

	return []byte(*value)
}

type mapTextMapCarrier map[string]string

func (m mapTextMapCarrier) Get(key string) string {
	return m[key]
}

func (m mapTextMapCarrier) Set(key, value string) {
	m[key] = value
}

func (m mapTextMapCarrier) Keys() []string {
	return slices.Collect(maps.Keys(m))
}

type eventWithTraceContext struct {
	TraceContext map[string]string
	Event
}

func (e *eventWithTraceContext) Unwrap() Event {
	return e.Event
}

func addTraceContextToEvent(ctx context.Context, event Event) Event {
	// capture the trace context for propagation
	carrier := mapTextMapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	if len(carrier) > 0 {
		// update the event and add the context
		event = &eventWithTraceContext{
			TraceContext: carrier,
			Event:        event,
		}
	}

	return event
}

type eventWithContext struct {
	Context context.Context
	Event
}

func (e *eventWithContext) Unwrap() Event {
	return e.Event
}

func contextOf(event Event) context.Context {
	ev, ok := asEventType[*eventWithContext](event)
	if ok {
		return ev.Context
	}

	return context.Background()
}
