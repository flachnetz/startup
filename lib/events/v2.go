package events

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	confluent "github.com/Landoop/schema-registry"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
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

	// wait group to wait for pending background tasks on close
	wg sync.WaitGroup
}

func NewInitializer(
	confluentClient *confluent.Client,
	kafkaSender *kafka.Producer,
	fileSender io.WriteCloser,
	eventTopics EventTopics,
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
		return nil, errors.WithMessage(err, "normalize event topics")
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
		eventSender:     eventSender,
	}

	return eventSenderInitializer, nil
}

func (ev *eventSender) SendAsync(ctx context.Context, event Event) {
	select {
	case ev.AsyncBufferCh <- event:
	default:
		log.Warnf("Async event queue is full, discarding event %s", eventToString(event))
	}
}

func (ev *eventSender) SendAsyncCh() chan<- Event {
	return ev.AsyncBufferCh
}

func (ev *eventSender) SendInTx(ctx context.Context, tx sqlx.ExecerContext, event Event) error {
	meta, avro, err := ev.encodeAvro(event)
	if err != nil {
		return errors.WithMessage(err, "encode event")
	}

	return WriteToOutbox(ctx, tx, *meta, avro)
}

func (ev *eventSender) Close() error {
	close(ev.AsyncBufferCh)
	ev.wg.Wait()
	return nil
}

func (ev *eventSender) launchAsyncTasks() {
	ev.wg.Add(1)

	go func() {
		defer ev.wg.Done()

		defer func() {
			if ev.KafkaSender != nil {
				if count := ev.KafkaSender.Flush(5_000); count > 0 {
					log.Warnf("Flush says there are still %d queued messages to be send.", count)
				}
			}
		}()

		for event := range ev.AsyncBufferCh {
			ev.doSendAsync(event)
		}
	}()

	if ev.KafkaSender != nil {
		go func() {
			for e := range ev.KafkaSender.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						log.Warnf("Delivery to '%v' failed: %s", ev.TopicPartition, ev.TopicPartition.Error)
					}
				}
			}
		}()
	}
}

func (ev *eventSender) doSendAsync(event Event) {
	// ignore error as we're in the process of sending an async
	if err := ev.writeToFile(event); err != nil {
		log.Warnf("Failed to write async event to file: %s", err)
	}

	if err := ev.sendToKafka(event); err != nil {
		log.Warnf("Failed to send async event to kafka: %s", err)
	}
}

func (ev *eventSender) writeToFile(event Event) error {
	if ev.FileSender == nil {
		return nil
	}

	buf, err := json.Marshal(event)
	if err != nil {
		return errors.WithMessage(err, "marshal json")
	}

	_, err = ev.FileSender.Write(bytes.TrimSpace(buf))
	if err != nil {
		return errors.WithMessage(err, "write to file")
	}

	_, err = ev.FileSender.Write([]byte("\n"))
	if err != nil {
		return errors.WithMessage(err, "write to file")
	}

	return nil
}

func (ev *eventSender) sendToKafka(event Event) error {
	if ev.KafkaSender == nil {
		return nil
	}

	meta, avro, err := ev.encodeAvro(event)
	if err != nil {
		return errors.WithMessage(err, "encode event")
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

		return errors.WithMessage(err, "kafka produce")
	}

	return nil
}

func (ev *eventSender) encodeAvro(event Event) (*EventMetadata, []byte, error) {
	meta, err := ev.EventTypes.MetadataOf(event)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "lookup event metadata")
	}

	schemaId, ok := ev.SchemaIdCache[meta.Type]
	if !ok {
		return nil, nil, errors.Errorf("no schema found for '%s'", meta.Type)
	}

	avro, err := ev.eventToConfluentAvro(schemaId, event)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "serialize to avro")
	}

	return meta, avro, nil
}

func (ev *eventSender) eventToConfluentAvro(schemaId uint32, event Event) ([]byte, error) {
	// encode id in 4 bytes
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], schemaId)

	// write the magic byte followed by the 4 byte id.
	// see https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html
	// for a description of the format.
	var buffer bytes.Buffer
	buffer.WriteByte(0)
	buffer.Write(buf[:])

	// serialize the event
	if err := event.Serialize(&buffer); err != nil {
		return nil, errors.WithMessage(err, "encode avro event")
	}

	return buffer.Bytes(), nil
}

func eventToString(event Event) string {
	buf, _ := json.Marshal(event)
	return strings.TrimSpace(string(buf))
}

func byteSliceOf(value *string) []byte {
	if value == nil {
		return nil
	} else {
		return []byte(*value)
	}
}
