package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	rdkafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	confluent "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/flachnetz/startup/v2/lib/events/avro"
	"github.com/flachnetz/startup/v2/startup_kafka"
)

type EventSenderInitializer interface {
	Close() error
	Initialize() (EventSender, error)
}

type eventSenderInitializer struct {
	ConfluentClient confluent.Client
	EventTopics     *NormalizedEventTypes
	OutboxTable     string

	eventSender *eventSender
}

// Close closes the initializer. This does nothing if the event sender
// is already initialized and can be used with `defer`.
func (esi *eventSenderInitializer) Close() error {
	if esi.eventSender == nil {
		return nil
	}

	return esi.eventSender.Close()
}

func (esi *eventSenderInitializer) Initialize() (EventSender, error) {
	if esi.eventSender == nil {
		return nil, errors.New("event sender already initialized")
	}

	// create kafka topics if the event sender has a kafka sender
	if esi.eventSender.KafkaSender != nil {
		if err := esi.createKafkaTopics(); err != nil {
			return nil, fmt.Errorf("create kafka topics: %w", err)
		}
	}

	schemaIdCache, err := esi.registerSchemaCache()
	if err != nil {
		return nil, fmt.Errorf("register confluent schemas: %w", err)
	}

	// mark the event sender as initialized
	eventSender := esi.eventSender
	eventSender.SchemaIdCache = schemaIdCache
	eventSender.NoAvro = schemaIdCache == nil
	eventSender.OutboxTable = esi.OutboxTable

	// and remove it from this initializer
	esi.eventSender = nil

	log.Info("Event sender initialized")

	return eventSender, nil
}

func (esi *eventSenderInitializer) registerSchemaCache() (map[reflect.Type]uint32, error) {
	if esi.eventSender.KafkaSender != nil && esi.ConfluentClient == nil {
		return nil, fmt.Errorf("confluent url must be defined if kafka is enabled")
	}

	if esi.ConfluentClient == nil {
		// skip schema registration
		return nil, nil
	}

	log.Info("Registering event schemas in confluent registry")

	schemaIdCache := map[reflect.Type]uint32{}

	for eventType := range esi.EventTopics.EventTypes {
		// create a new empty event
		event := reflect.New(eventType).Interface().(Event)

		// register the schema with confluent
		schemaInfo := confluent.SchemaInfo{Schema: event.Schema()}
		schemaId, err := esi.ConfluentClient.Register(avro.EventTypeOf(event), schemaInfo, true)
		if err != nil {
			return nil, fmt.Errorf("register schema for event type %q: %w", eventType, err)
		}

		// and cache the schema id for serializing later
		schemaIdCache[eventType] = uint32(schemaId)
	}

	return schemaIdCache, nil
}

func (esi *eventSenderInitializer) createKafkaTopics() error {
	topics := esi.EventTopics.Topics()
	if len(topics) == 0 {
		return nil
	}

	adminClient, err := rdkafka.NewAdminClientFromProducer(esi.eventSender.KafkaSender)
	if err != nil {
		return fmt.Errorf("admin client: %w", err)
	}

	defer func() { go adminClient.Close() }()

	log.Info("Creating kafka topics", slog.Int("count", len(topics)))
	return startup_kafka.CreateTopics(context.Background(), adminClient, topics)
}
