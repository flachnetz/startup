package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	confluent "github.com/Landoop/schema-registry"
	rdkafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type EventSenderInitializer interface {
	Close() error
	Initialize() (EventSender, error)
}

type eventSenderInitializer struct {
	ConfluentClient *confluent.Client
	EventTopics     *NormalizedEventTypes

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
		schemaId, err := esi.ConfluentClient.RegisterNewSchema(nameOf(event), event.Schema())
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

	var topicSpecifications []rdkafka.TopicSpecification
	topicSeen := map[string]bool{}

	for _, topic := range topics {
		if topicSeen[topic.Name] {
			continue
		}

		config := map[string]string{}
		for k, v := range topic.Config {
			if v != nil {
				config[k] = *v
			}
		}

		topicSpecifications = append(topicSpecifications, rdkafka.TopicSpecification{
			Topic:             topic.Name,
			NumPartitions:     int(topic.NumPartitions),
			ReplicationFactor: int(topic.ReplicationFactor),
			Config:            config,
		})

		topicSeen[topic.Name] = true
	}

	log.Info("Creating kafka topics", slog.Int("count", len(topics)))
	results, err := adminClient.CreateTopics(context.Background(), topicSpecifications)

	// check results first
	for _, result := range results {
		switch result.Error.Code() {
		case rdkafka.ErrNoError:
			log.Info("Kafka topic created", slog.String("topic", result.Topic))

		case rdkafka.ErrTopicAlreadyExists:
			log.Info("Kafka topic already exists", slog.String("topic", result.Topic))

		default:
			log.Warn("Failed to create topic", slog.String("topic", result.Topic), slog.String("error", result.Error.String()))

			if err == nil {
				err = errors.New("one or more topics could not be created")
			}
		}
	}

	// and then fail if we have any kind of error
	if err != nil {
		return fmt.Errorf("topic creation: %w", err)
	}

	return nil
}
