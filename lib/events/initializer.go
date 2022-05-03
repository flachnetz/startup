package events

import (
	"context"
	confluent "github.com/Landoop/schema-registry"
	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"reflect"
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
			return nil, errors.WithMessage(err, "create kafka topics")
		}
	}

	schemaIdCache, err := esi.registerSchemaCache()
	if err != nil {
		return nil, errors.WithMessage(err, "register confluent schemas")
	}

	// mark the event sender as initialized
	eventSender := esi.eventSender
	eventSender.SchemaIdCache = schemaIdCache

	// and remove it from this initializer
	esi.eventSender = nil

	log.Infof("Event sender initialized")

	return eventSender, nil
}

func (esi *eventSenderInitializer) registerSchemaCache() (map[reflect.Type]uint32, error) {
	log.Infof("Registering event schemas in confluent registry")

	schemaIdCache := map[reflect.Type]uint32{}

	for eventType := range esi.EventTopics.EventTypes {
		// create a new empty event
		event := reflect.New(eventType).Interface().(Event)

		// register the schema with confluent
		schemaId, err := esi.ConfluentClient.RegisterNewSchema(nameOf(event), event.Schema())
		if err != nil {
			return nil, errors.WithMessagef(err, "register schema for event type '%s'", eventType)
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
		return errors.WithMessage(err, "admin client")
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

	log.Infof("Creating %d kafka topics", len(topics))
	results, err := adminClient.CreateTopics(context.Background(), topicSpecifications)

	// check results first
	for _, result := range results {
		switch result.Error.Code() {
		case rdkafka.ErrNoError:
			log.Infof("Kafka topic '%s' create", result.Topic)

		case rdkafka.ErrTopicAlreadyExists:
			log.Infof("Kafka topic '%s' already exists", result.Topic)

		default:
			log.Warnf("Failed to create topic '%s': %s", result.Topic, result.Error)

			if err == nil {
				err = errors.New("one or more topics could not be created")
			}
		}
	}

	// and then fail if we have any kind of error
	if err != nil {
		return errors.WithMessage(err, "topic creation")
	}

	return nil
}
