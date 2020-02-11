package kafka

import (
	"sync"

	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/flachnetz/startup/lib/schema"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type kafkaRegistry struct {
	consumer sarama.Consumer

	lock        sync.Mutex
	schemaCache map[string]string

	log *logrus.Entry

	wg           sync.WaitGroup
	consumerLock sync.Mutex
	consumers    []sarama.PartitionConsumer
	producer     sarama.SyncProducer

	schemaTopic string
}

func NewSchemaRegistry(kafkaClient sarama.Client, topic string, replicationFactor int16) (schema.Registry, error) {
	// ensure that the topic exists
	err := EnsureTopics(kafkaClient, Topics{
		Topic{
			Name:              topic,
			ReplicationFactor: replicationFactor,
			NumPartitions:     1,
		},
	})

	if err != nil {
		return nil, errors.WithMessage(err, "create topic")
	}

	consumer, err := sarama.NewConsumerFromClient(kafkaClient)
	if err != nil {
		return nil, errors.WithMessage(err, "Cannot create kafka consumer")
	}

	producer, err := sarama.NewSyncProducerFromClient(kafkaClient)
	if err != nil {
		return nil, errors.WithMessage(err, "Cannot create kafka producer")
	}

	registry := kafkaRegistry{
		consumer:    consumer,
		schemaCache: make(map[string]string),
		log:         logrus.WithField("prefix", "kafka-schema-registry"),
		consumers:   []sarama.PartitionConsumer{},
		producer:    producer,
		schemaTopic: topic,
	}

	err = registry.createPartitionConsumer(topic)
	if err != nil {
		_ = registry.Close()
		return nil, errors.WithMessage(err, "create partition consumer")
	}

	return &registry, nil
}

func (r *kafkaRegistry) Set(schemaString string) (string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	key := schema.Hash(schemaString)
	if _, ok := r.schemaCache[key]; ok {
		return key, nil
	}

	_, _, err := r.producer.SendMessage(&sarama.ProducerMessage{
		Topic: r.schemaTopic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(schemaString),
	})

	if err != nil {
		return "", errors.Wrapf(err, "write schema %s to topic %s", schemaString, r.schemaTopic)
	}

	r.schemaCache[key] = schemaString
	return key, nil
}

func (r *kafkaRegistry) Init(schemas []string) (map[string]string, error) {
	panic("implement me")
}


func (r *kafkaRegistry) Get(key string) (string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if cached, ok := r.schemaCache[key]; ok {
		return cached, nil
	}

	return "", errors.Errorf("Schema for key not found: %s", key)
}

func (r *kafkaRegistry) Close() error {
	r.log.Info("waiting for all partition consumers to close")

	r.consumerLock.Lock()
	for _, c := range r.consumers {
		c.AsyncClose()
	}
	r.consumerLock.Unlock()

	r.wg.Wait()
	errConsumer := r.consumer.Close()
	errProducer := r.producer.Close()

	if errProducer != nil || errConsumer != nil {
		return errors.Errorf("cannot close kafka-registry %v %v", errConsumer, errProducer)
	}

	return nil
}

func (r *kafkaRegistry) createPartitionConsumer(topic string) error {
	partitions, err := r.consumer.Partitions(topic)
	if err != nil {
		return errors.Errorf("cannot fetch partitions for topic %s", topic)
	}

	r.log.Infof("Creating %d partition consumers for topic %s", len(partitions), topic)

	for _, partitionIndex := range partitions {
		pc, err := r.consumer.ConsumePartition(topic, partitionIndex, sarama.OffsetOldest)
		if err != nil {
			return errors.Wrapf(err, "init partition consumer for topic %s", topic)
		}

		r.consumerLock.Lock()
		r.consumers = append(r.consumers, pc)
		r.consumerLock.Unlock()

		r.wg.Add(1)

		go func() {
			defer r.wg.Done()

			for {
				select {
				case msg, ok := <-pc.Messages():
					if !ok {
						return
					}

					schemaName := guessSchemaName(msg)

					func() {
						r.lock.Lock()
						defer r.lock.Unlock()

						if _, exists := r.schemaCache[string(msg.Key)]; !exists {
							r.log.Debugf("Adding new schema %s to internal registry", schemaName)
							r.schemaCache[string(msg.Key)] = string(msg.Value)

						} else {
							r.log.Debugf("Skipping schema with hash %s, already known", schemaName)
						}
					}()

				case err := <-pc.Errors():
					if err != nil {
						r.log.WithError(err).Warn("an error occurred while consuming messages")
					}
				}
			}
		}()
	}

	return nil
}

func guessSchemaName(message *sarama.ConsumerMessage) string {
	var decoded map[string]interface{}

	if err := json.Unmarshal(message.Value, &decoded); err == nil {
		if schemaType, _ := decoded["type"].(string); schemaType == "record" {
			if schemaName, ok := decoded["name"].(string); ok {
				return string(message.Key) + " (" + schemaName + ")"
			}
		}
	}

	return string(message.Key)
}
