package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"time"
)

type Topics []Topic

type TopicsFunc func(replicationFactor int16) Topics

func EnsureTopics(client sarama.Client, topics Topics) error {
	var err error
	for _, broker := range client.Brokers() {
		// open connection asynchronously
		if err = broker.Open(client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
			err = errors.WithMessage(err, "open broker connection")
			continue
		}

		// this one will wait for the actual connection.
		if _, err = broker.Connected(); err != nil {
			err = errors.WithMessage(err, "open broker connection")
			continue
		}

		_, err = broker.CreateTopics(&sarama.CreateTopicsRequest{
			Version:      1,
			TopicDetails: topics.details(),
			Timeout:      1 * time.Second,
		})

		if err != nil {
			err = errors.WithMessage(err, "create topics request")
			continue
		}

		return nil
	}

	return errors.WithMessage(err, "creating topics")
}

type Topic struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
}

// Creates a TopicDetail map that can be used to create the topics on the
// kafka broker.
func (topics Topics) details() map[string]*sarama.TopicDetail {
	details := map[string]*sarama.TopicDetail{}

	for _, topic := range topics {
		replicationFactor := topic.ReplicationFactor
		if replicationFactor == 0 {
			replicationFactor = 1
		}

		partitionCount := topic.NumPartitions
		if partitionCount == 0 {
			partitionCount = 1
		}

		details[topic.Name] = &sarama.TopicDetail{
			NumPartitions:     partitionCount,
			ReplicationFactor: replicationFactor,
		}
	}

	return details
}
