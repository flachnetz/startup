package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"time"
)

type Topics []Topic

type TopicsFunc func(replicationFactor int16) Topics

func EnsureTopics(client sarama.Client, topics Topics) error {
	createTopics := func(broker *sarama.Broker) error {
		// open connection asynchronously
		if err := broker.Open(client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
			return errors.WithMessage(err, "open broker connection")
		}

		// this one will wait for the actual connection.
		if _, err := broker.Connected(); err != nil {
			return errors.WithMessage(err, "open broker connection")
		}

		resp, err := broker.CreateTopics(&sarama.CreateTopicsRequest{
			Version:      1,
			TopicDetails: topics.details(),
			Timeout:      1 * time.Second,
		})

		if err != nil {
			return errors.WithMessage(err, "create topics request")
		}

		if resp != nil && len(resp.TopicErrors) > 0 {
			logrus.Warnf("there was at least one error during topic creation, ignoring for now: %+v", resp.TopicErrors)
		}

		return nil
	}

	var err error
	broker, err := client.Controller()
	if err != nil {
		broker, err = client.RefreshController()
		if err == nil {
			return errors.WithMessage(err, "cannot get broker controller, trying each broker now to create topics")
		}
	}

	return createTopics(broker)
}

type Topic struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	Config            map[string]*string
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
			ConfigEntries:     topic.Config,
		}
	}

	return details
}
