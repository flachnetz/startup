package startup_kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"

	"github.com/flachnetz/startup/v2/lib/events/avro"
	"github.com/flachnetz/startup/v2/startup_base"
)

var topics = []string{"my_fancy"}

type Consumer struct {
	log *logrus.Entry

	consumer      *kafka.Consumer
	avroConverter *avro.Converter
	kafkaDone     chan bool
	wg            sync.WaitGroup

	running bool
}

type ConsumerOptions struct {
	KafkaAddresses     []string
	KafkaConsumerGroup string
	KafkaOffsetReset   string

	SchemaRegistry *avro.SchemaRegistry
}

func NewConsumer(options ConsumerOptions) *Consumer {
	logger := logrus.WithField("prefix", "kafka-consumer")

	c := &Consumer{
		log:           logger,
		avroConverter: avro.NewConverter(options.SchemaRegistry, avro.ConverterOptions{}),
	}

	configMap := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(options.KafkaAddresses, ","),
		"group.id":          options.KafkaConsumerGroup,
		"auto.offset.reset": options.KafkaOffsetReset,
		"security.protocol": "ssl",

		// auto commit true is the default.
		"enable.auto.commit": "false",
	}

	consumer, err := kafka.NewConsumer(&configMap)
	startup_base.FatalOnError(err, "create kafka consumer failed")
	c.consumer = consumer


	return c
}

func (c *Consumer) Close() error {
	if c.running {
		c.log.Infof("Closing")

		c.kafkaDone <- true
		c.wg.Wait()
	}
	return nil
}

func (c *Consumer) Run() {
	if c.running {
		return
	}

	err := c.consumer.SubscribeTopics(topics, nil)
	startup_base.FatalOnError(err, "subscribe to topics")

	go func() {
		defer func() {
			startup_base.Close(c.consumer, "close consumer")
			c.wg.Done()
		}()

		c.wg.Add(1)

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		doStuffAndCommit := func(message map[string]interface{}) error {
			// stuff(message)

			// commit whatever we've read
			_, err = c.consumer.Commit()
			if err != nil {
				c.log.Errorf("failed to commit kafka message, will try again later: %s", err)
			}

			return nil
		}

		for {
			select {
			case <-ticker.C:
				if err := doStuffAndCommit(nil); err != nil {
					c.log.Errorf("ticker failed to merge in kafka message, stopping here, will try again during next startup: %s", err)
					return
				}
			case <-c.kafkaDone:
				return

			default:
				msg, err := c.consumer.ReadMessage(100 * time.Millisecond)
				if err == nil {
					m, _, err := c.avroConverter.Parse(msg.Value)
					if err != nil {
						c.log.Errorf("failed to strip schema hash from kafka message %s", err)
						continue
					}

					if err := doStuffAndCommit(m); err != nil {
						c.log.Errorf("failed to merge in kafka message, stopping here, will try again during next startup: %s", err)
						return
					}
					ticker.Reset(5 * time.Second)

				} else {
					if err.(kafka.Error).Code() != kafka.ErrTimedOut {
						// The client will automatically try to recover from all errors.
						c.log.Errorf("Consumer error: %v (%v)", err, msg)
					}
				}

			}

		}
	}()

	c.running = true
}
