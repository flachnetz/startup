package startup_kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/flachnetz/startup/v2/startup_base"
	"strings"
	"time"
)

// KafkaOptions provides simple flags for to create a kafka consumer
type KafkaOptions struct {
	KafkaAddresses        []string `long:"kafka-address" validate:"dive,hostport" description:"Address of kafka server to use. Can be specified multiple times to connect to multiple brokers."`
	KafkaConsumerGroup    string   `long:"kafka-consumer-group" default:"RANDOM" description:"Consumer group of kafka messages"`
	KafkaOffsetReset      string   `long:"kafka-offset-reset" default:"smallest" description:"Offset reset for kafka topic" choice:"smallest" choice:"largest"`
	KafkaSecurityProtocol string   `long:"kafka-security-protocol" default:"ssl" description:"Security protocol" choice:"ssl" choice:"plaintext"`
}

func (opts KafkaOptions) NewConsumer(config kafka.ConfigMap) *kafka.Consumer {
	if opts.KafkaConsumerGroup == "RANDOM" {
		opts.KafkaConsumerGroup = fmt.Sprintf("golang-%d", time.Now().UnixNano())
	}

	configMap := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(opts.KafkaAddresses, ","),
		"group.id":          opts.KafkaConsumerGroup,
		"auto.offset.reset": opts.KafkaOffsetReset,
		"security.protocol": opts.KafkaSecurityProtocol,

		// auto commit true is the default.
		"enable.auto.commit": "false",
	}

	for key, value := range config {
		configMap[key] = value
	}

	consumer, err := kafka.NewConsumer(&configMap)
	startup_base.FatalOnError(err, "create kafka consumer failed")

	return consumer
}
