package startup_kafka

// Simple flags for consumer
type KafkaOptions struct {
	KafkaAddresses     []string `long:"kafka-address" validate:"dive,hostport" description:"Address of kafka server to use. Can be specified multiple times to connect to multiple brokers."`
	KafkaConsumerGroup string   `long:"kafka-consumer-group" default:"abconfig" description:"Consumer group of kafka messages"`
	KafkaOffsetReset   string   `long:"kafka-offset-reset" default:"smallest" description:"Offset reset for kafka topic" choice:"smallest" choice:"largest"`
}


