package kafka

import (
	"github.com/Shopify/sarama"
	"time"
)

func DefaultConfig() *sarama.Config {
	config := sarama.NewConfig()

	config.Version = sarama.V2_4_0_0

	config.Consumer.MaxWaitTime = 8 * time.Second
	config.Consumer.Fetch.Min = 1024
	config.Consumer.Return.Errors = true

	config.Net.MaxOpenRequests = 16
	config.Net.DialTimeout = 10 * time.Second
	config.Net.KeepAlive = 1
	config.Net.TLS.Enable = true

	config.Metadata.Retry.Max = 60
	config.Metadata.Retry.Backoff = 2000 * time.Millisecond

	config.Producer.Timeout = 3 * time.Second
	config.Producer.Idempotent = false

	config.Producer.Retry.Max = 60
	config.Producer.Retry.Backoff = 2000 * time.Millisecond

	config.Producer.Flush.Bytes = 64000
	config.Producer.Flush.Frequency = 1000 * time.Millisecond
	config.Producer.Flush.MaxMessages = 100
	config.Producer.Flush.Messages = 100

	return config
}
