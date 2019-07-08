module github.com/flachnetz/startup/startup_kafka

go 1.12

require (
	github.com/Shopify/sarama v1.21.0
	github.com/flachnetz/startup/lib/kafka v1.0.0
	github.com/flachnetz/startup/lib/schema v1.0.0
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/sirupsen/logrus v1.4.0
)

replace (
	github.com/flachnetz/startup/lib/kafka => ../lib/kafka
	github.com/flachnetz/startup/lib/schema => ../lib/schema
	github.com/flachnetz/startup/startup_base => ../startup_base
)
