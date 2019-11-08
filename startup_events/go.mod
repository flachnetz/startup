module github.com/flachnetz/startup/startup_events

go 1.12

require (
	github.com/Landoop/schema-registry v0.0.0-20190327143759-50a5701c1891
	github.com/Shopify/sarama v1.21.0
	github.com/flachnetz/startup/lib/events v1.0.5
	github.com/flachnetz/startup/lib/schema v1.1.1
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/golang/snappy v0.0.1 // indirect
	github.com/sirupsen/logrus v1.4.2
)

replace (
	github.com/flachnetz/startup/lib/events => ../lib/events
	github.com/flachnetz/startup/lib/schema => ../lib/schema
	github.com/flachnetz/startup/startup_base => ../startup_base
)
