module github.com/flachnetz/startup/startup_tracing

go 1.12

require (
	github.com/Shopify/sarama v1.21.0 // indirect
	github.com/apache/thrift v0.12.0
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/flachnetz/startup/startup_http v1.0.0
	github.com/flachnetz/startup/startup_logrus v1.0.5
	github.com/go-logfmt/logfmt v0.4.0 // indirect
	github.com/gogo/protobuf v1.2.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/gls v0.0.0-20180301095631-18e3a666c380
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/opentracing-contrib/go-observer v0.0.0-20170622124052-a52f23424492 // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/openzipkin-contrib/zipkin-go-opentracing v0.3.5 // indirect
	github.com/openzipkin/zipkin-go-opentracing v0.3.5
	github.com/sirupsen/logrus v1.4.0
)

replace github.com/openzipkin/zipkin-go-opentracing v0.3.5 => github.com/flachnetz/zipkin-go-opentracing v0.3.5

replace (
	github.com/flachnetz/startup/startup_base => ../startup_base
	github.com/flachnetz/startup/startup_http => ../startup_http
	github.com/flachnetz/startup/startup_logrus => ../startup_logrus
)
