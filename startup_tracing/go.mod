module github.com/flachnetz/startup/startup_tracing

go 1.12

require (
	github.com/apache/thrift v0.12.0
	github.com/flachnetz/startup v1.5.3
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/grpc-ecosystem/grpc-gateway v1.8.5
	github.com/modern-go/gls v0.0.0-20180301095631-18e3a666c380
	github.com/opentracing/opentracing-go v1.0.2
	github.com/openzipkin/zipkin-go-opentracing v0.3.5
	github.com/sirupsen/logrus v1.4.0
	google.golang.org/grpc v1.19.1
)

replace github.com/flachnetz/startup v1.5.3 => ../.
