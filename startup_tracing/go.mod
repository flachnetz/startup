module github.com/flachnetz/startup/startup_tracing

go 1.12

require (
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/flachnetz/startup/startup_http v1.0.3
	github.com/flachnetz/startup/startup_logrus v1.0.6
	github.com/golang/protobuf v1.3.2 // indirect
	github.com/gorilla/handlers v1.4.2 // indirect
	github.com/mattn/go-colorable v0.1.2 // indirect
	github.com/mattn/go-isatty v0.0.9 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/gls v0.0.0-20190610040709-84558782a674
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/onsi/ginkgo v1.9.0 // indirect
	github.com/onsi/gomega v1.6.0 // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/openzipkin-contrib/zipkin-go-opentracing v0.4.2
	github.com/openzipkin/zipkin-go v0.2.0
	github.com/rcrowley/go-metrics v0.0.0-20190706150252-9beb055b7962 // indirect
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0 // indirect
	golang.org/x/crypto v0.0.0-20190701094942-4def268fd1a4 // indirect
	golang.org/x/net v0.0.0-20190724013045-ca1201d0de80 // indirect
	golang.org/x/text v0.3.2 // indirect
	google.golang.org/grpc v1.23.0 // indirect
	gopkg.in/go-playground/validator.v9 v9.29.1 // indirect
)

replace (
	github.com/flachnetz/startup/startup_base => ../startup_base
	github.com/flachnetz/startup/startup_http => ../startup_http
	github.com/flachnetz/startup/startup_logrus => ../startup_logrus
)
