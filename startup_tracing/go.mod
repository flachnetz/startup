module github.com/flachnetz/startup/startup_tracing

go 1.12

require (
	cloud.google.com/go v0.44.3 // indirect
	github.com/DataDog/zstd v1.4.1 // indirect
	github.com/Shopify/sarama v1.23.1 // indirect
	github.com/apache/thrift v0.12.0
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/flachnetz/startup/startup_http v1.0.3
	github.com/flachnetz/startup/startup_logrus v1.0.6
	github.com/go-logfmt/logfmt v0.4.0 // indirect
	github.com/gogo/protobuf v1.2.2-0.20190730201129-28a6bbf47e48 // indirect
	github.com/google/go-cmp v0.3.1 // indirect
	github.com/gorilla/handlers v1.4.2 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/kr/pty v1.1.8 // indirect
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
	github.com/pierrec/lz4 v2.2.6+incompatible // indirect
	github.com/rcrowley/go-metrics v0.0.0-20190706150252-9beb055b7962 // indirect
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/objx v0.2.0 // indirect
	github.com/stretchr/testify v1.4.0 // indirect
	golang.org/x/crypto v0.0.0-20190701094942-4def268fd1a4 // indirect
	golang.org/x/exp v0.0.0-20190627132806-fd42eb6b336f // indirect
	golang.org/x/image v0.0.0-20190703141733-d6a02ce849c9 // indirect
	golang.org/x/mobile v0.0.0-20190607214518-6fa95d984e88 // indirect
	golang.org/x/mod v0.1.0 // indirect
	golang.org/x/net v0.0.0-20190724013045-ca1201d0de80 // indirect
	golang.org/x/tools v0.0.0-20190806215303-88ddfcebc769 // indirect
	google.golang.org/api v0.9.0 // indirect
	google.golang.org/grpc v1.23.0 // indirect
	gopkg.in/go-playground/validator.v9 v9.29.1 // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.3.0 // indirect
	honnef.co/go/tools v0.0.1-2019.2.2 // indirect
)

// replace github.com/openzipkin/zipkin-go-opentracing v0.3.5 => github.com/flachnetz/zipkin-go-opentracing v0.3.5

replace (
	github.com/flachnetz/startup/startup_base => ../startup_base
	github.com/flachnetz/startup/startup_http => ../startup_http
	github.com/flachnetz/startup/startup_logrus => ../startup_logrus
)
