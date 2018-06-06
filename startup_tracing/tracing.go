package startup_tracing

import (
	"github.com/opentracing/opentracing-go"
	zipkin "github.com/openzipkin/zipkin-go-opentracing"
	"github.com/sirupsen/logrus"

	// dummy import, see
	// https://github.com/golang/dep/blob/master/docs/FAQ.md#how-do-i-constrain-a-transitive-dependency-s-version
	_ "github.com/apache/thrift/lib/go/thrift"
	"github.com/flachnetz/startup"
	"sync"
)

var log = logrus.WithField("prefix", "zipkin")

// logging for the collector
var httpLogger = zipkin.LoggerFunc(func(kv ...interface{}) error {
	log.Warn(kv...)
	return nil
})

type TracingOptions struct {
	Zipkin string `long:"zipkin" validate:"omitempty,url" description:"Zipkin server base url, an URL like http://host:9411/"`

	Inputs struct {
		// The service name of your application
		ServiceName string `validate:"required"`
	}

	once sync.Once
}

func (opts *TracingOptions) IsActive() bool {
	return opts.Zipkin != ""
}

func (opts *TracingOptions) Initialize() {
	if !opts.IsActive() {
		return
	}

	opts.once.Do(func() {
		log.Infof("Sending zipkin traces to %s", opts.Zipkin)

		// create collector
		collector, err := zipkin.NewHTTPCollector(opts.Zipkin, zipkin.HTTPLogger(httpLogger))
		startup.PanicOnError(err, "unable to create zipkin http collector")

		// create recorder
		recorder := zipkin.NewRecorder(collector, false, "", opts.Inputs.ServiceName)

		// create tracer
		tracer, err := zipkin.NewTracer(
			recorder,
			zipkin.ClientServerSameSpan(true),
			zipkin.TraceID128Bit(false))

		startup.PanicOnError(err, "Unable to create zipkin tracer")

		// explicitly set our tracer to be the default tracer.
		opentracing.InitGlobalTracer(tracer)
	})
}
