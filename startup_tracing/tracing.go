package startup_tracing

import (
	"github.com/flachnetz/startup/startup_base"
	"github.com/opentracing/opentracing-go"
	zipkinot "github.com/openzipkin-contrib/zipkin-go-opentracing"
	"github.com/openzipkin/zipkin-go"
	zipkinhttp "github.com/openzipkin/zipkin-go/reporter/http"
	"github.com/sirupsen/logrus"
	"strings"

	"sync"
)

var log = logrus.WithField("prefix", "zipkin")

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

		if strings.Contains(opts.Zipkin, "/v1/spans") {
			log.Warnf("Using zipkin v2 span reporting but a v1 span url was given.")
		}

		url := strings.ReplaceAll(opts.Zipkin, "/v1/spans", "/v2/spans")
		reporter := zipkinhttp.NewReporter(url)

		endpoint, err := zipkin.NewEndpoint(opts.Inputs.ServiceName, "")
		startup_base.PanicOnError(err, "Unable to create zipkin endpoint")

		nativeTracer, err := zipkin.NewTracer(reporter,
			zipkin.WithLocalEndpoint(endpoint),
			zipkin.WithSharedSpans(false),
			zipkin.WithTraceID128Bit(false))

		startup_base.PanicOnError(err, "Unable to create zipkin tracer")

		tracer := zipkinot.Wrap(nativeTracer)

		// explicitly set our tracer to be the default tracer.
		opentracing.InitGlobalTracer(tracer)
	})
}
