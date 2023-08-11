package startup_metrics

import (
	logrus "github.com/sirupsen/logrus"
	"net"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/DataDog/datadog-go/v5/statsd"

	"github.com/flachnetz/go-datadog"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
)

var log = logrus.WithField("prefix", "main")

type MetricsPrefix string

type MetricsOptions struct {
	Datadog struct {
		ApiKey        string        `long:"datadog-apikey" description:"Datadog app key to enable datadog metrics reporting."`
		Tags          string        `long:"datadog-tags" description:"Extra datadog tags to add to every metric. Comma or space separated list of key:value pairs."`
		Interval      time.Duration `long:"datadog-report-interval" default:"60s" description:"Data collection and reporting interval."`
		StatsDAddress string        `long:"datadog-statsd-address" description:"Address of statsd,e.g. 127.0.0.1:8125"`
	}

	Inputs struct {
		// Prefix to apply to all metrics. This must not be empty.
		MetricsPrefix string `validate:"required"`

		// Disable capture of runtime metrics for some reasons
		NoRuntimeMetrics bool
	}

	once sync.Once
}

func (opts *MetricsOptions) Initialize() {
	opts.once.Do(func() {
		registry := metrics.DefaultRegistry

		if prefix := strings.TrimSuffix(opts.Inputs.MetricsPrefix, "."); prefix != "" {
			log.Debugf("Prefixing all metrics with '%s'", prefix)
			registry = prefixRegistry(registry, prefix+".")
			metrics.DefaultRegistry = registry

		} else {
			startup_base.Panicf("Metrics prefix must be set")
			return
		}

		if !opts.Inputs.NoRuntimeMetrics {
			captureRuntimeMetrics(registry)
		}

		if opts.Datadog.ApiKey != "" {
			err := opts.setupDatadogMetricsReporter(registry)
			startup_base.PanicOnError(err, "Cannot start datadog metrics reporter")
		}

		if opts.Datadog.StatsDAddress != "" {
			tags, err := opts.baseTags()
			startup_base.PanicOnError(err, "cannot create base datadog tags")

			udpAddr, err := net.ResolveUDPAddr("udp", opts.Datadog.StatsDAddress)
			startup_base.PanicOnError(err, "cannot resolve "+opts.Datadog.StatsDAddress)

			c, err := statsd.New(opts.Datadog.StatsDAddress, statsd.WithTags(tags))
			startup_base.PanicOnError(err, "cannot create statsd client")

			log.Infof("Activating statsd for metrics: '%s' (%+v)", opts.Datadog.StatsDAddress, udpAddr)
			r, err := datadog.NewReporter(registry, c, opts.Datadog.Interval)
			startup_base.PanicOnError(err, "cannot start datadog statsd metrics reporter")
			go r.Flush()
		}

		if opts.Datadog.ApiKey != "" && opts.Datadog.StatsDAddress != "" {
			log.Warn("there are two datadog reports active now: statsd address has been configured and api key has been set")
		}
	})
}

func captureRuntimeMetrics(registry metrics.Registry) {
	log.Debug("Start capturing of golang runtime metrics")

	// start capturing of metrics
	metrics.RegisterRuntimeMemStats(registry)
	go metrics.CaptureRuntimeMemStats(registry, 5*time.Second)
}

func (opts *MetricsOptions) setupDatadogMetricsReporter(registry metrics.Registry) error {
	tags, err := opts.baseTags()
	if err != nil {
		return err
	}

	log.Infof("Starting datadog metrics reporting with tags: %s", strings.Join(tags, ", "))
	client := datadog.New("", opts.Datadog.ApiKey)
	reporter := datadog.Reporter(client, registry, tags)
	go reporter.Start(opts.Datadog.Interval)

	return nil
}

func (opts *MetricsOptions) baseTags() ([]string, error) {
	node, err := os.Hostname()
	if err != nil {
		return nil, errors.WithMessage(err, "get hostname of machine")
	}

	tags := strings.FieldsFunc(opts.Datadog.Tags, isCommaOrSpace)
	tags = append(tags, "node:"+node)
	return tags, nil
}

func isCommaOrSpace(r rune) bool {
	return r == ',' || unicode.IsSpace(r)
}

func prefixRegistry(r metrics.Registry, prefix string) metrics.Registry {
	// remove the "." at the end
	prefix = strings.TrimRight(prefix, ".")

	// get a copy of all metrics
	backup := make(map[string]interface{})
	r.Each(func(name string, metric interface{}) {
		backup[name] = metric
	})

	// We must not unregister everything from this metrics, as this would
	// stop the Meters from updating.
	// r.UnregisterAll()

	// insert them all into the prefixed registry
	prefixed := metrics.NewPrefixedRegistry(prefix + ".")
	for name, metric := range backup {
		err := prefixed.Register(name, metric)
		startup_base.PanicOnError(err, "init prefixed registry")
	}

	return prefixed
}
