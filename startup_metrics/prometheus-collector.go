package startup_metrics

import (
	"github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rcrowley/go-metrics"
	"golang.org/x/net/context"
	"net/http"
	"strings"
	"time"
)

type PrometheusConfig struct {
	Enabled bool   `long:"prometheus" description:"Enable Prometheus metrics"`
	Path    string `long:"prometheus-path" default:"/metrics" description:"Path for Prometheus metrics endpoint"`
	Port    string `long:"prometheus-port" default:":9090" description:"Port for Prometheus metrics endpoint"`

	httpServer *http.Server
}

// rcrowleyCollector implements prometheus.Collector
type rcrowleyCollector struct{}

func (c *rcrowleyCollector) Describe(ch chan<- *prometheus.Desc) {
	// Intentionally left empty. Using DescribeByCollect is simpler for dynamic metric sets.
}

func (c *rcrowleyCollector) Collect(ch chan<- prometheus.Metric) {
	metricsRegistry := metrics.DefaultRegistry
	metricsRegistry.Each(func(name string, i interface{}) {
		safeName := sanitizeMetricName(name)

		switch metric := i.(type) {
		case metrics.Counter:
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(safeName, "Counter from go-metrics", nil, nil),
				prometheus.CounterValue,
				float64(metric.Count()),
			)
		case metrics.Gauge:
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(safeName, "Gauge from go-metrics", nil, nil),
				prometheus.GaugeValue,
				float64(metric.Value()),
			)
		case metrics.GaugeFloat64:
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(safeName, "GaugeFloat64 from go-metrics", nil, nil),
				prometheus.GaugeValue,
				metric.Value(),
			)
		case metrics.Timer:
			snapshot := metric.Snapshot()
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(safeName+"_count", "Timer count", nil, nil),
				prometheus.CounterValue,
				float64(snapshot.Count()),
			)
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(safeName+"_mean", "Timer mean", nil, nil),
				prometheus.GaugeValue,
				snapshot.Mean()/float64(time.Millisecond),
			)
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(safeName+"_95th_percentile", "Timer 95th percentile", nil, nil),
				prometheus.GaugeValue,
				snapshot.Percentile(0.95)/float64(time.Millisecond),
			)
		case metrics.Meter:
			snapshot := metric.Snapshot()
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(safeName+"_rate1", "Meter 1m rate", nil, nil),
				prometheus.GaugeValue,
				snapshot.Rate1(),
			)
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(safeName+"_rate5", "Meter 5m rate", nil, nil),
				prometheus.GaugeValue,
				snapshot.Rate5(),
			)
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(safeName+"_rate15", "Meter 15m rate", nil, nil),
				prometheus.GaugeValue,
				snapshot.Rate15(),
			)
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(safeName+"_count", "Meter count", nil, nil),
				prometheus.CounterValue,
				float64(snapshot.Count()),
			)
		}
	})
}

func sanitizeMetricName(name string) string {
	safe := strings.ReplaceAll(name, ".", "_")
	safe = strings.ReplaceAll(safe, "[", "_")
	safe = strings.ReplaceAll(safe, "]", "")
	safe = strings.ReplaceAll(safe, ",", "_")
	safe = strings.ReplaceAll(safe, ":", "_")
	return safe
}

func startPrometheusMetrics(opts PrometheusConfig) *http.Server {
	mux := http.NewServeMux()
	mux.Handle(opts.Path, promhttp.Handler())
	prometheusHttpServer := &http.Server{
		Addr:    opts.Port,
		Handler: mux,
	}

	go func() {
		// Start Prometheus HTTP server

		logger := startup_logrus.LoggerOf(context.Background())
		logger.Infof("Starting Prometheus metrics endpoint on %s%s", opts.Port, opts.Path)
		if err := prometheusHttpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.WithError(err).Error("Prometheus HTTP server failed")
		}
	}()

	return prometheusHttpServer
}
