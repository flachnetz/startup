module github.com/flachnetz/startup/startup_metrics

go 1.12

require (
	github.com/flachnetz/go-datadog v1.2.0 // indirect
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/pkg/errors v0.8.1
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a
	github.com/sirupsen/logrus v1.4.2
	github.com/syntaqx/go-metrics-datadog v0.0.0-20181220201509-312b31920cc5
)

replace github.com/flachnetz/startup/startup_base => ../startup_base
