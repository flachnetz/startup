module github.com/flachnetz/startup/startup_metrics

go 1.12

require (
	github.com/flachnetz/go-datadog v1.2.0 // indirect
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/golang/protobuf v1.3.1 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/mattn/go-isatty v0.0.7 // indirect
	github.com/pkg/errors v0.8.1
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a
	github.com/sirupsen/logrus v1.4.0
	github.com/syntaqx/go-metrics-datadog v0.0.0-20181220201509-312b31920cc5
	golang.org/x/crypto v0.0.0-20190325154230-a5d413f7728c // indirect
	golang.org/x/net v0.0.0-20190326090315-15845e8f865b // indirect
	golang.org/x/sys v0.0.0-20190322080309-f49334f85ddc // indirect
	gopkg.in/yaml.v2 v2.2.2 // indirect
)

replace github.com/flachnetz/startup/startup_base => ../startup_base
