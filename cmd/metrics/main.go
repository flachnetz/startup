package main

import (
	"github.com/rcrowley/go-metrics"
	datadog "github.com/syntaqx/go-metrics-datadog"
	"log"
	"time"
)

func main() {
	registry := metrics.DefaultRegistry
	reporter, err := datadog.NewReporter(
		registry, // Metrics registry, or nil for default
		"app-01-iwg.test.t24.eu-west-1.sg-cloud.co.uk:8125", // DogStatsD UDP address
		time.Second, // Update interval
		datadog.UsePercentiles([]float64{0.25, 0.99}),
	)
	if err != nil {
		log.Fatal(err)
	}

	// configure a prefix, and send the EC2 availability zone as a tag with
	// every metric.
	reporter.Client.Namespace = "test."
	reporter.Client.Tags = append(reporter.Client.Tags, "us-east-1a")

	go reporter.Flush()

	for i := 0; i < 100; i++ {
		time.Sleep(1 * time.Second)
		metrics.GetOrRegisterCounter("first.count", registry).Inc(1)
		println("ping")
	}
	println("done")
}
