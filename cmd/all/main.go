package main

import (
	"github.com/flachnetz/startup/v2"
	"github.com/flachnetz/startup/v2/lib/events"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_consul"
	"github.com/flachnetz/startup/v2/startup_events"
	"github.com/flachnetz/startup/v2/startup_http"
	"github.com/flachnetz/startup/v2/startup_kafka"
	"github.com/flachnetz/startup/v2/startup_metrics"
	"github.com/flachnetz/startup/v2/startup_postgres"
	"github.com/flachnetz/startup/v2/startup_schema"
	"github.com/flachnetz/startup/v2/startup_tracing"
	"github.com/flachnetz/startup/v2/startup_tracing_pg"
	"github.com/jmoiron/sqlx"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"net/http/httputil"
)

func main() {
	var opts struct {
		Base            startup_base.BaseOptions
		Metrics         startup_metrics.MetricsOptions
		Consul          startup_consul.ConsulOptions
		Tracing         startup_tracing.TracingOptions
		Postgres        startup_postgres.PostgresOptions
		PostgresTracing startup_tracing_pg.PostgresTracingOptions
		Kafka           startup_kafka.KafkaOptions
		Schema          startup_schema.SchemaRegistryOptions
		Events          startup_events.EventOptions
		HTTP            startup_http.HTTPOptions
	}

	// set required values
	opts.Events.Inputs.Topics = EventTopics
	opts.Tracing.Inputs.ServiceName = "my-service"
	opts.Metrics.Inputs.MetricsPrefix = "my.prefix"

	startup.MustParseCommandLine(&opts)

	defer opts.Postgres.
		Connection().
		Close()

	defer opts.Events.
		EventSender("test").
		Close()

	opts.HTTP.Serve(startup_http.Config{
		Routing: func(router *httprouter.Router) http.Handler {
			router.GET("/test", func(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
				writer.Header().Set("Content-Type", "text/plain")

				r, _ := httputil.DumpRequest(request, true)
				writer.Write([]byte("your request:\n"))
				writer.Write(r)
			})

			return router
		},
	})
}

func DatabaseInitializer(db *sqlx.DB) error {
	return nil
}

func EventTopics(replicationFactor int16) events.EventTopics {
	return events.EventTopics{
		Fallback: "events",
	}
}
