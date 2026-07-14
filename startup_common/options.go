package startup_common

import (
	"context"

	"github.com/flachnetz/startup/v2/lib/timejump"
	sb "github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_events"
	"github.com/flachnetz/startup/v2/startup_history"
	ht "github.com/flachnetz/startup/v2/startup_http"
	"github.com/flachnetz/startup/v2/startup_kafka"
	"github.com/flachnetz/startup/v2/startup_kube"
	metrics "github.com/flachnetz/startup/v2/startup_metrics"
	pg "github.com/flachnetz/startup/v2/startup_postgres"
	tracing "github.com/flachnetz/startup/v2/startup_tracing"
	tracing_pg "github.com/flachnetz/startup/v2/startup_tracing_pg"
	"github.com/flachnetz/startup/v2/startup_unleash"
)

// Options define a set of common options that are used by most of our services.
type Options struct {
	Base            sb.BaseOptions                    `group:"Base configuration"`
	HTTP            ht.HTTPOptions                    `group:"HTTP server settings"`
	Postgres        pg.PostgresOptions                `group:"Database configuration"`
	Tracing         tracing.TracingOptions            `group:"Tracing configuration"`
	PostgresTracing tracing_pg.PostgresTracingOptions `group:"Tracing configuration (database)"`
	Metrics         metrics.MetricsOptions            `group:"Metrics & reporting"`
	Unleash         startup_unleash.Unleash           `group:"Unleash feature flags"`
	Kafka           startup_kafka.KafkaOptions        `group:"Kafka options"`
	Events          startup_events.EventOptions       `group:"Event sending"`
	Kubernetes      startup_kube.KubernetesOptions    `group:"Kubernetes options"`
	History         startup_history.HistoryOptions    `group:"History options"`
	Timejump        timejump.Options                  `group:"Timejump options"`
}

func (o *Options) PropagateInputs() {
	if o.Postgres.Inputs.Initializer == nil {
		// automatically apply migrations on startup
		o.Postgres.Inputs.Initializer = pg.DefaultMigration(o.Base.TableName("schema"))
	}

	if o.Events.Inputs.OutboxTable == "" {
		// configure event sending
		o.Events.Inputs.OutboxTable = o.Base.TableName("outbox")
	}
}

func (o *Options) Initialize(ctx context.Context) {
	o.Events.EventSender()
}
