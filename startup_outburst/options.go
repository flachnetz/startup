package startup_outburst

import (
	"context"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/flachnetz/startup/v2/lib/events/outburst"
	sb "github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_kafka"
	"github.com/flachnetz/startup/v2/startup_postgres"
)

// Options wires the outburst outbox consumer into the startup framework and
// exposes its tunables as command line flags. Zero values fall back to the
// library defaults (4 workers, 128 queue buffer, 1024 batch size).
type Options struct {
	WorkerCount       uint `long:"outburst-worker-count" env:"OUTBURST_WORKER_COUNT" default:"4" description:"Number of key-sharded workers on the notify path. Rows sharing a kafka_key keep their order at any value."`
	WorkerQueueBuffer uint `long:"outburst-queue-buffer" env:"OUTBURST_QUEUE_BUFFER" default:"128" description:"Buffer size of each per-shard worker queue. A full queue applies backpressure to the listen loop."`
	BatchSize         uint `long:"outburst-batch-size" env:"OUTBURST_BATCH_SIZE" default:"512" description:"Number of rows read per batch by the sweeper."`
	EnableDebug       bool `long:"outburst-debug" env:"OUTBURST_DEBUG" description:"Enable outburst debug logging."`
}

// Initialize creates the outbox table and starts the outburst background task.
func (o *Options) Initialize(
	ctx context.Context,
	base sb.BaseOptions,
	kafka startup_kafka.KafkaOptions,
	pg *startup_postgres.PostgresOptions,
) {
	err := outburst.Initialize(ctx, outburst.Options{
		// The sweeper waits for every delivery report of a batch, so linger adds
		// straight to each batch. A batch is produced at once, so 25ms still
		// compresses it as one.
		Kafka:              kafka.NewProducer(confluent.ConfigMap{"queue.buffering.max.ms": 25}),
		Database:           pg.Connection(),
		OutboxTable:        base.TableName("outbox"),
		WorkerCount:        o.WorkerCount,
		WorkerQueueBuffer:  o.WorkerQueueBuffer,
		BatchSize:          o.BatchSize,
		EnableDebugLogging: o.EnableDebug,
	})

	sb.FatalOnError(err, "Create outbox failed")
}
