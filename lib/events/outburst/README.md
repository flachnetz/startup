# outburst

A transactional outbox relay: it moves rows from a Postgres table to Kafka.

## Usage

Import it as a library and start it during application boot:

```golang
var db *sqlx.DB = connectToDB()

err := outburst.Initialize(ctx, outburst.Options{
  Kafka: kafkaProducer,
  Database: db,
  OutboxTable: "outbox",
})
```

`Initialize` provisions the table when it is missing and launches a background
relay: a periodic sweeper that polls the outbox table every 500ms, publishing
rows to Kafka in `id` order and deleting them once they're delivered.

## Ordering

Rows are delivered in `id` order, and the sweep is serialized across instances
behind a Postgres advisory lock, so at most one instance is ever draining the
outbox at a time — ordering holds globally, no matter how many instances of
the service are running.

## NOTIFY fast path (disabled)

The outbox table still accepts a `LISTEN`/`NOTIFY`-based low-latency path in
the code (see `runNotifyListener`), but `Initialize` does not start it:
`NOTIFY` is broadcast to every listening process, so with more than one
instance running, the per-key ordering guarantee that path relies on (routing
a `kafka_key` to a single worker goroutine) only holds inside one process, not
across the fleet. Re-enabling it needs either a single-instance deployment or
leader election so only one instance ever listens.
