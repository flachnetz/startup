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
relay: a `LISTEN`/`NOTIFY` consumer for low-latency delivery, backed by a
periodic sweeper that catches anything a missed notification left behind.

## Notifying the relay

Have your insert trigger notify the `kafka-message` channel so a freshly written
row is forwarded right away; without a notification the sweeper still picks it up
on its next pass, just later.

The payload is a JSON object carrying the row id together with its `kafka_key`:

```sql
SELECT pg_notify('kafka-message', json_build_object('id', id, 'key', kafka_key)::text);
```

Shipping the key inside the notification lets the relay choose a worker shard
without a second query. Shards are keyed on `kafka_key`, so every row for a given
key is published in insertion order — and therefore lands on its Kafka partition
in order — regardless of `WorkerCount`.

Only this JSON shape is accepted. Any other payload is ignored on the notify path
and left for the sweeper to forward.
