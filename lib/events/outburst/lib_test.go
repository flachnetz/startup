package outburst

import (
	"strconv"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/flachnetz/pgtest/v2"
	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/flachnetz/startup/v2/lib/testx"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type testServices struct {
	*testing.T
	DB    *sqlx.DB
	Kafka *testx.Kafka
}

func setupService(t *testing.T) testServices {
	kafkaCluster := testx.KafkaCluster(t)

	db := sqlx.NewDb(pgtest.Connect(t), "pgx")

	return testServices{
		T:     t,
		DB:    db,
		Kafka: kafkaCluster,
	}
}

func (t *testServices) Consume(topic string, n int) []kafka.Message {
	t.Helper()

	configMap := kafka.ConfigMap{
		"bootstrap.servers": t.Kafka.BootstrapServers,
		"group.id":          strconv.Itoa(int(time.Now().UnixNano())),
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(&configMap)
	require.NoError(t, err)

	defer consumer.Close()

	err = consumer.Subscribe(topic, nil)
	require.NoError(t, err)

	defer consumer.Unsubscribe()

	var messages []kafka.Message
	for len(messages) < n {
		msg, err := consumer.ReadMessage(1 * time.Second)
		if err != nil && err.(kafka.Error).IsTimeout() {
			require.NoError(t, t.Context().Err())

			idx := len(messages)
			t.Logf("Waiting for kafka event idx=%d", idx)
			continue
		}

		require.NoError(t, err)

		messages = append(messages, *msg)
	}

	return messages
}

type outboxEntry struct {
	Topic      string
	Key        *string
	Value      []byte
	HeaderKeys []string
	HeaderVals []string
}

func (t *testServices) InsertOutbox(entry outboxEntry) {
	t.Helper()

	if entry.HeaderKeys == nil {
		entry.HeaderKeys = []string{}
	}

	if entry.HeaderVals == nil {
		entry.HeaderVals = []string{}
	}

	stmt := `
		WITH
			ids AS (
				INSERT INTO outbox (kafka_topic, kafka_key, kafka_value, kafka_header_keys, kafka_header_values)
				VALUES ($1, $2, $3, $4, $5)
				RETURNING id, kafka_key)

		SELECT pg_notify('kafka-message', json_build_object('id', id, 'key', kafka_key)::text)
		FROM ids;
	`

	testx.MustTransact(t.T, t.DB, func(ctx ql.TxContext) {
		_, err := t.DB.Exec(
			stmt,
			entry.Topic,
			entry.Key,
			entry.Value,
			entry.HeaderKeys,
			entry.HeaderVals,
		)
		require.NoError(t, err)
	})
}

func TestOutburstNotifyListen(t *testing.T) {
	svc := setupService(t)

	svc.Kafka.CreateTopic("foobar", 4)

	ctx := t.Context()

	producer := svc.Kafka.Producer()

	err := Initialize(ctx, Options{
		Kafka:                producer,
		Database:             svc.DB,
		OutboxTable:          "outbox",
		testDisableIterBatch: true,
	})

	require.NoError(t, err)

	svc.InsertOutbox(outboxEntry{
		Topic: "foobar",
		Key:   new("key-a"),
		Value: []byte("message-a"),
	})

	message := svc.Kafka.TestConsumer("foobar").Message()
	require.Equal(t, []byte("key-a"), message.Key)
	require.Equal(t, []byte("message-a"), message.Value)
}

func TestOutburstBatch(t *testing.T) {
	svc := setupService(t)

	svc.Kafka.CreateTopic("foobar", 4)

	ctx := t.Context()

	err := Initialize(ctx, Options{
		Kafka:                 svc.Kafka.Producer(),
		Database:              svc.DB,
		OutboxTable:           "outbox",
		testDisableIterNotify: true,
	})

	require.NoError(t, err)

	svc.InsertOutbox(outboxEntry{
		Topic: "foobar",
		Key:   new("key-a"),
		Value: []byte("message-a"),
	})

	messages := svc.Consume("foobar", 1)
	require.Equal(t, []byte("key-a"), messages[0].Key)
	require.Equal(t, []byte("message-a"), messages[0].Value)
}

// A round-trip of headers must survive, and a NULL kafka_key must produce a
// nil message key.
func TestOutburstHeadersAndNilKey(t *testing.T) {
	svc := setupService(t)

	svc.Kafka.CreateTopic("foobar", 4)

	ctx := t.Context()

	err := Initialize(ctx, Options{
		Kafka:                svc.Kafka.Producer(),
		Database:             svc.DB,
		OutboxTable:          "outbox",
		testDisableIterBatch: true,
	})
	require.NoError(t, err)

	svc.InsertOutbox(outboxEntry{
		Topic:      "foobar",
		Key:        nil, // NULL key -> nil message key
		Value:      []byte("message-a"),
		HeaderKeys: []string{"h1", "h2"},
		HeaderVals: []string{"v1", "v2"},
	})

	message := svc.Kafka.TestConsumer("foobar").Message()
	require.Nil(t, message.Key)
	require.Equal(t, []byte("message-a"), message.Value)

	got := map[string]string{}
	for _, h := range message.Headers {
		got[h.Key] = string(h.Value)
	}
	require.Equal(t, map[string]string{"h1": "v1", "h2": "v2"}, got)
}

// forwardRow against an id that no longer exists must quietly succeed rather
// than error.
func TestForwardRowMissing(t *testing.T) {
	svc := setupService(t)

	ctx := t.Context()

	db := outboxDB{DB: svc.DB, Table: "outbox"}
	require.NoError(t, ensureOutboxTable(ctx, db))

	err := forwardRow(ctx, db, 999999, svc.Kafka.Producer())
	require.NoError(t, err)
}

// outboxSizeJob must publish the current row count onto the outbox-size gauge.
func TestOutboxSizeJob(t *testing.T) {
	svc := setupService(t)

	ctx := t.Context()

	db := outboxDB{DB: svc.DB, Table: "outbox"}
	require.NoError(t, ensureOutboxTable(ctx, db))

	for i := 0; i < 3; i++ {
		svc.InsertOutbox(outboxEntry{Topic: "foobar", Value: []byte("x")})
	}

	outboxSizeJob(db)()

	require.Equal(t, float64(3), testutil.ToFloat64(outboxSizeGauge))
}

// vacuumJob must run VACUUM and leave the rows untouched; when another caller
// already holds the advisory lock the job must bow out cleanly.
func TestVacuumJob(t *testing.T) {
	svc := setupService(t)

	ctx := t.Context()

	db := outboxDB{DB: svc.DB, Table: "outbox"}
	require.NoError(t, ensureOutboxTable(ctx, db))

	svc.InsertOutbox(outboxEntry{Topic: "foobar", Value: []byte("x")})

	// happy path: lock is free, VACUUM runs, the row survives
	vacuumJob(db)()

	var count int
	require.NoError(t, svc.DB.GetContext(ctx, &count, "SELECT COUNT(*) FROM outbox"))
	require.Equal(t, 1, count)

	// contention: keep the advisory lock on a second connection so the job is
	// forced to give up without error
	conn, err := db.Connx(ctx)
	require.NoError(t, err)
	defer conn.Close()

	locked, err := acquireAdvisoryLock(ctx, conn, advisoryLockID("outburst:vacuum"))
	require.NoError(t, err)
	require.True(t, locked)

	vacuumJob(db)() // returns promptly, no panic

	require.NoError(t, releaseAdvisoryLock(ctx, conn, advisoryLockID("outburst:vacuum")))
}
