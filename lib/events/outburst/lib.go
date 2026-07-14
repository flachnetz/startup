package outburst

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flachnetz/startup/v2/lib/clock"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/go-co-op/gocron/v2"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/trace"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_tracing"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

// Options configures the outburst outbox relay.
type Options struct {
	// Producer used to publish outbox rows to Kafka.
	Kafka *kafka.Producer

	// Handle to the database that holds the outbox table.
	Database *sqlx.DB

	// Name of the outbox table to drain. Created on startup when it is missing.
	OutboxTable string

	// Scheduler that the periodic jobs attach to. When nil, outburst builds and
	// owns its own scheduler.
	Cron gocron.Scheduler

	// Rows fetched per pass by the fallback sweeper. Defaults to 128.
	BatchSize uint

	// Size of the worker pool on the LISTEN/NOTIFY path. Defaults to 4.
	//
	// Every row is dispatched to shard hash(kafka_key)%WorkerCount and each
	// shard publishes strictly in sequence. Two rows sharing a kafka_key thus
	// reach Kafka in insertion order — and land on the same partition in that
	// order — no matter how many workers run. Rows without a key carry no
	// ordering guarantee and all fall to shard 0.
	WorkerCount uint

	// Capacity of each shard's hand-off channel on the NOTIFY path. Defaults to
	// 128. Once a shard channel is full the LISTEN loop stops pulling new
	// notifications until that shard drains. Grow it to ride out spikes, shrink
	// it to bound the memory held in flight.
	WorkerQueueBuffer uint

	// Turn on verbose debug logging.
	EnableDebugLogging bool

	// test hooks
	testDisableIterBatch  bool
	testDisableIterNotify bool
}

// debugEnabled gates every debug log call; mirrors Options.EnableDebugLogging.
var debugEnabled atomic.Bool

// Prometheus metrics, auto-registered on the default registry.
var (
	outboxSizeGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outburst_outbox_size",
		Help: "Current number of undelivered rows in the outbox table.",
	})
	vacuumDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "outburst_vacuum_duration_seconds",
		Help: "Wall-clock time spent in the outbox VACUUM job.",
	})
	iterationDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "outburst_iteration_duration_seconds",
		Help: "Wall-clock time of a single sweeper pass.",
	})
	errorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "outburst_errors_total",
		Help: "Total number of failed sweeper passes.",
	})
	eventsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "outburst_events_total",
		Help: "Total number of rows published to Kafka.",
	}, []string{"topic", "type"})
)

// debugLog emits a debug log line only while debug logging is enabled.
func debugLog(ctx context.Context, log *slog.Logger, msg string, args ...any) {
	if debugEnabled.Load() {
		log.DebugContext(ctx, msg, args...)
	}
}

// Initialize provisions the outbox table when needed and launches the
// background relay: a LISTEN/NOTIFY consumer plus periodic maintenance jobs. It
// returns once everything is wired; the relay keeps running until ctx is
// cancelled.
func Initialize(ctx context.Context, opts Options) error {
	debugEnabled.Store(opts.EnableDebugLogging)

	if opts.Database == nil {
		return fmt.Errorf("database must be specified")
	}

	if opts.OutboxTable == "" {
		return fmt.Errorf("no outbox table defined")
	}

	db := outboxDB{
		DB:    opts.Database,
		Table: opts.OutboxTable,
	}

	if err := ensureOutboxTable(ctx, db); err != nil {
		return fmt.Errorf("create outbox table: %w", err)
	}

	if opts.Kafka == nil {
		slog.Warn("No kafka producer configured, outburst stays idle")
		return nil
	}

	if err := scheduleJobs(ctx, opts, db, opts.Kafka, orDefault(opts.BatchSize, 128)); err != nil {
		return fmt.Errorf("schedule cron tasks: %w", err)
	}

	slog.Info("Starting outburst background task")
	go func() {
		if opts.testDisableIterNotify {
			return
		}

		for ctx.Err() == nil {
			err := runNotifyListener(ctx, db, opts.Kafka, orDefault(opts.WorkerCount, 4), orDefault(opts.WorkerQueueBuffer, 128))
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				slog.InfoContext(ctx, "Notify listener stopped on context cancellation", sl.Error(err))
				return
			}

			if err != nil {
				slog.ErrorContext(ctx, "Notify listener failed, restarting shortly", sl.Error(err))
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	return nil
}

// orDefault returns value unless it is the zero value, in which case it returns
// fallback.
func orDefault[T comparable](value, fallback T) T {
	var zero T
	if value != zero {
		return value
	}

	return fallback
}

func scheduleJobs(ctx context.Context, opts Options, db outboxDB, producer *kafka.Producer, batchSize uint) error {
	scheduler := opts.Cron
	ownScheduler := scheduler == nil
	if ownScheduler {
		var err error
		scheduler, err = gocron.NewScheduler(
			gocron.WithClock(clock.ToClockworkClock(clock.GlobalClock)),
		)
		if err != nil {
			return fmt.Errorf("create cron scheduler: %w", err)
		}
	}

	// Reclaim dead tuples roughly every 10-15 minutes.
	if _, err := scheduler.NewJob(
		gocron.DurationRandomJob(10*time.Minute, 15*time.Minute),
		gocron.NewTask(vacuumJob(db)),
		gocron.WithContext(ctx),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	); err != nil {
		return fmt.Errorf("schedule vacuum job: %w", err)
	}

	// Report the outbox backlog roughly every 30-45 seconds.
	if _, err := scheduler.NewJob(
		gocron.DurationRandomJob(30*time.Second, 45*time.Second),
		gocron.NewTask(outboxSizeJob(db)),
		gocron.WithContext(ctx),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	); err != nil {
		return fmt.Errorf("schedule outbox size job: %w", err)
	}

	if !opts.testDisableIterBatch {
		// Safety net that sweeps up rows any missed NOTIFY left behind.
		if _, err := scheduler.NewJob(
			gocron.DurationRandomJob(10*time.Second, 20*time.Second),
			gocron.NewTask(sweepJob(ctx, db, producer, batchSize)),
			gocron.WithContext(ctx),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		); err != nil {
			return fmt.Errorf("schedule batch iteration job: %w", err)
		}
	}

	if ownScheduler {
		scheduler.Start()

		// Shut the scheduler down once the context is cancelled.
		go func() {
			<-ctx.Done()
			_ = scheduler.Shutdown()
		}()
	}

	return nil
}

// outboxDB pairs a database handle with the name of the outbox table it serves.
type outboxDB struct {
	*sqlx.DB
	Table string
}

func sweepJob(ctx context.Context, db outboxDB, producer *kafka.Producer, batchSize uint) func() {
	return func() {
		_ = startup_tracing.Trace(ctx, "sweep", func(ctx context.Context, span trace.Span) error {
			sweepOutbox(ctx, db, producer, batchSize)
			return nil
		})
	}
}

func runNotifyListener(ctx context.Context, db outboxDB, producer *kafka.Producer, workerCount, queueBuffer uint) (err error) {
	// A panic on the listen path must not take the whole process down outside of
	// dev/test; convert it into an error so the caller can restart the loop.
	defer func() {
		if r := recover(); r != nil {
			if startup_base.IsDevelopment() || startup_base.IsTesting() {
				panic(r)
			}
			switch rValue := r.(type) {
			case error:
				err = fmt.Errorf("recovered from panic: %w", rValue)
			default:
				err = fmt.Errorf("recovered from panic: %s", rValue)
			}
		}
	}()

	log := slog.Default().With("component", "outburst")

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get connection: %w", err)
	}

	defer startup_base.Close(conn, "Close session")

	return conn.Raw(func(driverConn any) error {
		pgConn := driverConn.(*stdlib.Conn)
		return consumeNotifications(ctx, pgConn.Conn(), log, db, producer, workerCount, queueBuffer)
	})
}

func consumeNotifications(ctx context.Context, conn *pgx.Conn, log *slog.Logger, db outboxDB, producer *kafka.Producer, workerCount, queueBuffer uint) error {
	if _, err := conn.Exec(ctx, `LISTEN "kafka-message"`); err != nil {
		return fmt.Errorf("listen for events: %w", err)
	}

	if workerCount < 1 {
		workerCount = 1
	}
	if queueBuffer < 1 {
		queueBuffer = 1
	}

	// One channel per shard, each drained by a single goroutine. Routing a row
	// to shard hash(kafka_key)%N guarantees every row for a given key is handled
	// by the same goroutine, and therefore published in order, while different
	// keys make progress in parallel — the property CQRS consumers rely on for
	// per-aggregate ordering.
	shards := make([]chan int64, workerCount)
	var wg sync.WaitGroup
	for i := range shards {
		// A saturated channel back-pressures the LISTEN loop (see
		// WorkerQueueBuffer).
		shards[i] = make(chan int64, queueBuffer)
		wg.Add(1)
		go func(ids <-chan int64) {
			defer wg.Done()
			for id := range ids {
				forwardGuarded(ctx, log, db, producer, id)
			}
		}(shards[i])
	}
	defer func() {
		for _, ids := range shards {
			close(ids)
		}
		wg.Wait()
	}()

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("waiting for message: %w", err)
		}

		debugLog(ctx, log, "Received notification", slog.String("payload", notification.Payload))

		id, key, ok := parseNotification(ctx, log, notification.Payload)
		if !ok {
			continue
		}

		shards[shardFor(key, len(shards))] <- id
	}
}

// notifyPayload is the JSON body carried on the "kafka-message" channel. It
// bundles the outbox row id with its kafka_key so the listener can pick a shard
// without a follow-up query. Emit it from the insert trigger, for example:
//
//	pg_notify('kafka-message', json_build_object('id', id, 'key', kafka_key)::text)
type notifyPayload struct {
	ID  int64   `json:"id"`
	Key *string `json:"key"`
}

// parseNotification decodes a JSON notification payload into its row id and
// kafka_key. It reports ok=false for anything it cannot decode; such rows are
// left for the fallback sweeper to pick up.
func parseNotification(ctx context.Context, log *slog.Logger, payload string) (id int64, key sql.NullString, ok bool) {
	var decoded notifyPayload
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		log.WarnContext(ctx, "Ignoring notification: payload is not JSON {\"id\":..,\"key\":..}", slog.String("payload", payload), sl.Error(err))
		return 0, sql.NullString{}, false
	}
	if decoded.Key != nil {
		key = sql.NullString{String: *decoded.Key, Valid: true}
	}
	return decoded.ID, key, true
}

// shardFor picks the worker shard for a kafka_key. Rows with no key carry no
// ordering guarantee, so they all land on shard 0.
func shardFor(key sql.NullString, n int) int {
	if !key.Valid {
		return 0
	}
	digest := fnv.New32a()
	_, _ = digest.Write([]byte(key.String))
	// n is the worker count: small and positive, so the result fits an int.
	return int(digest.Sum32() % uint32(n)) // #nosec G115 -- n is a small positive worker count
}

func forwardGuarded(ctx context.Context, log *slog.Logger, db outboxDB, producer *kafka.Producer, id int64) {
	// Without this, an unexpected panic here would tear down the whole process.
	// Dev/test still panic loudly; production logs and leaves the rolled-back
	// row for the sweeper to retry.
	defer func() {
		if r := recover(); r != nil {
			if startup_base.IsDevelopment() || startup_base.IsTesting() {
				panic(r)
			}
			log.ErrorContext(ctx, "Recovered from panic while forwarding message", "id", id, "panic", r)
		}
	}()

	_ = startup_tracing.Trace(ctx, "forwardRow", func(ctx context.Context, span trace.Span) error {
		err := forwardRow(ctx, db, id, producer)
		if err != nil {
			log.WarnContext(ctx, "Failed to forward message", "id", id, sl.Error(err))
		}
		return err
	})
}

func outboxSizeJob(db outboxDB) func() {
	return func() {
		log := slog.Default().With("component", "outbox-size")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var count int64
		countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, db.Table)
		if err := db.GetContext(ctx, &count, countQuery); err != nil {
			log.WarnContext(ctx, "Failed to read outbox size", sl.Error(err))
			return
		}

		outboxSizeGauge.Set(float64(count))
		debugLog(ctx, log, "Outbox size", "count", count)
	}
}

func vacuumJob(db outboxDB) func() {
	lockID := advisoryLockID("outburst:vacuum")

	return func() {
		log := slog.Default().With("component", "vacuum")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		// Dedicated connection so the session-scoped advisory lock stays put.
		conn, err := db.Connx(ctx)
		if err != nil {
			log.WarnContext(ctx, "Failed to get connection for vacuum", sl.Error(err))
			return
		}
		defer startup_base.Close(conn, "Close vacuum connection")

		// Only one instance should vacuum at a time.
		locked, err := acquireAdvisoryLock(ctx, conn, lockID)
		if err != nil {
			log.WarnContext(ctx, "Failed to acquire advisory lock", sl.Error(err))
			return
		}
		if !locked {
			// Someone else is vacuuming; try again on the next tick.
			return
		}

		defer func(ctx context.Context, conn *sqlx.Conn, lockID int64) {
			if err := releaseAdvisoryLock(ctx, conn, lockID); err != nil {
				log.WarnContext(ctx, "Failed to release advisory lock", sl.Error(err))
			}
		}(context.WithoutCancel(ctx), conn, lockID)

		log.InfoContext(ctx, "Running vacuum")
		start := time.Now()
		defer func() { vacuumDuration.Observe(time.Since(start).Seconds()) }()

		// VACUUM refuses to run inside a transaction, so issue it directly on
		// the raw connection.
		if _, err := conn.ExecContext(ctx, "VACUUM "+db.Table); err != nil {
			log.WarnContext(ctx, "Failed to run vacuum", sl.Error(err))
		}
	}
}

func sweepOutbox(ctx context.Context, db outboxDB, producer *kafka.Producer, batchSize uint) {
	log := slog.Default().With("component", "outburst")
	lockID := advisoryLockID("outburst:batchIter")

	conn, err := db.Connx(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get connection from database", sl.Error(err))
		return
	}

	defer startup_base.Close(conn, "Close database connection")

	// Serialize sweepers across instances behind a single advisory lock.
	locked, err := acquireAdvisoryLock(ctx, conn, lockID)
	if err != nil {
		slog.ErrorContext(ctx, "Lock connection", sl.Error(err))
		return
	}

	if !locked {
		return
	}

	defer func(ctx context.Context, conn *sqlx.Conn, lockID int64) {
		if err := releaseAdvisoryLock(ctx, conn, lockID); err != nil {
			slog.ErrorContext(ctx, "Failed to release connection lock", sl.Error(err))
		}
	}(context.WithoutCancel(ctx), conn, lockID)

	limit := batchSize
	for {
		start := time.Now()

		count, err := startup_tracing.TraceWithResult[uint](ctx, "sweepBatch", func(ctx context.Context, span trace.Span) (uint, error) {
			return sweepBatch(ctx, db, producer, limit)
		})

		iterationDuration.Observe(time.Since(start).Seconds())

		if err != nil {
			log.WarnContext(ctx, "Sweep failed", sl.Error(err))

			errorCounter.Inc()

			// Back off briefly before retrying.
			time.Sleep(1 * time.Second)

			continue
		}

		if count == 0 {
			debugLog(ctx, log, "Outbox empty, yielding until the next tick")
			return
		}

		// A full batch hints there may be more waiting, so keep going after a
		// short pause. A partial batch means we drained it and can hand control
		// back to the scheduler.
		if count == limit {
			debugLog(ctx, log, "Published batch to kafka", "count", count)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		debugLog(ctx, log, "Published batch to kafka", "count", count)
		return
	}
}

func acquireAdvisoryLock(ctx context.Context, conn *sqlx.Conn, lockID int64) (bool, error) {
	row := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lockID)

	var locked bool
	if err := row.Scan(&locked); err != nil {
		return false, fmt.Errorf("lock connection: %w", err)
	}

	return locked, nil
}

func releaseAdvisoryLock(ctx context.Context, conn *sqlx.Conn, lockID int64) error {
	_, err := conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", lockID)
	return err
}

// advisoryLockID derives a stable 64-bit advisory-lock key from a name.
func advisoryLockID(name string) int64 {
	digest := fnv.New64()
	_, _ = digest.Write([]byte(name))
	return int64(digest.Sum64()) // #nosec G115 -- advisory-lock keys are arbitrary 64-bit values
}

func sweepBatch(ctx context.Context, db outboxDB, producer *kafka.Producer, limit uint) (uint, error) {
	return ql.InNewTransactionWithResult(ctx, db, func(ctx ql.TxContext) (uint, error) {
		log := slog.Default()
		debugLog(ctx, log, "Selecting pending rows")

		query := fmt.Sprintf(`
			SELECT id, kafka_topic, kafka_key, kafka_value, kafka_header_keys, kafka_header_values
			FROM %s
			WHERE create_time < current_timestamp - interval '2' second
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		`, db.Table)

		rows, err := ql.Select[Message](ctx, query, limit)
		if err != nil {
			return 0, err
		}

		if len(rows) == 0 {
			return 0, nil
		}

		if err := publishToKafka(ctx, producer, rows, true); err != nil {
			return 0, fmt.Errorf("send: %w", err)
		}

		ids := make([]int64, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.ID)
		}

		deleteStmt := fmt.Sprintf(`DELETE FROM %s WHERE id=ANY($1)`, db.Table)
		if err := ql.Exec(ctx, deleteStmt, ids); err != nil {
			return 0, fmt.Errorf("delete rows: %w", err)
		}

		return uint(len(rows)), nil
	})
}

func forwardRow(ctx context.Context, db outboxDB, id int64, producer *kafka.Producer) error {
	return ql.InNewTransaction(ctx, db, func(ctx ql.TxContext) error {
		log := slog.Default()
		debugLog(ctx, log, "Selecting pending row")

		query := fmt.Sprintf(`
			SELECT id, kafka_topic, kafka_key, kafka_value, kafka_header_keys, kafka_header_values
			FROM %s
			WHERE id=$1
			FOR UPDATE SKIP LOCKED
		`, db.Table)

		row, err := ql.FirstOrNil[Message](ctx, query, id)
		if err != nil {
			return err
		}

		if row == nil {
			// Another instance already claimed or deleted this row.
			return nil
		}

		if err := publishToKafka(ctx, producer, []Message{*row}, false); err != nil {
			return fmt.Errorf("send: %w", err)
		}

		deleteStmt := fmt.Sprintf(`DELETE FROM %s WHERE id=$1`, db.Table)
		if err := ql.Exec(ctx, deleteStmt, row.ID); err != nil {
			return fmt.Errorf("delete row: %w", err)
		}

		return nil
	})
}

func publishToKafka(ctx context.Context, producer *kafka.Producer, rows []Message, batch bool) error {
	debugLog(ctx, slog.Default(), "Publishing rows to kafka", slog.Int("count", len(rows)))

	sendType := "single"
	operation := "publishSingle"
	if batch {
		sendType = "batch"
		operation = "publishBatch"
	}

	deliveries := make(chan kafka.Event, len(rows))

	return startup_tracing.Trace(ctx, operation, func(ctx context.Context, span trace.Span) error {
		for _, row := range rows {
			var key []byte
			if row.Key.Valid && len(row.Key.String) > 0 {
				key = []byte(row.Key.String)
			}

			var headers []kafka.Header
			if len(row.HeaderKeys) > 0 && len(row.HeaderValues) > 0 {
				for idx, hk := range row.HeaderKeys {
					headers = append(headers, kafka.Header{Key: hk, Value: []byte(row.HeaderValues[idx])})
				}
			}

			msg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &row.Topic, Partition: kafka.PartitionAny},
				Value:          row.Value,
				Key:            key,
				Timestamp:      row.Timestamp,
				Headers:        headers,
			}

			debugLog(ctx, slog.Default(), "Producing message", "id", row.ID)

			if err := producer.Produce(msg, deliveries); err != nil {
				return fmt.Errorf("produce message %d: %w", row.ID, err)
			}
			eventsCounter.WithLabelValues(row.Topic, sendType).Inc()
		}

		debugLog(ctx, slog.Default(), "Awaiting delivery reports")
		for range rows {
			var ev kafka.Event
			select {
			case ev = <-deliveries:
			case <-ctx.Done():
				// Don't hold the transaction open forever if delivery stalls.
				return ctx.Err()
			}

			debugLog(ctx, slog.Default(), "Delivery report", "event", ev)

			switch e := ev.(type) {
			case *kafka.Message:
				if e.TopicPartition.Error != nil {
					return fmt.Errorf("delivery failed for partition %d: %w",
						e.TopicPartition.Partition, e.TopicPartition.Error)
				}
			case kafka.Error:
				return fmt.Errorf("sending kafka event: %w", e)
			case error:
				return fmt.Errorf("sending kafka event: %w", e)
			default:
				// Unknown event type: fail closed so the row is retried instead
				// of being deleted as if it had been delivered.
				return fmt.Errorf("unexpected delivery event %T: %v", ev, ev)
			}
		}
		debugLog(ctx, slog.Default(), "All deliveries confirmed")
		return nil
	})
}

// Message is a single outbox row ready to be published to Kafka.
type Message struct {
	ID        int64     `db:"id"`
	Timestamp time.Time `db:"create_time"`

	Topic        string         `db:"kafka_topic"`
	Key          sql.NullString `db:"kafka_key"`
	Value        []byte         `db:"kafka_value"`
	HeaderKeys   ql.StringArray `db:"kafka_header_keys"`
	HeaderValues ql.StringArray `db:"kafka_header_values"`
}
