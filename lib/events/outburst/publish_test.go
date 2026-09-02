package outburst

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/stretchr/testify/require"
)

// TestKafkaMessageOf covers the mapping from an outbox row to the produced
// message, in particular the key and header handling.
func TestKafkaMessageOf(t *testing.T) {
	ts := time.Date(2026, 4, 16, 10, 0, 0, 0, time.UTC)

	t.Run("key and headers are carried over", func(t *testing.T) {
		msg := kafkaMessageOf(Message{
			ID:           1,
			Timestamp:    ts,
			Topic:        "orders",
			Key:          sql.NullString{String: "order-1", Valid: true},
			Value:        []byte("payload"),
			HeaderKeys:   ql.StringArray{"actor-type", "actor-id"},
			HeaderValues: ql.StringArray{"user", "sub-1"},
		})

		require.Equal(t, "orders", *msg.TopicPartition.Topic)
		require.Equal(t, kafka.PartitionAny, msg.TopicPartition.Partition)
		require.Equal(t, []byte("order-1"), msg.Key)
		require.Equal(t, []byte("payload"), msg.Value)
		require.Equal(t, ts, msg.Timestamp)
		require.Equal(t, []kafka.Header{
			{Key: "actor-type", Value: []byte("user")},
			{Key: "actor-id", Value: []byte("sub-1")},
		}, msg.Headers)
	})

	t.Run("an invalid key stays nil", func(t *testing.T) {
		msg := kafkaMessageOf(Message{Topic: "orders", Key: sql.NullString{Valid: false}})
		require.Nil(t, msg.Key)
	})

	t.Run("an empty key stays nil", func(t *testing.T) {
		msg := kafkaMessageOf(Message{Topic: "orders", Key: sql.NullString{String: "", Valid: true}})
		require.Nil(t, msg.Key)
	})

	t.Run("header values without keys are dropped", func(t *testing.T) {
		msg := kafkaMessageOf(Message{Topic: "orders", HeaderValues: ql.StringArray{"user"}})
		require.Empty(t, msg.Headers)
	})
}

// TestDeliveryError covers the delivery report classification: only a message
// without a partition error counts as delivered, everything else must fail so
// the outbox row is retried.
func TestDeliveryError(t *testing.T) {
	topic := "orders"

	t.Run("delivered message is no error", func(t *testing.T) {
		require.NoError(t, deliveryError(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 2},
		}))
	})

	t.Run("partition error is reported", func(t *testing.T) {
		err := deliveryError(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: 2,
				Error:     kafka.NewError(kafka.ErrMsgTimedOut, "timed out", false),
			},
		})
		require.ErrorContains(t, err, "delivery failed for partition 2")
	})

	t.Run("kafka error is reported", func(t *testing.T) {
		err := deliveryError(kafka.NewError(kafka.ErrAllBrokersDown, "all brokers down", false))
		require.ErrorContains(t, err, "sending kafka event")
	})

	t.Run("plain error is reported", func(t *testing.T) {
		err := deliveryError(eventError{errors.New("boom")})
		require.ErrorContains(t, err, "sending kafka event")
	})

	t.Run("unknown event fails closed", func(t *testing.T) {
		err := deliveryError(kafka.OAuthBearerTokenRefresh{})
		require.ErrorContains(t, err, "unexpected delivery event")
	})
}

// eventError is a kafka.Event that is also a plain error, which is the third
// delivery report shape deliveryError has to handle.
type eventError struct{ err error }

func (e eventError) String() string { return e.err.Error() }
func (e eventError) Error() string  { return e.err.Error() }
