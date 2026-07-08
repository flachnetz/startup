package kconsumer

import (
	"cmp"
	"context"
	"unicode/utf8"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func continueTrace(ctx context.Context, msg *kafka.Message, handler HandleMessage) error {
	tracer := otel.Tracer("")

	// Extract propagated context from incoming message headers
	ctx = otel.GetTextMapPropagator().Extract(ctx, (*messageCarrier)(msg))

	ctx, consumerSpan := tracer.Start(
		ctx, "kafka-consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("key", string(msg.Key)),
			attribute.String("topic", *cmp.Or(msg.TopicPartition.Topic, new(""))),
			attribute.Int("partition", int(msg.TopicPartition.Partition)),
		),
	)
	defer consumerSpan.End()

	// forward to handler
	err := handler(ctx, msg)
	if err != nil {
		consumerSpan.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

type messageCarrier kafka.Message

func (m *messageCarrier) Get(key string) string {
	message := (*kafka.Message)(m)

	for _, header := range message.Headers {
		if header.Key == key && utf8.Valid(header.Value) {
			return string(header.Value)
		}
	}

	return ""
}

func (m *messageCarrier) Set(key, value string) {
	message := (*kafka.Message)(m)

	for _, header := range message.Headers {
		if header.Key == key {
			header.Value = []byte(value)
			return
		}
	}

	message.Headers = append(message.Headers, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
}

func (m *messageCarrier) Keys() []string {
	var keys []string

	message := (*kafka.Message)(m)

	for _, header := range message.Headers {
		if utf8.Valid(header.Value) {
			keys = append(keys, header.Key)
		}
	}

	return keys
}
