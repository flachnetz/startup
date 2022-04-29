package events

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"github.com/pkg/errors"
)

type PostgresEventSender struct {
	lookupTopic func(event Event) string
}

func (p *PostgresEventSender) Close() error {
	// All messages are send in transactions,
	// so we dont need to do anything here.
	return nil
}

func (p *PostgresEventSender) SendInTx(ctx context.Context, tx *sql.Tx, event Event) error {
	// parse kafka metadata from event
	topic, key, headers := p.metadataOf(event)

	// serialize event to bytes
	var buf bytes.Buffer
	if err := event.Serialize(&buf); err != nil {
		return errors.WithMessage(err, "serialize event")
	}

	// insert event into database
	stmt := "INSERT INTO kafka_outbox (topic, key, headers, schema, payload) VALUES ($1, $2, $3, $4, $5)"
	_, err := tx.ExecContext(ctx, stmt, topic, key, headers, event.Schema(), buf.Bytes())
	return errors.WithMessage(err, "write event into database")
}

func (p *PostgresEventSender) metadataOf(event Event) (topic string, key *string, headers []byte) {
	type pair struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	var headerPairs []pair

	if msg, ok := event.(*KafkaMessage); ok {
		key = &msg.Key

		for _, h := range msg.Headers {
			headerPairs = append(headerPairs, pair{
				Key:   string(h.Key),
				Value: string(h.Value),
			})
		}

		headers, _ = json.Marshal(headers)
		topic = p.lookupTopic(msg.Event)

	} else {
		topic = p.lookupTopic(event)
	}

	return
}

func CreateOutbox(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.WithMessage(err, "begin transaction")
	}

	createTable := `
		CREATE TABLE IF NOT EXISTS kafka_outbox (
		  id      BIGSERIAL NOT NULL PRIMARY KEY,
		
		  -- the topic to write the message to.
		  topic   TEXT      NOT NULL,
		
		  -- an optional key that will be used as the kafka message key
		  KEY     TEXT      NULL,
		
		  -- a json encoded list of {key, value} pairs
		  headers JSON      NULL,
		
		  -- the avro schema that was used to encode the payload
		  SCHEMA  JSON      NOT NULL,
		
		  -- the payload of this kafka message
		  payload BYTEA     NOT NULL
		)
	`

	if _, err := tx.ExecContext(ctx, createTable); err != nil {
		if err := tx.Rollback(); err != nil {
			log.Warnf("Failed to rollback create outbox table transaction: %s", err)
		}

		return errors.WithMessage(err, "create table")
	}

	if err := tx.Commit(); err != nil {
		return errors.WithMessage(err, "commit")
	}

	return nil
}
