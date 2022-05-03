package events

import (
	"context"
	"database/sql"
	"github.com/pkg/errors"
)

func WriteToOutbox(ctx context.Context, tx *sql.Tx, metadata EventMetadata, payload []byte) error {
	// TODO adjust to goharvester table format

	topic := metadata.Topic
	key := metadata.Key
	headers := metadata.Headers.ToJSON()

	// insert event into database
	stmt := "INSERT INTO kafka_outbox (topic, key, headers, payload) VALUES ($1, $2, $3, $4)"
	_, err := tx.ExecContext(ctx, stmt, topic, key, headers, payload)
	return errors.WithMessage(err, "write event into database")
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
