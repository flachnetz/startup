package events

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/flachnetz/startup/v2/lib"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

func WriteToOutbox(ctx context.Context, tx sqlx.ExecerContext, metadata EventMetadata, payload []byte) error {
	topic := metadata.Topic
	key := metadata.Key

	if key == nil {
		key = lib.PtrOf(fmt.Sprintf("%d", time.Now().UnixMilli()))
	}

	headerKeys := make([]string, 0, len(metadata.Headers))
	headerValues := make([]string, 0, len(metadata.Headers))

	for _, header := range metadata.Headers {
		headerKeys = append(headerKeys, header.Key)
		headerValues = append(headerValues, header.Value)
	}

	// insert event into database and notify listeners
	stmt := `
		WITH
			ids AS (
				INSERT INTO public.kafka_outbox (kafka_topic, kafka_key, kafka_value, kafka_header_keys, kafka_header_values)
				VALUES ($1, $2, $3, $4, $5)
				RETURNING id)
		
		SELECT pg_notify('kafka-message', id::text)
		FROM ids;
	`
	_, err := tx.ExecContext(ctx, stmt, topic, key, payload, headerKeys, headerValues)
	return errors.WithMessage(err, "write event into database")
}

func CreateOutbox(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.WithMessage(err, "begin transaction")
	}

	createTable := `
		CREATE TABLE IF NOT EXISTS PUBLIC.kafka_outbox (
			id                  bigserial NOT NULL PRIMARY KEY,
			create_time         timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP ,
			leader_id           UUID NULL DEFAULT NULL,
			
			kafka_topic         text NOT NULL,
			kafka_key           text NOT NULL,
			kafka_value         BYTEA NOT NULL,
			kafka_header_keys   text[] NOT NULL,
			kafka_header_values text[] NOT NULL
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
