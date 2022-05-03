package events

import (
	"context"
	"database/sql"
	"github.com/jackc/pgtype"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

func WriteToOutbox(ctx context.Context, tx sqlx.ExecerContext, metadata EventMetadata, payload []byte) error {
	topic := metadata.Topic
	key := metadata.Key
	headers := metadata.Headers.ToJSON()

	header_keys := make([]string, 0, len(headers))
	header_values := make([]string, 0, len(headers))

	for _, header := range metadata.Headers {
		header_keys = append(header_keys, header.Key)
		header_values = append(header_keys, header.Value)
	}

	// insert event into database
	stmt := "INSERT INTO kafka_outbox (kafka_topic, kafka_key, kafka_value, kafka_header_keys, kafka_header_values) VALUES ($1, $2, $3, $4, $5)"
	_, err := tx.ExecContext(ctx, stmt, topic, toText(key), toTextArray(header_keys), toTextArray(header_values), payload)
	return errors.WithMessage(err, "write event into database")
}

func toText(value *string) pgtype.Text {
	var arr pgtype.Text
	_ = arr.Set(value)
	return arr
}
func toTextArray(values []string) pgtype.TextArray {
	var arr pgtype.TextArray
	_ = arr.Set(values)
	return arr
}

func CreateOutbox(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.WithMessage(err, "begin transaction")
	}

	createTable := `
		CREATE TABLE IF NOT EXISTS kafka_outbox (
			id                  BIGSERIAL NOT NULL PRIMARY KEY,
			create_time         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP ,
			leader_id           UUID NULL DEFAULT NULL,
			
			kafka_topic         TEXT NOT NULL,
			kafka_key           TEXT NULL,
			kafka_value         BYTEA NOT NULL,
			kafka_header_keys   TEXT[] NOT NULL,
			kafka_header_values TEXT[] NOT NULL,
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
