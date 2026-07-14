package outburst

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/flachnetz/startup/v2/lib/ql"
)

// ensureOutboxTable creates the outbox table when it does not already exist.
func ensureOutboxTable(ctx context.Context, db outboxDB) error {
	return ql.InNewTransaction(ctx, db, func(ctx ql.TxContext) error {
		createTable := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id                  bigserial NOT NULL PRIMARY KEY,
				create_time         timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP ,

				kafka_topic         text NOT NULL,
				kafka_key           text NULL,
				kafka_value         BYTEA NOT NULL,
				kafka_header_keys   text[] NOT NULL,
				kafka_header_values text[] NOT NULL
			)
			`, db.Table)

		slog.InfoContext(ctx, "Create outbox table", slog.String("table", db.Table))
		return ql.Exec(ctx, createTable)
	})
}
