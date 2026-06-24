package history

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/flachnetz/startup/v2/startup_base"
)

type AthenaQuery struct {
	GroupId GroupId

	// required athena configuration
	Database       string
	Table          string
	WorkGroup      string
	OutputLocation *url.URL

	// Optional region
	Region string

	// optional values to reduce the amount of data that
	// the query needs to scan. Either value can be left empty to
	MinTimestamp *time.Time
	MaxTimestamp *time.Time
}

func (q AthenaQuery) Records(ctx context.Context) ([]Record, error) {
	values := url.Values{}
	values.Set("db", q.Database)
	values.Set("region", q.Region)
	values.Set("workgroup", q.WorkGroup)
	values.Set("output_location", q.OutputLocation.String())
	values.Set("poll_frequency", "1s")

	// open athena driver
	db, err := sql.Open("athena", values.Encode())
	if err != nil {
		return nil, fmt.Errorf("open athena database: %w", err)
	}

	defer startup_base.Close(db, "Closing athena")

	var records []Record

	// run the athena query
	rows, err := db.QueryContext(ctx, queryOf(q.Table, q.GroupId.String(), q.MinTimestamp, q.MaxTimestamp))
	if err != nil {
		return nil, fmt.Errorf("start query: %w", err)
	}

	defer startup_base.Close(rows, "Closing rows")

	for rows.Next() {
		var timestamp time.Time
		var historyId, step, description, payload, eventSender, eventSenderVersion string
		var requestTraceId RequestTraceId

		// scan values into variables
		if err := rows.Scan(&timestamp, &historyId, &requestTraceId, &step, &description, &payload, &eventSender, &eventSenderVersion); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		records = append(records, Record{
			Timestamp:          timestamp,
			RequestTraceId:     requestTraceId,
			Step:               step,
			Description:        description,
			Payload:            json.RawMessage(payload),
			EventSender:        eventSender,
			EventSenderVersion: eventSenderVersion,
		})
	}

	return records, nil
}

func queryOf(table string, historyId string, minTimestamp, maxTimestamp *time.Time) string {
	query := fmt.Sprintf(`
		SELECT timestamp, historyId, COALESCE(requesttraceid, '00'), step, description, payload, eventsender, eventsenderversion
		FROM %s
		WHERE groupid='%s'
	`, table, escapeAthenaLiteral(historyId))

	if minTimestamp != nil {
		formatted := minTimestamp.In(time.UTC).Format("2006-01-02 15:04:05")
		query += fmt.Sprintf(" AND timestamp >= timestamp '%s'", formatted)
	}

	if maxTimestamp != nil {
		formatted := maxTimestamp.In(time.UTC).Format("2006-01-02 15:04:05")
		query += fmt.Sprintf(" AND timestamp <= timestamp '%s'", formatted)
	}

	return query
}

// escapeAthenaLiteral escapes a value for safe use inside a single-quoted Athena/Presto
// string literal by doubling any embedded single quotes.
func escapeAthenaLiteral(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
