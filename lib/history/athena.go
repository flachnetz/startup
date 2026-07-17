package history

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/flachnetz/startup/v2/lib/clock"
	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/flachnetz/startup/v2/startup_base"
	sl "github.com/flachnetz/startup/v2/startup_logging"

	// registers the "athena" database/sql driver used by AthenaQuery.
	_ "github.com/speee/go-athena"
)

// Default thresholds applied by RecordsAt when AthenaConfig leaves them zero.
const (
	defaultLookupThreshold = 24 * time.Hour
	defaultLookbackMargin  = 30 * time.Minute
)

// AthenaConfig enables the Athena read fallback on a Service (see WithAthena).
type AthenaConfig struct {
	// required Athena configuration
	Database       string
	Table          string
	WorkGroup      string
	OutputLocation string

	// optional AWS region
	Region string

	// LookupThreshold selects Athena over the local table once the tracked
	// object is older than this. Defaults to 24h when zero.
	LookupThreshold time.Duration

	// LookbackMargin is subtracted from the object creation time to form the
	// Athena query's MinTimestamp, bounding the scanned partitions. Defaults
	// to 30m when zero.
	LookbackMargin time.Duration
}

// RecordsAt returns the records for groupId. With Athena configured (WithAthena)
// it bridges the local table and long-term Athena storage, because a cleanup job
// deletes local rows after a retention window while Athena keeps them forever:
//
//   - createdTime older than AthenaConfig.LookupThreshold: read from Athena,
//     falling back to the local table if Athena fails.
//   - createdTime within the threshold: read the local table only.
//   - createdTime zero (age unknown): read the local table first and, only when
//     it returns nothing, fall back to Athena — so records aged out of the local
//     table are still found.
//
// Any Athena failure is logged and never fails the read.
func (h *Service) RecordsAt(ctx ql.TxContext, groupId GroupId, createdTime time.Time) ([]Record, error) {
	cfg := h.athena
	if cfg == nil {
		return h.Records(ctx, groupId)
	}

	threshold := cfg.LookupThreshold
	if threshold <= 0 {
		threshold = defaultLookupThreshold
	}

	knownOld := !createdTime.IsZero() && clock.GlobalClock.Now().Sub(createdTime) > threshold
	if knownOld {
		if records, ok := h.recordsFromAthena(ctx, cfg, groupId, createdTime); ok {
			return records, nil
		}
		return h.Records(ctx, groupId)
	}

	records, err := h.Records(ctx, groupId)
	if err != nil {
		return nil, err
	}

	// ponytail: unknown-age group with an empty local table always hits Athena
	// (aged out, or never existed). Fine for the rare history-page render; pass
	// createdTime when the caller knows it to skip this probe.
	if len(records) == 0 && createdTime.IsZero() {
		if athenaRecords, ok := h.recordsFromAthena(ctx, cfg, groupId, time.Time{}); ok {
			return athenaRecords, nil
		}
	}

	return records, nil
}

// recordsFromAthena runs the Athena query for groupId. A non-zero createdTime
// bounds the scan to createdTime-LookbackMargin; a zero createdTime means no
// lower bound (full scan). ok is false when Athena is misconfigured or errored,
// signalling the caller to fall back to the local table.
func (h *Service) recordsFromAthena(ctx ql.TxContext, cfg *AthenaConfig, groupId GroupId, createdTime time.Time) (_ []Record, ok bool) {
	loc, err := url.Parse(cfg.OutputLocation)
	if err != nil {
		sl.LoggerOf(ctx).WarnContext(ctx, "invalid athena output location, using local history", sl.Error(err))
		return nil, false
	}

	var minTimestamp *time.Time
	if !createdTime.IsZero() {
		margin := cfg.LookbackMargin
		if margin <= 0 {
			margin = defaultLookbackMargin
		}
		minTimestamp = new(createdTime.Add(-margin))
	}

	query := AthenaQuery{
		GroupId:        groupId,
		Database:       cfg.Database,
		Table:          cfg.Table,
		WorkGroup:      cfg.WorkGroup,
		OutputLocation: loc,
		Region:         cfg.Region,
		MinTimestamp:   minTimestamp,
	}

	queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	records, err := query.Records(queryCtx)
	if err != nil {
		sl.LoggerOf(ctx).WarnContext(ctx, "failed to load history from athena, using local history", sl.Error(err))
		return nil, false
	}

	return records, true
}

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
	rows, err := db.QueryContext(ctx, queryOf(q.Table, q.GroupId, q.MinTimestamp, q.MaxTimestamp))
	if err != nil {
		return nil, fmt.Errorf("start query: %w", err)
	}

	defer startup_base.Close(rows, "Closing rows")

	for rows.Next() {
		var timestamp time.Time
		var historyId, step, description, payload, eventSender, eventSenderVersion string
		var requestTraceId RequestTraceId
		var trSource, trDetail, trRefType, trRef string

		// scan values into variables
		if err := rows.Scan(&timestamp, &historyId, &requestTraceId, &step, &description, &payload, &eventSender, &eventSenderVersion, &trSource, &trDetail, &trRefType, &trRef); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		records = append(records, Record{
			Timestamp:          timestamp,
			RequestTraceId:     requestTraceId,
			Step:               step,
			Description:        description,
			Payload:            json.RawMessage(payload),
			Trigger:            Trigger{Source: trSource, Detail: trDetail, RefType: trRefType, Ref: trRef},
			EventSender:        eventSender,
			EventSenderVersion: eventSenderVersion,
		})
	}

	return records, nil
}

func queryOf(table string, groupId GroupId, minTimestamp, maxTimestamp *time.Time) string {
	query := fmt.Sprintf(`
		SELECT timestamp, historyId, COALESCE(requesttraceid, '00'), step, description, payload, eventsender, eventsenderversion,
		       COALESCE("trigger".source, ''), COALESCE("trigger".detail, ''), COALESCE("trigger".reftype, ''), COALESCE("trigger".ref, '')
		FROM %s
		WHERE groupid='%s'
	`, table, escapeAthenaLiteral(groupId.String()))

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
