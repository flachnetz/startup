package idempotency

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"

	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
)

type Status string

const (
	Pending   Status = "pending"
	Completed Status = "completed"
)

type IdempotencyStore interface {
	io.Closer
	Get(ctx context.Context, key string) (*IdempotencyRequest, error)
	Create(ctx context.Context, key string) error
	Update(ctx context.Context, key string, code int, headers, body []byte) error
	Cleanup(ctx context.Context) error
	DB() *sqlx.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS idempotency_requests (
    idempotency_key TEXT PRIMARY KEY,
    
    -- The status of the request processing
    status TEXT NOT NULL CHECK (status IN ('pending', 'completed')),
    
    -- The response data to be returned on subsequent requests
    response_code INT,
    response_headers BYTEA,
    response_body BYTEA,

    -- Timestamps for tracking and potential cleanup
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);
`

// IdempotencyRequest represents a stored request in the database.
type IdempotencyRequest struct {
	Key             string        `db:"idempotency_key"`
	Status          Status        `db:"status"`
	ResponseCode    sql.NullInt64 `db:"response_code"`
	ResponseHeaders []byte        `db:"response_headers"`
	ResponseBody    []byte        `db:"response_body"`
	CreatedAt       time.Time     `db:"created_at"`
	UpdatedAt       sql.NullTime  `db:"updated_at"`
}

// idempotencyStore handles the database operations for idempotency checks.
type idempotencyStore struct {
	db                     *sqlx.DB
	cleanUpThresholdInDays int
	c                      *cron.Cron
}

// NewIdempotencyStore creates a new store for idempotency checks.
func NewIdempotencyStore(db *sqlx.DB, cleanUpThresholdInDays int) (IdempotencyStore, error) {
	_, err := db.ExecContext(context.Background(), schema)
	if err != nil {
		return nil, fmt.Errorf("failed to create idempotency_requests table: %w", err)
	}
	i := &idempotencyStore{db: db, cleanUpThresholdInDays: cleanUpThresholdInDays, c: cron.New()}
	// Schedule the cleanup job to run daily at midnight
	_, err = i.c.AddFunc("@daily", func() {
		ctx := context.Background()
		err := i.Cleanup(ctx)
		if err != nil {
			startup_logrus.LoggerOf(ctx).WithError(err).Error("Failed to clean up old idempotency records")
		}
	})
	if err != nil {
		return nil, errors.Errorf("failed to schedule idempotency cleanup job: %q", err)
	}
	i.c.Start()
	return i, nil
}

// Close stops the cron scheduler
func (s *idempotencyStore) Close() error {
	s.c.Stop()
	return nil
}

// DB returns the underlying database connection.
func (s *idempotencyStore) DB() *sqlx.DB {
	return s.db
}

// Get finds an idempotency request by its key. It uses a transaction
// and `SELECT ... FOR UPDATE` to lock the row, preventing race conditions
// from concurrent requests with the same key.
func (s *idempotencyStore) Get(ctx context.Context, key string) (*IdempotencyRequest, error) {
	req, err := ql.InExistingTransactionWithResult(ctx, func(ctx ql.TxContext) (*IdempotencyRequest, error) {
		req, err := ql.Get[IdempotencyRequest](ctx, `
			SELECT * 
			FROM idempotency_requests 
			WHERE idempotency_key = $1 FOR UPDATE`, key)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to get idempotency key: %w", err)
		}
		return req, nil
	})

	return req, err
}

// Create saves a new idempotency request in the 'pending' state.
// This should be called within the transaction started by `Get`.
func (s *idempotencyStore) Create(ctx context.Context, key string) error {
	return ql.InExistingTransaction(ctx, func(ctx ql.TxContext) error {
		return ql.Exec(ctx, `
		INSERT INTO idempotency_requests (idempotency_key, status) 
		VALUES ($1, 'pending')`, key)
	})
}

// Update saves the final response of a completed request.
// This should be called within the transaction started by `Get`.
func (s *idempotencyStore) Update(ctx context.Context, key string, code int, headers, body []byte) error {
	return ql.InExistingTransaction(ctx, func(ctx ql.TxContext) error {
		return ql.Exec(ctx, `	
		UPDATE idempotency_requests 
		SET status = 'completed', response_code = $2, response_headers = $3, response_body = $4, updated_at = NOW()
		WHERE idempotency_key = $1
    `, key, code, headers, body)
	})
}

// Cleanup removes old, completed idempotency records from the store.
func (s *idempotencyStore) Cleanup(ctx context.Context) error {
	startup_logrus.LoggerOf(ctx).Infof("Cleaning up old idempotency records older than %d days", s.cleanUpThresholdInDays)
	threshold := time.Now().Add(-24 * time.Hour * time.Duration(s.cleanUpThresholdInDays))
	return ql.InAnyTransaction(ctx, s.db, func(ctx ql.TxContext) error {
		return ql.Exec(ctx, `
		DELETE FROM idempotency_requests 
		WHERE status = 'completed' AND updated_at < $1
	`, threshold)
	})
}
