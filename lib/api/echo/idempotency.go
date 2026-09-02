package echo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/flachnetz/startup/v2/lib/api"

	"github.com/flachnetz/startup/v2/lib/api/idempotency"
	"github.com/flachnetz/startup/v2/lib/ql"
	sl "github.com/flachnetz/startup/v2/startup_logging"
	"github.com/labstack/echo/v5"
)

const IdempotencyKey = "Idempotency-Key"

// responseWriterInterceptor captures the response body and status code.
type responseWriterInterceptor struct {
	http.ResponseWriter
	body       *bytes.Buffer
	header     http.Header
	statusCode int
}

func (w *responseWriterInterceptor) Write(b []byte) (int, error) {
	if w.body == nil {
		w.body = bytes.NewBufferString("")
	}
	w.body.Write(b)
	return w.ResponseWriter.Write(b)
}

func (w *responseWriterInterceptor) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	for key, values := range w.header {
		for _, value := range values {
			w.ResponseWriter.Header().Add(key, value)
		}
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

// Header returns the response headers.
func (w *responseWriterInterceptor) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

// stuckPendingAfter is how long a pending record may stay pending before it is
// reported as stuck rather than as "retry later".
const stuckPendingAfter = 2 * time.Minute

// IdempotencyMiddlewareEcho provides idempotency for Echo handlers.
func IdempotencyMiddlewareEcho(store idempotency.IdempotencyStore) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			// Only apply to methods that change state
			if !changesState(c.Request().Method) {
				return next(c)
			}

			ctx := c.Request().Context()

			key := c.Request().Header.Get(IdempotencyKey)
			if key == "" {
				return api.ErrBadRequest.WithDescription("missing idempotency key")
			}

			logger := sl.LoggerOf(ctx).With(slog.String("idempotency_key", key))

			return ql.InNewTransaction(ctx, store.DB(), func(ctx ql.TxContext) error {
				record, err := store.Get(ctx, key)
				if err != nil {
					return fmt.Errorf("failed to retrieve idempotency record: %w", err)
				}

				if record != nil {
					handled, err := replayRecord(ctx, c, logger, key, record)
					if handled {
						return err
					}
				}

				return runAndStore(ctx, c, next, store, logger, key)
			})
		}
	}
}

// changesState reports whether a request method needs idempotency protection.
func changesState(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPatch, http.MethodPut:
		return true
	default:
		return false
	}
}

// replayRecord deals with an existing record for this key. It runs inside the
// middleware's transaction, so ctx is the transaction context, not the plain
// request context. It reports whether
// the request is already settled by that record: a completed request is
// answered from the store, a pending one is refused, and a previously failed
// one is left to the caller so the business logic runs again.
func replayRecord(ctx ql.TxContext, c *echo.Context, logger *slog.Logger, key string, record *idempotency.IdempotencyRequest) (bool, error) {
	switch record.Status {
	case idempotency.Completed:
		logger.DebugContext(ctx, "idempotency key already processed. Returning saved response")

		return true, writeStoredResponse(c, key, record)

	case idempotency.Pending:
		// if it is still pending for more than two minutes, we can assume it is stuck
		if time.Since(record.CreatedAt) > stuckPendingAfter {
			return true, fmt.Errorf("idempotency key %q is stuck in pending state", key)
		}

		return true, fmt.Errorf("idempotency key %q is still pending, please retry later", key)

	case idempotency.Error:
		logger.DebugContext(ctx, "idempotency key resulted in an error, will retry business logic")

		return false, nil

	default:
		return false, nil
	}
}

// writeStoredResponse replays the stored response, including its headers.
func writeStoredResponse(c *echo.Context, key string, record *idempotency.IdempotencyRequest) error {
	var headers http.Header
	if err := json.Unmarshal(record.ResponseHeaders, &headers); err == nil {
		for name, values := range headers {
			for _, value := range values {
				c.Response().Header().Add(name, value)
			}
		}
	}

	// add idempotency key to response headers
	c.Response().Header().Set(IdempotencyKey, key)

	// Use Blob to write the raw body with the correct status code and content type
	return c.Blob(int(record.ResponseCode.Int64), headers.Get("Content-Type"), record.ResponseBody)
}

// runAndStore creates the pending record, runs the handler and stores whatever
// it answered, so a repeat of the same key is answered from the store. It runs
// inside the middleware's transaction, so ctx is the transaction context: the
// store calls must see it, while the handler keeps working on the request.
func runAndStore(ctx ql.TxContext, c *echo.Context, next echo.HandlerFunc, store idempotency.IdempotencyStore, logger *slog.Logger, key string) error {
	// Handle new requests: Create pending record
	if err := store.Create(ctx, key); err != nil {
		return fmt.Errorf("failed to create idempotency record for key %q: %w", key, err)
	}

	// Call the actual handler and capture the response
	originalWriter := c.Response()
	interceptor := &responseWriterInterceptor{
		ResponseWriter: originalWriter,
		body:           bytes.NewBufferString(""),
		statusCode:     http.StatusOK, // Default
		header:         make(http.Header),
	}

	c.SetResponse(interceptor)
	c.SetRequest(c.Request().Clone(ctx))

	handlerErr := next(c)

	c.SetResponse(originalWriter)

	headersBytes, err := json.Marshal(interceptor.Header())
	if err != nil {
		// Log the error but do not return it to avoid breaking the response flow
		logger.ErrorContext(ctx, "Failed to marshal response headers", sl.Error(err))
	}

	if handlerErr != nil || interceptor.statusCode >= 400 {
		if err := store.Error(ctx, key, interceptor.statusCode, headersBytes, interceptor.body.Bytes()); err != nil {
			logger.ErrorContext(ctx, "Failed to store idempotency error", sl.Error(err))
		}

		return handlerErr
	}

	if err := store.Update(ctx, key, interceptor.statusCode, headersBytes, interceptor.body.Bytes()); err != nil {
		// Log the error but do not return it to avoid breaking the response flow
		logger.ErrorContext(ctx, "Failed to update idempotency record", sl.Error(err))
	}

	return nil
}
