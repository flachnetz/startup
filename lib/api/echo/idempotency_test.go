package echo

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flachnetz/pgtest/v2"
	"github.com/flachnetz/startup/v2/lib/api/echox"
	"github.com/flachnetz/startup/v2/lib/api/idempotency"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
)

// idempotentRoute serves POST /orders behind the idempotency middleware. The
// returned call helper sends a request, calls counts how often the handler ran.
func idempotentRoute(t *testing.T, handler echo.HandlerFunc) (idempotency.IdempotencyStore, *int, func(method, key string) *httptest.ResponseRecorder) {
	t.Helper()

	db := sqlx.NewDb(pgtest.Connect(t), "pgx")

	store, err := idempotency.NewIdempotencyStore(db, 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	calls := 0

	counting := func(c *echo.Context) error {
		calls++

		return handler(c)
	}

	e := echo.New()
	// the api.Error the middleware returns only becomes a 400 through the
	// project's error handler
	e.HTTPErrorHandler = echox.ErrorHandler(nil)
	e.Add(http.MethodPost, "/orders", counting, IdempotencyMiddlewareEcho(store))
	e.Add(http.MethodGet, "/orders", counting, IdempotencyMiddlewareEcho(store))

	return store, &calls, func(method, key string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/orders", strings.NewReader(`{"amount":10}`))
		req.Header.Set("Content-Type", "application/json")

		if key != "" {
			req.Header.Set(IdempotencyKey, key)
		}

		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		return rec
	}
}

// createdHandler answers 201 with a JSON body and a custom response header.
func createdHandler(c *echo.Context) error {
	c.Response().Header().Set("Order-Id", "order-1")

	return c.JSON(http.StatusCreated, map[string]string{"id": "order-1"})
}

func TestIdempotency_ReadMethodsPassThrough(t *testing.T) {
	_, calls, call := idempotentRoute(t, createdHandler)

	// no idempotency key needed for a GET
	rec := call(http.MethodGet, "")
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, 1, *calls)
}

func TestIdempotency_MissingKeyIsRejected(t *testing.T) {
	_, calls, call := idempotentRoute(t, createdHandler)

	rec := call(http.MethodPost, "")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, 0, *calls, "handler must not run without an idempotency key")
}

func TestIdempotency_FirstRequestRunsHandlerAndIsStored(t *testing.T) {
	store, calls, call := idempotentRoute(t, createdHandler)

	rec := call(http.MethodPost, "key-1")
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, 1, *calls)
	require.JSONEq(t, `{"id":"order-1"}`, rec.Body.String())

	record := mustGet(t, store, "key-1")
	require.Equal(t, idempotency.Completed, record.Status)
	require.Equal(t, int64(http.StatusCreated), record.ResponseCode.Int64)
	require.JSONEq(t, `{"id":"order-1"}`, string(record.ResponseBody))

	var headers http.Header
	require.NoError(t, json.Unmarshal(record.ResponseHeaders, &headers))
	require.Equal(t, "order-1", headers.Get("Order-Id"))
}

func TestIdempotency_ReplayReturnsStoredResponse(t *testing.T) {
	_, calls, call := idempotentRoute(t, createdHandler)

	first := call(http.MethodPost, "key-1")
	require.Equal(t, http.StatusCreated, first.Code)

	replay := call(http.MethodPost, "key-1")
	require.Equal(t, 1, *calls, "handler must run only once per key")
	require.Equal(t, http.StatusCreated, replay.Code)
	require.JSONEq(t, first.Body.String(), replay.Body.String())
	require.Equal(t, "key-1", replay.Header().Get(IdempotencyKey))
	require.Equal(t, "order-1", replay.Header().Get("Order-Id"))
	require.Contains(t, replay.Header().Get("Content-Type"), "application/json")
}

func TestIdempotency_DifferentKeysRunSeparately(t *testing.T) {
	_, calls, call := idempotentRoute(t, createdHandler)

	require.Equal(t, http.StatusCreated, call(http.MethodPost, "key-1").Code)
	require.Equal(t, http.StatusCreated, call(http.MethodPost, "key-2").Code)
	require.Equal(t, 2, *calls)
}

func TestIdempotency_ErrorResponseIsRetried(t *testing.T) {
	failing := func(c *echo.Context) error {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "boom"})
	}

	store, calls, call := idempotentRoute(t, failing)

	require.Equal(t, http.StatusInternalServerError, call(http.MethodPost, "key-1").Code)
	require.Equal(t, idempotency.Error, mustGet(t, store, "key-1").Status)

	// a stored error must not be replayed - the business logic runs again
	require.Equal(t, http.StatusInternalServerError, call(http.MethodPost, "key-1").Code)
	require.Equal(t, 2, *calls)
}

func TestIdempotency_PendingKeyIsRejected(t *testing.T) {
	store, calls, call := idempotentRoute(t, createdHandler)

	require.NoError(t, insertPending(t, store, "key-1", time.Now()))

	rec := call(http.MethodPost, "key-1")
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, 0, *calls, "a pending key must not run the handler again")
}

func TestIdempotency_StuckPendingKeyIsRejected(t *testing.T) {
	store, calls, call := idempotentRoute(t, createdHandler)

	require.NoError(t, insertPending(t, store, "key-1", time.Now().Add(-5*time.Minute)))

	rec := call(http.MethodPost, "key-1")
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, 0, *calls)
}

// mustGet reads one stored record, failing the test when it is missing.
func mustGet(t *testing.T, store idempotency.IdempotencyStore, key string) *idempotency.IdempotencyRequest {
	t.Helper()

	var record idempotency.IdempotencyRequest
	require.NoError(t, store.DB().GetContext(t.Context(), &record,
		`SELECT * FROM idempotency_requests WHERE idempotency_key = $1`, key))

	return &record
}

// insertPending writes a pending record with an explicit creation time, which is
// what decides between "still pending" and "stuck".
func insertPending(t *testing.T, store idempotency.IdempotencyStore, key string, createdAt time.Time) error {
	t.Helper()

	_, err := store.DB().ExecContext(t.Context(),
		`INSERT INTO idempotency_requests (idempotency_key, status, created_at) VALUES ($1, 'pending', $2)`,
		key, createdAt)

	return err
}
