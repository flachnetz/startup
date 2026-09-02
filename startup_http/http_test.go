package startup_http

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/felixge/httpsnoop"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/stretchr/testify/require"
)

// okHandler answers every request with 200 and a fixed body.
var okHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	_, _ = io.WriteString(w, "ok")
})

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

	return rec
}

func TestRequireAuth(t *testing.T) {
	t.Run("protected route needs credentials", func(t *testing.T) {
		rec := get(requireAuth(false, "admin", "bingo", okHandler), "/admin/")
		require.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("correct credentials pass", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin/", nil)
		req.SetBasicAuth("admin", "bingo")

		rec := httptest.NewRecorder()
		requireAuth(false, "admin", "bingo", okHandler).ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ping is always reachable", func(t *testing.T) {
		rec := get(requireAuth(false, "admin", "bingo", okHandler), "/admin/ping")
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("auth can be disabled", func(t *testing.T) {
		rec := get(requireAuth(true, "admin", "bingo", okHandler), "/admin/")
		require.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestMergeWithAdminHandler(t *testing.T) {
	adminHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "admin")
	})
	rest := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "rest")
	})

	merged := mergeWithAdminHandler(adminHandler, rest)

	require.Equal(t, "admin", get(merged, "/admin").Body.String())
	require.Equal(t, "admin", get(merged, "/admin/pprof").Body.String())
	require.Equal(t, "rest", get(merged, "/administration").Body.String())
	require.Equal(t, "rest", get(merged, "/orders/v1").Body.String())
}

func TestTryRegisterAdminHandlerRedirect(t *testing.T) {
	t.Run("registers the redirect", func(t *testing.T) {
		router := http.NewServeMux()
		tryRegisterAdminHandlerRedirect(router)

		rec := get(router, "/")
		require.Equal(t, http.StatusTemporaryRedirect, rec.Code)
		require.Equal(t, "/admin", rec.Header().Get("Location"))
	})

	t.Run("a conflicting route does not panic", func(t *testing.T) {
		router := http.NewServeMux()
		router.Handle("GET /", okHandler)

		require.NotPanics(t, func() { tryRegisterAdminHandlerRedirect(router) })
		require.Equal(t, "ok", get(router, "/").Body.String())
	})
}

func TestUpdateLogLevelHandler(t *testing.T) {
	handler := logLevelHandler()

	t.Run("GET reports the current level", func(t *testing.T) {
		startup_base.LogLevel.Set(slog.LevelInfo)

		rec := get(handler, "/log/level")
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "INFO", rec.Body.String())
	})

	t.Run("POST sets the level", func(t *testing.T) {
		startup_base.LogLevel.Set(slog.LevelInfo)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/log/level", strings.NewReader("debug\n")))

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, slog.LevelDebug, startup_base.LogLevel.Level())

		startup_base.LogLevel.Set(slog.LevelInfo)
	})

	t.Run("POST rejects an unknown level", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/log/level", strings.NewReader("shout")))

		require.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("other methods are rejected", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/log/level", nil))

		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	})
}

func TestBuildAccessLogAttrs(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/orders/v1?debug=1", nil)
	req.RemoteAddr = "10.0.0.7:54321"

	attrs := attrMap(buildAccessLogAttrs(req, httpsnoop.Metrics{
		Code:     201,
		Written:  17,
		Duration: 250 * time.Millisecond,
	}))

	require.Equal(t, "10.0.0.7", attrs["host"])
	require.Equal(t, "GET", attrs["method"])
	require.Equal(t, "/orders/v1?debug=1", attrs["uri"])
	require.Equal(t, "HTTP/1.1", attrs["proto"])
	require.Equal(t, "201", attrs["status"])
	require.Equal(t, "17", attrs["size"])
	require.Equal(t, "250ms", attrs["latency"])
}

func TestBuildAccessLogAttrsWithoutPort(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.7"

	attrs := attrMap(buildAccessLogAttrs(req, httpsnoop.Metrics{Code: 200}))
	require.Equal(t, "10.0.0.7", attrs["host"])
}

// attrMap renders log attributes as plain strings, so a test can assert on
// single fields.
func attrMap(attrs []slog.Attr) map[string]string {
	out := make(map[string]string, len(attrs))
	for _, a := range attrs {
		out[a.Key] = a.Value.String()
	}

	return out
}
