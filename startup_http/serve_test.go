package startup_http

import (
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildHandlerRoutesAppAdminAndRedirect(t *testing.T) {
	opts := &HTTPOptions{
		BasicAuthUsername: "admin",
		BasicAuthPassword: "bingo",
		DisableAuth:       true,
		AccessLog:         "/dev/null",
	}

	handler := opts.buildHandler(Config{
		Name: "test-service",
		Routing: func(mux *http.ServeMux) http.Handler {
			mux.Handle("GET /orders/v1", okHandler)

			return mux
		},
	})

	require.Equal(t, "ok", get(handler, "/orders/v1").Body.String())

	redirect := get(handler, "/")
	require.Equal(t, http.StatusTemporaryRedirect, redirect.Code)
	require.Equal(t, "/admin", redirect.Header().Get("Location"))

	require.Equal(t, http.StatusOK, get(handler, "/admin/ping").Code)
}

func TestBuildHandlerRequiresAdminAuth(t *testing.T) {
	opts := &HTTPOptions{
		BasicAuthUsername: "admin",
		BasicAuthPassword: "bingo",
		AccessLog:         "/dev/null",
	}

	handler := opts.buildHandler(Config{Name: "test-service"})

	require.Equal(t, http.StatusUnauthorized, get(handler, "/admin/").Code)
	// ping stays open, so a liveness probe needs no login
	require.Equal(t, http.StatusOK, get(handler, "/admin/ping").Code)
}

func TestBuildHandlerRecoversPanic(t *testing.T) {
	opts := &HTTPOptions{DisableAuth: true, AccessLog: "/dev/null"}

	handler := opts.buildHandler(Config{
		Routing: func(mux *http.ServeMux) http.Handler {
			mux.HandleFunc("GET /boom", func(w http.ResponseWriter, r *http.Request) {
				panic("boom")
			})

			return mux
		},
	})

	require.Equal(t, http.StatusInternalServerError, get(handler, "/boom").Code)
}

func TestBuildHandlerAppliesMiddleware(t *testing.T) {
	opts := &HTTPOptions{DisableAuth: true, AccessLog: "/dev/null"}

	handler := opts.buildHandler(Config{
		Routing: func(mux *http.ServeMux) http.Handler { return okHandler },
		UseMiddleware: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Middleware", "yes")
				next.ServeHTTP(w, r)
			})
		},
	})

	require.Equal(t, "yes", get(handler, "/orders/v1").Header().Get("Middleware"))
}

func TestWithAccessLogWritesToFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "access.log")
	opts := &HTTPOptions{AccessLog: file}

	get(opts.withAccessLog(okHandler), "/orders/v1")

	content, err := os.ReadFile(file) // #nosec G304 -- path is this test's temp dir
	require.NoError(t, err)
	require.Contains(t, string(content), "uri=/orders/v1")
	require.Contains(t, string(content), "status=200")
}

func TestWithAccessLogDevNullDoesNotWrap(t *testing.T) {
	opts := &HTTPOptions{AccessLog: "/dev/null"}

	_, wrapped := opts.withAccessLog(okHandler).(loggingHandler)
	require.False(t, wrapped, "/dev/null must not install an access log")
}

func TestIsAdminRoute(t *testing.T) {
	require.True(t, isAdminRoute([]slog.Attr{slog.String("uri", "/admin/ping")}))
	require.False(t, isAdminRoute([]slog.Attr{slog.String("uri", "/orders/v1")}))
	require.False(t, isAdminRoute([]slog.Attr{slog.String("method", "/admin/ping")}))
}

func TestAccessLogLine(t *testing.T) {
	line := accessLogLine([]slog.Attr{
		slog.String("method", "GET"),
		slog.Int("status", 200),
	})

	require.Equal(t, "method=GET status=200\n", line)
}
