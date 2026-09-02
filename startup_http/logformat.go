package startup_http

import (
	"context"
	"log/slog"
	"net"
	"net/http"

	"github.com/felixge/httpsnoop"
)

type loggingHandler struct {
	handler http.Handler
	log     func(ctx context.Context, attrs []slog.Attr)
}

func (h loggingHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Code defaults to 200, like httpsnoop.CaptureMetrics does: a handler that
	// only writes a body never calls WriteHeader, and would otherwise be logged
	// with status 0.
	metrics := httpsnoop.Metrics{Code: http.StatusOK}
	metrics.CaptureMetrics(w, func(writer http.ResponseWriter) {
		h.handler.ServeHTTP(writer, req)
	})

	attrs := buildAccessLogAttrs(req, metrics)
	h.log(req.Context(), attrs)
}

func buildAccessLogAttrs(req *http.Request, metrics httpsnoop.Metrics) []slog.Attr {
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		host = req.RemoteAddr
	}

	uri := req.RequestURI
	if req.ProtoMajor == 2 && req.Method == "CONNECT" {
		uri = req.Host
	}
	if uri == "" {
		uri = req.URL.RequestURI()
	}

	return []slog.Attr{
		slog.String("host", host),
		slog.String("method", req.Method),
		slog.String("uri", uri),
		slog.String("proto", req.Proto),
		slog.Int("status", metrics.Code),
		slog.Int("size", int(metrics.Written)),
		slog.Duration("latency", metrics.Duration),
	}
}
