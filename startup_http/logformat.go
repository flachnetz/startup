package startup_http

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/felixge/httpsnoop"
)

type loggingHandler struct {
	handler http.Handler
	log     func(ctx context.Context, attrs []slog.Attr)
}

func (h loggingHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	startTime := time.Now()

	var metrics httpsnoop.Metrics
	metrics.CaptureMetrics(w, func(writer http.ResponseWriter) {
		h.handler.ServeHTTP(writer, req)
	})

	attrs := buildAccessLogAttrs(req, startTime, metrics)
	h.log(req.Context(), attrs)
}

func buildAccessLogAttrs(req *http.Request, ts time.Time, metrics httpsnoop.Metrics) []slog.Attr {
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
