package startup_http

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/felixge/httpsnoop"
)

type loggingHandler struct {
	handler http.Handler
	log     func(ctx context.Context, line string)
}

func (h loggingHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	startTime := time.Now()

	var metrics httpsnoop.Metrics
	metrics.CaptureMetrics(w, func(writer http.ResponseWriter) {
		h.handler.ServeHTTP(writer, req)
	})

	line := buildCommonLogLine(req, *req.URL, startTime, metrics)
	h.log(req.Context(), string(line))
}

// buildCommonLogLine builds a log entry for req in Apache Common Log Format.
// ts is the timestamp with which the entry should be logged.
// status and size are used to provide the response HTTP status and size.
func buildCommonLogLine(req *http.Request, url url.URL, ts time.Time, metrics httpsnoop.Metrics) []byte {
	username := "-"
	if url.User != nil {
		if name := url.User.Username(); name != "" {
			username = name
		}
	}

	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		host = req.RemoteAddr
	}

	uri := req.RequestURI

	// Requests using the CONNECT method over HTTP/2.0 must use
	// the authority field (aka r.Host) to identify the target.
	// Refer: https://httpwg.github.io/specs/rfc7540.html#CONNECT
	if req.ProtoMajor == 2 && req.Method == "CONNECT" {
		uri = req.Host
	}
	if uri == "" {
		uri = url.RequestURI()
	}

	buf := make([]byte, 0, 3*(len(host)+len(username)+len(req.Method)+len(uri)+len(req.Proto)+50)/2)
	buf = append(buf, host...)
	buf = append(buf, " - "...)
	buf = append(buf, username...)
	buf = append(buf, " ["...)
	buf = append(buf, ts.Format("02/Jan/2006:15:04:05 -0700")...)
	buf = append(buf, `] "`...)
	buf = append(buf, req.Method...)
	buf = append(buf, " "...)
	buf = append(buf, fmt.Sprintf("%q", uri)...)
	buf = append(buf, " "...)
	buf = append(buf, req.Proto...)
	buf = append(buf, `" `...)
	buf = append(buf, strconv.Itoa(metrics.Code)...)
	buf = append(buf, " "...)
	buf = append(buf, strconv.Itoa(int(metrics.Written))...)
	buf = append(buf, " "...)
	buf = append(buf, metrics.Duration.String()...)
	return buf
}
