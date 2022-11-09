package startup_tracing

import (
	"net/http"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

func responseLoggerOf(w http.ResponseWriter) (*responseLogger, http.ResponseWriter) {
	hijacker, _ := w.(http.Hijacker)
	flusher, _ := w.(http.Flusher)

	rl := &responseLogger{ResponseWriter: w, status: http.StatusOK}

	switch {
	case hijacker != nil && flusher != nil:
		return rl, &rlWithHijackerFlusher{rl, hijacker, flusher}

	case hijacker != nil:
		return rl, &rlWithHijacker{rl, hijacker}

	case flusher != nil:
		return rl, &rlWithFlusher{rl, flusher}

	default:
		return rl, rl
	}
}

// responseLogger is wrapper of http.ResponseWriter that keeps track
// of its HTTP status code
type responseLogger struct {
	http.ResponseWriter
	status int
}

func (l *responseLogger) WriteHeader(s int) {
	l.ResponseWriter.WriteHeader(s)
	l.status = s
}

func (l *responseLogger) addStatusToSpan(span opentracing.Span) {
	ext.HTTPStatusCode.Set(span, uint16(l.status))
	if l.status == http.StatusNotFound {
		ext.Error.Set(span, false)
	}
}

type rlWithHijacker struct {
	http.ResponseWriter
	http.Hijacker
}

type rlWithFlusher struct {
	http.ResponseWriter
	http.Flusher
}
type rlWithHijackerFlusher struct {
	http.ResponseWriter
	http.Hijacker
	http.Flusher
}
