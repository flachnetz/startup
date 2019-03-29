package startup_tracing

import (
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"net/http"
)

func responseLoggerOf(w http.ResponseWriter) *responseLogger {
	if rl, ok := w.(*responseLogger); ok {
		return rl
	} else {
		return &responseLogger{ResponseWriter: w, status: http.StatusOK}
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

func (l *responseLogger) Flush() {
	f, ok := l.ResponseWriter.(http.Flusher)
	if ok {
		f.Flush()
	}
}

func (l *responseLogger) addStatusToSpan(span opentracing.Span) {
	ext.HTTPStatusCode.Set(span, uint16(l.status))
}
