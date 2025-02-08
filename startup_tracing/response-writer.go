package startup_tracing

import (
	"net/http"
)

func statusWriter(w http.ResponseWriter) *responseLogger {
	return &responseLogger{ResponseWriter: w, status: http.StatusOK}
}

// responseLogger is wrapper of http.ResponseWriter that keeps track
// of its HTTP status code
type responseLogger struct {
	http.ResponseWriter
	status int
}

func (l *responseLogger) Unwrap() http.ResponseWriter {
	return l.ResponseWriter
}

func (l *responseLogger) WriteHeader(s int) {
	l.ResponseWriter.WriteHeader(s)
	l.status = s
}
