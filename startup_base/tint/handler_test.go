package tint

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fixedTime is the timestamp of every record below, so the rendered time is
// predictable.
var fixedTime = time.Date(2026, 4, 16, 10, 11, 12, 0, time.UTC)

// plainOpts is the option set most tests use: colours would only add escape
// codes to the assertions.
func plainOpts() *Options {
	return &Options{NoColor: true, TimeFormat: time.RFC3339}
}

// handle renders one record through h and returns the written line without its
// trailing newline.
func handle(t *testing.T, h slog.Handler, r slog.Record) string {
	t.Helper()

	buf, ok := writerOf(h)
	require.True(t, ok, "handler must write to the test buffer")
	require.NoError(t, h.Handle(context.Background(), r))

	return strings.TrimSuffix(buf.String(), "\n")
}

// writerOf exposes the buffer a handler writes to, so a test can build the
// handler and read the output without keeping both around.
func writerOf(h slog.Handler) (*bytes.Buffer, bool) {
	hh, ok := h.(*handler)
	if !ok {
		return nil, false
	}

	buf, ok := hh.w.(*bytes.Buffer)

	return buf, ok
}

// record builds a record with the fixed timestamp and no source PC.
func record(level slog.Level, msg string, attrs ...slog.Attr) slog.Record {
	r := slog.NewRecord(fixedTime, level, msg, 0)
	r.AddAttrs(attrs...)

	return r
}

func TestHandlerRendersTimeLevelMessageAndAttrs(t *testing.T) {
	h := NewHandler(&bytes.Buffer{}, plainOpts())

	line := handle(t, h, record(slog.LevelInfo, "order captured",
		slog.String("orderId", "order-1"), slog.Int("amount", 42)))

	require.Equal(t, "2026-04-16T10:11:12Z  INFO order captured orderId=order-1 amount=42", line)
}

func TestHandlerRendersLevels(t *testing.T) {
	cases := map[slog.Level]string{
		slog.LevelDebug:     "DEBUG",
		slog.LevelInfo:      " INFO",
		slog.LevelWarn:      " WARN",
		slog.LevelError:     "ERROR",
		slog.LevelWarn + 1:  " WARN+1",
		slog.LevelDebug - 2: "DEBUG-2",
	}

	for level, want := range cases {
		t.Run(want, func(t *testing.T) {
			opts := plainOpts()
			opts.Level = slog.LevelDebug - 4

			line := handle(t, NewHandler(&bytes.Buffer{}, opts), record(level, "msg"))
			require.Equal(t, "2026-04-16T10:11:12Z "+want+" msg", line)
		})
	}
}

func TestHandlerRendersPrefixAttrOnce(t *testing.T) {
	h := NewHandler(&bytes.Buffer{}, plainOpts())

	line := handle(t, h, record(slog.LevelInfo, "started",
		slog.String(prefixKey, "payments"), slog.String("port", "8080")))

	// the prefix takes the source position, before the message, and is not
	// repeated among the attributes
	require.Equal(t, "2026-04-16T10:11:12Z  INFO payments started port=8080", line)
	require.Equal(t, 1, strings.Count(line, "payments"))
}

func TestHandlerAddSourceReplacesPrefix(t *testing.T) {
	opts := plainOpts()
	opts.AddSource = true

	var pcs [1]uintptr
	runtime.Callers(1, pcs[:])

	r := slog.NewRecord(fixedTime, slog.LevelInfo, "with source", pcs[0])
	r.AddAttrs(slog.String(prefixKey, "payments"))

	line := handle(t, NewHandler(&bytes.Buffer{}, opts), r)

	require.Contains(t, line, "handler_test.go:")
	require.Contains(t, line, "tint.TestHandlerAddSourceReplacesPrefix")
	require.NotContains(t, line, "payments")
}

func TestHandlerEnabledFiltersBelowLevel(t *testing.T) {
	opts := plainOpts()
	opts.Level = slog.LevelWarn

	h := NewHandler(&bytes.Buffer{}, opts)

	require.False(t, h.Enabled(context.Background(), slog.LevelInfo))
	require.True(t, h.Enabled(context.Background(), slog.LevelWarn))
}

func TestHandlerReplaceAttrCanDropTimeAndRewriteValues(t *testing.T) {
	opts := &Options{
		NoColor: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.TimeKey:
				return slog.Attr{}
			case "secret":
				return slog.String("secret", "***")
			}

			return a
		},
	}

	line := handle(t, NewHandler(&bytes.Buffer{}, opts),
		record(slog.LevelError, "failed", slog.String("secret", "hunter2")))

	require.Equal(t, "ERROR failed secret=***", line)
}

func TestHandlerWithAttrsAndGroups(t *testing.T) {
	h := NewHandler(&bytes.Buffer{}, plainOpts()).
		WithAttrs([]slog.Attr{slog.String("service", "payments")}).
		WithGroup("http")

	line := handle(t, h, record(slog.LevelInfo, "request", slog.Int("status", 200)))

	require.Equal(t, "2026-04-16T10:11:12Z  INFO request service=payments http.status=200", line)
}

func TestHandlerErrorAttr(t *testing.T) {
	h := NewHandler(&bytes.Buffer{}, plainOpts())

	line := handle(t, h, record(slog.LevelError, "boom", Err(errors.New("test failure"))))

	require.Equal(t, `2026-04-16T10:11:12Z ERROR boom err="test failure"`, line)
}

func TestHandlerZeroTimeIsOmitted(t *testing.T) {
	h := NewHandler(&bytes.Buffer{}, plainOpts())

	line := handle(t, h, slog.NewRecord(time.Time{}, slog.LevelInfo, "no time", 0))

	require.Equal(t, " INFO no time", line)
}
