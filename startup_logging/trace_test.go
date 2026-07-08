package sl

import (
	"context"
	"log/slog"
	"testing"

	"github.com/flachnetz/startup/v2/lib/tls"
	"go.opentelemetry.io/otel/trace"
)

func TestWithTraceId_FromSpan(t *testing.T) {
	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	record, cont, err := WithTraceId(ctx, slog.Record{})
	if err != nil {
		t.Fatal(err)
	}
	if !cont {
		t.Fatal("expected continue=true")
	}

	assertTraceAttr(t, record, "0102030405060708090a0b0c0d0e0f10")
}

func TestWithTraceId_FromThreadLocal(t *testing.T) {
	traceID, _ := trace.TraceIDFromHex("aabbccddeeff00112233445566778899")
	tls.Put(ThreadLocalTraceID(traceID))
	defer tls.Clear[ThreadLocalTraceID]()

	record, _, _ := WithTraceId(context.Background(), slog.Record{})
	assertTraceAttr(t, record, "aabbccddeeff00112233445566778899")
}

func TestWithTraceId_NoTrace(t *testing.T) {
	record, _, _ := WithTraceId(context.Background(), slog.Record{})

	var found bool
	record.Attrs(func(a slog.Attr) bool {
		if a.Key == "traceId" {
			found = true
			return false
		}
		return true
	})

	if found {
		t.Error("expected no traceId attribute")
	}
}

func assertTraceAttr(t *testing.T, record slog.Record, expected string) {
	t.Helper()

	var got string
	record.Attrs(func(a slog.Attr) bool {
		if a.Key == "traceId" {
			got = a.Value.String()
			return false
		}
		return true
	})

	if got != expected {
		t.Errorf("traceId = %q, want %q", got, expected)
	}
}
