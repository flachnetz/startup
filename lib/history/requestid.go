package history

import (
	"context"
	"database/sql/driver"

	"go.opentelemetry.io/otel/trace"
)

type RequestTraceId struct {
	traceId trace.TraceID
}

//goland:noinspection GoMixedReceiverTypes
func (h *RequestTraceId) Scan(src any) error {
	hex, ok := src.(string)
	if !ok {
		// NULL or non-string: leave the zero (invalid) trace id.
		return nil
	}

	traceId, err := trace.TraceIDFromHex(hex)
	if err != nil {
		// '00' placeholder or any other non-trace value: treat as no trace id.
		return nil
	}

	h.traceId = traceId

	return nil
}

func (h RequestTraceId) Value() (driver.Value, error) {
	if !h.IsValid() {
		// store NULL when there is no valid request trace id.
		return nil, nil
	}

	return h.String(), nil
}

func (h RequestTraceId) IsValid() bool {
	return h.traceId.IsValid()
}

func (h RequestTraceId) String() string {
	return h.traceId.String()
}

// requestTraceIdOf extracts the current otel traceId from the context.
func requestTraceIdOf(ctx context.Context) RequestTraceId {
	spanContext := trace.SpanContextFromContext(ctx)
	return RequestTraceId{traceId: spanContext.TraceID()}
}
