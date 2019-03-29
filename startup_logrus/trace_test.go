package startup_logrus

import (
	"fmt"
	"testing"
)

func TestHook(t *testing.T) {
	if traceId := traceIdOf(spanContext{}); traceId != "hex value" {
		t.Fatalf("traceId should be 'hex value' but was '%s'", traceId)
	}

	var i fmt.Stringer = spanContext{}
	if traceId := traceIdOf(i); traceId != "hex value" {
		t.Fatalf("traceId should be 'hex value' but was '%s'", traceId)
	}
}

type spanContext struct {
	TraceID hexer
}

func (spanContext) String() string {
	panic("implement me")
}

type hexer struct{}

func (hexer) ToHex() string {
	return "hex value"
}
