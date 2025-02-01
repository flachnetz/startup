package startup_tracing

import (
	"github.com/bytedance/sonic"
	"github.com/openzipkin/zipkin-go/model"
)

// sonicSerializer implements the default JSON encoding SpanSerializer.
type sonicSerializer struct{}

// Serialize takes an array of Zipkin SpanModel objects and returns a JSON
// encoding of it.
func (sonicSerializer) Serialize(spans []*model.SpanModel) ([]byte, error) {
	return sonic.Marshal(spans)
}

// ContentType returns the ContentType needed for this encoding.
func (sonicSerializer) ContentType() string {
	return "application/json"
}
