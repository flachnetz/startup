package startup_tracing

import (
	"github.com/mailru/easyjson"
	"github.com/openzipkin/zipkin-go/model"
	"net"
	"strings"
	"time"
)

//go:generate easyjson span-serializer.go

// spanSerializer implements the default JSON encoding SpanSerializer.
type spanSerializer struct{}

// Serialize takes an array of Zipkin model.SpanModel objects and returns a JSON
// encoding of it.
func (spanSerializer) Serialize(spans []*model.SpanModel) ([]byte, error) {
	jsonSpans := make(jsonSpanSlice, len(spans))

	for idx, span := range spans {
		jsonSpans[idx] = mapSpan(span)
	}

	return easyjson.Marshal(jsonSpans)
}

func mapSpan(span *model.SpanModel) jsonSpanModel {
	jsonSpan := jsonSpanModel{
		TraceID:   span.TraceID.String(),
		ID:        span.ID.String(),
		Debug:     span.Debug,
		Name:      strings.ToLower(span.Name),
		Kind:      string(span.Kind),
		Timestamp: (span.Timestamp.UnixNano() + 500) / 1e3,
		Duration:  durationInMicros(span.Duration),
		Shared:    span.Shared,
		Tags:      span.Tags,
	}

	if span.ParentID != nil {
		jsonSpan.ParentID = span.ParentID.String()
	}

	if ep := span.LocalEndpoint; ep != nil && !ep.Empty() {
		jsonSpan.LocalEndpoint = jsonEndpoint{
			ServiceName: ep.ServiceName,
			IPv4:        ep.IPv4,
			Port:        ep.Port,
		}
	}

	if ep := span.RemoteEndpoint; ep != nil && !ep.Empty() {
		jsonSpan.RemoteEndpoint = jsonEndpoint{
			ServiceName: ep.ServiceName,
			IPv4:        ep.IPv4,
			Port:        ep.Port,
		}
	}

	if len(span.Annotations) > 0 {
		jsonAnnotations := make([]jsonAnnotation, len(span.Annotations))

		for idx, ann := range span.Annotations {
			jsonAnnotations[idx] = jsonAnnotation{
				Timestamp: ann.Timestamp.Round(time.Microsecond).UnixNano() / 1e3,
				Value:     ann.Value,
			}
		}

		jsonSpan.Annotations = jsonAnnotations
	}

	return jsonSpan
}

func durationInMicros(duration time.Duration) int64 {
	if duration < 1*time.Microsecond {
		return 1
	}

	return int64((duration + 500*time.Nanosecond) / time.Microsecond)
}

// ContentType returns the ContentType needed for this encoding.
func (spanSerializer) ContentType() string {
	return "application/json"
}

//easyjson:json
type jsonSpanSlice []jsonSpanModel

//easyjson:json
type jsonSpanModel struct {
	TraceID        string            `json:"traceId"`
	ID             string            `json:"id"`
	ParentID       string            `json:"parentId,omitempty"`
	Debug          bool              `json:"debug,omitempty"`
	Name           string            `json:"name,omitempty"`
	Kind           string            `json:"kind,omitempty"`
	Shared         bool              `json:"shared,omitempty"`
	Timestamp      int64             `json:"timestamp,omitempty"`
	Duration       int64             `json:"duration,omitempty"`
	LocalEndpoint  jsonEndpoint      `json:"localEndpoint,omitempty"`
	RemoteEndpoint jsonEndpoint      `json:"remoteEndpoint,omitempty"`
	Annotations    []jsonAnnotation  `json:"annotations,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
}

//easyjson:json
type jsonEndpoint struct {
	ServiceName string `json:"serviceName,omitempty"`
	IPv4        net.IP `json:"ipv4,omitempty"`
	IPv6        net.IP `json:"ipv6,omitempty"`
	Port        uint16 `json:"port,omitempty"`
}

//easyjson:json
type jsonAnnotation struct {
	Timestamp int64  `json:"timestamp"`
	Value     string `json:"value"`
}
