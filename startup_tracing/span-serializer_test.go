package startup_tracing

import (
	"bytes"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/flachnetz/startup/v2/lib"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter"
	"github.com/stretchr/testify/require"
)

func demoSpan(spanId model.ID) *model.SpanModel {
	return &model.SpanModel{
		SpanContext: model.SpanContext{
			TraceID: model.TraceID{
				Low: 0xdead,
			},
			ID:       spanId,
			ParentID: lib.PtrOf(model.ID(0xb00b)),
		},
		Name:      "name of span",
		Kind:      model.Client,
		Timestamp: time.Date(2012, 7, 3, 11, 14, 0, 0, time.UTC),
		Duration:  1337 * time.Microsecond,
		LocalEndpoint: &model.Endpoint{
			ServiceName: "the service name",
			IPv4:        net.IPv4(127, 0, 0, 1),
			Port:        8080,
		},
		Annotations: []model.Annotation{
			{
				Timestamp: time.Date(2012, 7, 3, 11, 14, 0, 50, time.UTC),
				Value:     "foobar",
			},
		},
		Tags: map[string]string{
			"one": "eins",
			"two": "zwei",
		},
	}
}

func TestSpanSerializer(t *testing.T) {
	span := demoSpan(0xbeef)
	buf, err := spanSerializer{}.Serialize([]*model.SpanModel{span})
	require.NoError(t, err)

	dec := json.NewDecoder(bytes.NewReader(buf))
	dec.UseNumber()

	var decoded any
	err = dec.Decode(&decoded)
	require.NoError(t, err)

	type J = map[string]any

	expected := []any{
		J{
			"timestamp": json.Number("1341314040000000"),
			"traceId":   "000000000000dead",
			"annotations": []any{
				J{
					"timestamp": json.Number("1341314040000000"),
					"value":     "foobar",
				},
			},
			"duration": json.Number("1337"),
			"id":       "000000000000beef",
			"kind":     "CLIENT",
			"localEndpoint": J{
				"ipv4":        "127.0.0.1",
				"port":        json.Number("8080"),
				"serviceName": "the service name",
			},
			"name":     "name of span",
			"parentId": "000000000000b00b",
			"tags": J{
				"one": "eins",
				"two": "zwei",
			},
		},
	}

	require.Equal(t, expected, decoded)
}

func TestSerializeTheSame(t *testing.T) {
	span := demoSpan(0xbeef)
	bufSelf, err := spanSerializer{}.Serialize([]*model.SpanModel{span})
	require.NoError(t, err)

	bufZipkin, err := reporter.JSONSerializer{}.Serialize([]*model.SpanModel{span})
	require.NoError(t, err)

	require.JSONEq(t, string(bufSelf), string(bufZipkin))
}

func BenchmarkSpanSerializer(b *testing.B) {
	spans := []*model.SpanModel{
		demoSpan(100),
		demoSpan(101),
		demoSpan(102),
		demoSpan(103),
		demoSpan(103),
		demoSpan(104),
		demoSpan(105),
		demoSpan(106),
		demoSpan(107),
		demoSpan(108),
	}

	b.Run("OpenTracing", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = reporter.JSONSerializer{}.Serialize(spans)
		}
	})

	b.Run("Easyjson", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = spanSerializer{}.Serialize(spans)
		}
	})
}
