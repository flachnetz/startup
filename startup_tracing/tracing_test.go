package startup_tracing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

func TestNewResource_CarriesServiceNameAndGoSDK(t *testing.T) {
	res, err := newResource(t.Context(), "payment_service")
	require.NoError(t, err)

	attrs := map[attribute.Key]string{}
	for _, kv := range res.Attributes() {
		attrs[kv.Key] = kv.Value.Emit()
	}

	assert.Equal(t, "payment_service", attrs["service.name"])
	assert.Equal(t, "opentelemetry", attrs["telemetry.sdk.name"])
	assert.Equal(t, "go", attrs["telemetry.sdk.language"])
	assert.NotEmpty(t, attrs["telemetry.sdk.version"])
}
