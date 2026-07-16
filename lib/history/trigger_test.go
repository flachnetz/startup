package history

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTriggerRoundTrip(t *testing.T) {
	tr := Trigger{Source: "message-broker", Detail: "topic payment_captured", RefType: "kafkaEventId", Ref: "evt_1"}

	// Value (driver) -> Scan round-trips.
	v, err := tr.Value()
	require.NoError(t, err)
	var got Trigger
	require.NoError(t, got.Scan(v))
	assert.Equal(t, tr, got)

	// Display and JSON.
	assert.Equal(t, "message-broker: topic payment_captured (kafkaEventId=evt_1)", tr.Display())
	assert.Equal(t, `{"source":"message-broker","detail":"topic payment_captured","refType":"kafkaEventId","ref":"evt_1"}`, tr.JSON())

	// Zero value stores NULL, renders/serializes empty, scans NULL back to zero.
	zv, err := (Trigger{}).Value()
	require.NoError(t, err)
	assert.Nil(t, zv)
	assert.Empty(t, Trigger{}.Display())
	assert.Empty(t, Trigger{}.JSON())
	var z Trigger
	require.NoError(t, z.Scan(nil))
	assert.True(t, z.IsZero())

	// Context propagation.
	ctx := WithTrigger(context.Background(), tr)
	assert.Equal(t, tr, triggerOf(ctx))
	assert.True(t, triggerOf(context.Background()).IsZero())
}
