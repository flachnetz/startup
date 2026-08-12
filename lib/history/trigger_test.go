package history

import (
	"context"
	"testing"

	"github.com/flachnetz/startup/v2/lib/actor"
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

func TestTriggerActorRoundTripAndDisplay(t *testing.T) {
	tr := Trigger{
		Source: "http", Detail: "POST /refund", RefType: "requestId", Ref: "req_1",
		Actor: actor.Actor{Type: actor.TypeUser, Id: "sub-1", Label: "staff@example.com"},
	}

	v, err := tr.Value()
	require.NoError(t, err)
	var got Trigger
	require.NoError(t, got.Scan(v))
	assert.Equal(t, tr, got)

	assert.Equal(t, "http: POST /refund (requestId=req_1) by user sub-1 (staff@example.com)", tr.Display())

	// The persisted JSON is an audit column people grep: every key lowerCamel,
	// the actor included. Go-cased actor keys were shipped briefly and rows
	// written then lose their actor on Scan, which is accepted - the column is
	// display-only provenance, and re-reading old rows leniently would mean
	// carrying both spellings forever.
	assert.Equal(t,
		`{"source":"http","detail":"POST /refund","refType":"requestId","ref":"req_1",`+
			`"actor":{"type":"user","id":"sub-1","label":"staff@example.com"}}`,
		tr.JSON())

	// A trigger without an actor is unchanged, and omitzero keeps the key out.
	plain := Trigger{Source: "scheduler"}
	assert.Equal(t, "scheduler", plain.Display())
	assert.Equal(t, `{"source":"scheduler"}`, plain.JSON())
}

func TestTriggerOfTakesActorFromContext(t *testing.T) {
	a := actor.Actor{Type: actor.TypeService, Id: "order-service"}
	ctx := actor.WithActor(WithTrigger(context.Background(), Trigger{Source: "message-broker"}), a)

	assert.Equal(t, a, triggerOf(ctx).Actor)

	// An actor set on the trigger explicitly wins over the context.
	explicit := actor.Actor{Type: actor.TypeUser, Id: "sub-2"}
	ctx = actor.WithActor(WithTrigger(context.Background(), Trigger{Source: "http", Actor: explicit}), a)
	assert.Equal(t, explicit, triggerOf(ctx).Actor)

	// Actor alone is enough provenance to record.
	only := triggerOf(actor.WithActor(context.Background(), a))
	assert.False(t, only.IsZero())
	assert.Equal(t, "service order-service", only.Display())
}
