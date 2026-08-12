package history

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/flachnetz/startup/v2/lib/actor"
)

// Trigger records what caused a history entry: the transport it arrived on and,
// where known, who or what initiated it. It is sourced from context (WithTrigger)
// by Track, stored in the history table's trigger column, and carried on the
// emitted event (via RecordToSend) so it survives to long-term Athena storage.
type Trigger struct {
	Source  string `json:"source"`            // e.g. http, message-broker, scheduler
	Detail  string `json:"detail,omitempty"`  // e.g. "POST /checkout", "topic payment_captured"
	RefType string `json:"refType,omitempty"` // kind of the source id, e.g. requestId, kafkaEventId
	Ref     string `json:"ref,omitempty"`     // the source id value (request/event this entry came from)

	// Actor is who initiated the work, filled from the context by triggerOf when
	// the caller did not set it. No migration: trigger is a JSON column and the
	// event field is a JSON string, so an older reader just ignores the key.
	Actor actor.Actor `json:"actor,omitzero"`
}

// IsZero reports whether no provenance was set.
func (t Trigger) IsZero() bool { return t == Trigger{} }

// Display renders the trigger for the history page, e.g.
// "message-broker: topic payment_captured (kafkaEventId=evt_1)".
func (t Trigger) Display() string {
	if t.IsZero() {
		return ""
	}
	s := t.Source
	if t.Detail != "" {
		s += ": " + t.Detail
	}
	if t.Ref != "" {
		ref := t.Ref
		if t.RefType != "" {
			ref = t.RefType + "=" + ref
		}
		s += " (" + ref + ")"
	}
	if !t.Actor.Zero() {
		actorText := string(t.Actor.Type) + " " + t.Actor.Id
		if t.Actor.Label != "" {
			actorText += " (" + t.Actor.Label + ")"
		}
		if s == "" {
			s = actorText
		} else {
			s += " by " + actorText
		}
	}
	return s
}

// JSON returns the encoding used on the wire (event trigger field) and in the
// trigger column, or "" when no provenance is set. EventCreators map it onto
// their event's trigger field.
func (t Trigger) JSON() string {
	if t.IsZero() {
		return ""
	}
	b, _ := json.Marshal(t)
	return string(b)
}

// Scan implements sql.Scanner, decoding the JSON trigger column (NULL or empty
// yields the zero Trigger).
//
//goland:noinspection GoMixedReceiverTypes
func (t *Trigger) Scan(src any) error {
	var s string
	switch v := src.(type) {
	case nil:
		return nil
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		return fmt.Errorf("unsupported trigger source type %T", src)
	}

	if s == "" {
		return nil
	}

	if err := json.Unmarshal([]byte(s), t); err != nil {
		return fmt.Errorf("decode trigger: %w", err)
	}

	return nil
}

// Value implements driver.Valuer, storing the trigger as JSON or NULL when empty.
func (t Trigger) Value() (driver.Value, error) {
	if t.IsZero() {
		return nil, nil
	}
	return t.JSON(), nil
}

type triggerKey struct{}

// WithTrigger tags ctx with the provenance of the work about to be recorded. Set
// it once at each entry point (HTTP handler, kafka consumer, scheduler); every
// Track call under that ctx inherits it, so history entries say where they came
// from without threading the trigger through every layer.
func WithTrigger(ctx context.Context, t Trigger) context.Context {
	return context.WithValue(ctx, triggerKey{}, t)
}

// triggerOf returns the trigger set by WithTrigger, or the zero value. The actor
// is taken from the context when the trigger carries none, so every entry point
// that authenticates a caller records who acted without setting it twice.
func triggerOf(ctx context.Context) Trigger {
	t, _ := ctx.Value(triggerKey{}).(Trigger)

	if t.Actor.Zero() {
		if a, ok := actor.FromContext(ctx); ok {
			t.Actor = a
		}
	}

	return t
}
