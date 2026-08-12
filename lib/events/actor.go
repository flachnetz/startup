package events

import (
	"context"

	"github.com/flachnetz/startup/v2/lib/actor"
)

// Actor header names. Same vocabulary as the HTTP hops, so an audit trail reads
// identically whether a step arrived over HTTP or Kafka.
//
// DEV-NOTE: see BauerMediaGroup-Stardust/platform-gitops
// docs/plans/keycloak-service-auth.md section 8. These headers are audit
// information only. A consumer must never authorize on them: any producer can
// set any value, so trusting them would let one service impersonate a human.
const (
	HeaderActorType  = "Actor-Type"
	HeaderActorId    = "Actor-Id"
	HeaderActorLabel = "Actor-Label"
)

// eventWithActor carries the actor alongside the event, mirroring
// eventWithTraceContext: no Avro schema knows about it, MetadataOf turns it into
// Kafka headers, and the outbox persists those headers like any other.
type eventWithActor struct {
	Actor actor.Actor
	Event
}

func (e *eventWithActor) Unwrap() Event {
	return e.Event
}

// addActorToEvent wraps the event when ctx carries an actor, and returns it
// unchanged otherwise, so an anonymous producer emits no actor headers at all.
func addActorToEvent(ctx context.Context, event Event) Event {
	a, ok := actor.FromContext(ctx)
	if !ok {
		return event
	}

	return &eventWithActor{Actor: a, Event: event}
}

// actorHeaders returns the headers for the actor, omitting the optional label
// when it is empty.
func actorHeaders(a actor.Actor) EventHeaders {
	headers := EventHeaders{
		{Key: HeaderActorType, Value: string(a.Type)},
		{Key: HeaderActorId, Value: a.Id},
	}

	if a.Label != "" {
		headers = append(headers, EventHeader{Key: HeaderActorLabel, Value: a.Label})
	}

	return headers
}
