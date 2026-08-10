// Package actor carries the typed principal that caused a request or an event.
//
// It is a leaf package on purpose: lib/history already imports lib/events, so
// a type shared by both cannot live in either. It imports nothing but context.
//
// DEV-NOTE: see BauerMediaGroup-Stardust/platform-gitops
// docs/plans/keycloak-service-auth.md section 8. The actor is audit
// information, never an authorization input - a downstream service must not
// decide anything from it.
package actor

import "context"

// Type is the kind of principal an Actor describes. It is typed rather than a
// bare id so "everything this staff member did" is answerable without guessing
// id shapes, and a player ULID is never mistaken for a Keycloak subject.
type Type string

const (
	// TypePlayer is a guest or verified customer, identified by playerId.
	TypePlayer Type = "player"

	// TypeUser is a staff member, identified by the Keycloak subject.
	TypeUser Type = "user"

	// TypeService is another platform service acting on its own authority,
	// identified by its Keycloak client id.
	TypeService Type = "service"

	// TypeSystem is a scheduler or consumer with no external trigger,
	// identified by the service id.
	TypeSystem Type = "system"
)

// Actor is the principal that caused a request or an event. An anonymous
// visitor has no Actor at all; the request id is the only provenance there.
type Actor struct {
	Type Type

	// Id is stable per principal: playerId, Keycloak subject, client id, or
	// service id depending on Type.
	Id string

	// Label is a human-readable hint for support, e.g. a staff email. It is
	// display only and may be empty.
	Label string
}

// Zero reports whether the actor carries no principal.
func (a Actor) Zero() bool {
	return a.Type == "" && a.Id == ""
}

type contextKey struct{}

// WithActor returns a context carrying the actor.
func WithActor(ctx context.Context, a Actor) context.Context {
	return context.WithValue(ctx, contextKey{}, a)
}

// FromContext returns the actor carried by ctx, if any.
func FromContext(ctx context.Context) (Actor, bool) {
	a, ok := ctx.Value(contextKey{}).(Actor)
	if !ok || a.Zero() {
		return Actor{}, false
	}

	return a, true
}
