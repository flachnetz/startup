package good

import (
	"net/http"

	"github.com/flachnetz/startup/v2/lib/actor"
)

const HeaderActorId = "Actor-Id"

// recordActor is the legitimate use: read the headers, put them in the context
// for audit, decide nothing.
func recordActor(r *http.Request) *http.Request {
	actorType := r.Header.Get("Actor-Type")
	actorId := r.Header.Get(HeaderActorId)

	if actorId == "" {
		return r
	}

	ctx := actor.WithActor(r.Context(), actor.Actor{Type: actor.Type(actorType), Id: actorId})

	return r.WithContext(ctx)
}

// matchKafkaHeader matches header KEYS, which is how a consumer finds the actor.
func matchKafkaHeader(keys []string, values []string) (string, bool) {
	for i, key := range keys {
		if key == "Actor-Id" {
			return values[i], true
		}
	}

	return "", false
}
