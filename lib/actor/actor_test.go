package actor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActor_RoundTripsThroughContext(t *testing.T) {
	want := Actor{Type: TypeUser, Id: "9f1c-sub", Label: "jane@example.com"}

	got, ok := FromContext(WithActor(t.Context(), want))
	require.True(t, ok)
	require.Equal(t, want, got)
}

func TestActor_BareContextHasNoActor(t *testing.T) {
	_, ok := FromContext(context.Background())
	require.False(t, ok)
}

func TestActor_EmptyActorCountsAsAbsent(t *testing.T) {
	// an anonymous visitor must not look like a principal with an empty id
	_, ok := FromContext(WithActor(t.Context(), Actor{}))
	require.False(t, ok)
}

func TestActor_InnerActorWins(t *testing.T) {
	ctx := WithActor(t.Context(), Actor{Type: TypeService, Id: "order-service"})
	ctx = WithActor(ctx, Actor{Type: TypeUser, Id: "sub-1"})

	got, ok := FromContext(ctx)
	require.True(t, ok)
	require.Equal(t, Actor{Type: TypeUser, Id: "sub-1"}, got)
}
