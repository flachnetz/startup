package startup

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// withEmptyArgs replaces os.Args with a single program name for the duration of
// the test, so that go-flags does not try to parse the `go test` flags.
func withEmptyArgs(t *testing.T) {
	t.Helper()
	old := os.Args
	os.Args = []string{"cmd"}
	t.Cleanup(func() { os.Args = old })
}

// --- test fixtures ---------------------------------------------------------

// diLeaf is a dependency without any of its own dependencies, except the
// context that is always available.
type diLeaf struct {
	initCount int
	gotCtx    bool
}

func (l *diLeaf) Initialize(ctx context.Context) {
	l.initCount++
	l.gotCtx = ctx != nil
}

// diConsumer depends on diLeaf, both by value and by pointer.
type diConsumer struct {
	initCount    int
	gotLeafValue diLeaf
	gotLeafPtr   *diLeaf
}

func (c *diConsumer) Initialize(leafVal diLeaf, leafPtr *diLeaf) {
	c.initCount++
	c.gotLeafValue = leafVal
	c.gotLeafPtr = leafPtr
}

// diNoInit is a struct field without an Initialize method; it must be skipped.
type diNoInit struct {
	Name string `long:"name"`
}

func TestInitializeInjectsSeenDependencies(t *testing.T) {
	withEmptyArgs(t)

	type options struct {
		Leaf     diLeaf
		NoInit   diNoInit
		Consumer diConsumer
	}

	var opts options
	require.NoError(t, ParseCommandLine(t.Context(), &opts))

	// Leaf.Initialize must have been called exactly once with a context.
	require.Equal(t, 1, opts.Leaf.initCount, "Leaf.Initialize should be called exactly once")
	require.True(t, opts.Leaf.gotCtx, "Leaf.Initialize should receive a context")

	// Consumer.Initialize must have been called exactly once.
	require.Equal(t, 1, opts.Consumer.initCount, "Consumer.Initialize should be called exactly once")

	// The value injected by value must reflect the already-initialized Leaf.
	require.Equal(t, 1, opts.Consumer.gotLeafValue.initCount,
		"Consumer should receive the Leaf value after it was initialized")

	// The pointer injected must point at the real Leaf field inside opts.
	require.Same(t, &opts.Leaf, opts.Consumer.gotLeafPtr,
		"Consumer should receive a pointer to the real Leaf field")
}

// diMissing is never used as a field, so it is never "seen".
type diMissing struct{}

// diOptional depends on a *diMissing, which is optional and must be nil when
// no diMissing value was seen.
type diOptional struct {
	called bool
	gotPtr *diMissing
}

func (o *diOptional) Initialize(missing *diMissing) {
	o.called = true
	o.gotPtr = missing
}

func TestInitializeOptionalPointerIsNilWhenUnseen(t *testing.T) {
	withEmptyArgs(t)

	type options struct {
		Optional diOptional
	}

	var opts options
	require.NoError(t, ParseCommandLine(t.Context(), &opts))

	require.True(t, opts.Optional.called, "Optional.Initialize should be called")
	require.Nil(t, opts.Optional.gotPtr, "unseen optional dependency should be injected as nil")
}

// diRequiresMissing depends on a diMissing by value (not a pointer), which is a
// required dependency. Since it is never seen, Initialize must panic.
type diRequiresMissing struct{}

func (diRequiresMissing) Initialize(missing diMissing) {}

func TestInitializePanicsOnMissingRequiredDependency(t *testing.T) {
	withEmptyArgs(t)

	type options struct {
		Requires diRequiresMissing
	}

	var opts options
	require.Panics(t, func() {
		_ = ParseCommandLine(t.Context(), &opts)
	}, "missing required dependency should panic")
}
