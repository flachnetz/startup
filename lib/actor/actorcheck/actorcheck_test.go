package actorcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheck_AcceptsRecordingTheActor(t *testing.T) {
	violations, err := Check("testdata/good")
	require.NoError(t, err)
	assert.Empty(t, violations, "recording the actor for audit must be allowed: %v", violations)
}

func TestCheck_RejectsAuthorizingOnTheActor(t *testing.T) {
	violations, err := Check("testdata/bad")
	require.NoError(t, err)
	require.Len(t, violations, 2, "expected the if and the switch: %v", violations)

	assert.Equal(t, "actorId", violations[0].Expr)
	assert.Equal(t, "who", violations[1].Expr)
	assert.Contains(t, violations[0].String(), "testdata/bad/bad.go:")
}

func TestCheck_IgnoresTestFilesAndMissesNothingElse(t *testing.T) {
	// The whole startup library must be clean, which also proves Check runs over
	// a real tree without exploding.
	violations, err := Check("../../..")
	require.NoError(t, err)
	assert.Empty(t, violations, "%v", violations)
}
