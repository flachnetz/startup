package api

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorf_PlainMessage(t *testing.T) {
	err := Errorf(ErrorCodeBadRequest, "something went wrong")

	require.Equal(t, ErrorCodeBadRequest, err.Code)
	require.Equal(t, "something went wrong", err.Description)
	require.Nil(t, err.Cause)
}

func TestErrorf_FormatArgsWithoutError(t *testing.T) {
	err := Errorf(ErrorCodeNotFound, "user %s with id %d not found", "alice", 42)

	require.Equal(t, ErrorCodeNotFound, err.Code)
	require.Equal(t, "user alice with id 42 not found", err.Description)
	require.Nil(t, err.Cause)
}

func TestErrorf_WrappedErrorSetsCause(t *testing.T) {
	cause := errors.New("connection refused")

	err := Errorf(ErrorCodeInternal, "failed to connect: %w", cause)

	require.Equal(t, ErrorCodeInternal, err.Code)
	require.Equal(t, cause, err.Cause)
	require.ErrorIs(t, err, cause)
}

func TestErrorf_WrappedErrorIsNotExposedInDescription(t *testing.T) {
	cause := errors.New("super secret internal detail")

	err := Errorf(ErrorCodeInternal, "failed to connect: %w", cause)

	require.Equal(t, "failed to connect", err.Description)
	require.NotContains(t, err.Description, "super secret internal detail")
}

func TestErrorf_WrappedErrorInMiddleOfMessage(t *testing.T) {
	cause := errors.New("boom")

	err := Errorf(ErrorCodeInternal, "before %w after", cause)

	require.Equal(t, "before (redacted) after", err.Description)
	require.ErrorIs(t, cause, err.Cause)
}

func TestErrorf_OnlyWrappedError(t *testing.T) {
	cause := errors.New("boom")

	err := Errorf(ErrorCodeInternal, "%w", cause)

	require.Equal(t, "", err.Description)
	require.Equal(t, cause, err.Cause)
}

func TestErrorf_WrappedErrorWithExtraArgs(t *testing.T) {
	cause := errors.New("internal detail")

	err := Errorf(ErrorCodeBadRequest, "user %s failed: %w", "bob", cause)

	require.Equal(t, "user bob failed", err.Description)
	require.NotContains(t, err.Description, "internal detail")
	require.Equal(t, cause, err.Cause)
}

func TestErrorf_MultipleErrorsReplaced(t *testing.T) {
	err1 := errors.New("first secret")
	err2 := errors.New("second secret")

	err := Errorf(ErrorCodeInternal, "%w and %v", err1, err2)

	require.Equal(t, "and", err.Description)
	require.NotContains(t, err.Description, "first secret")
	require.NotContains(t, err.Description, "second secret")
	require.ErrorIs(t, err.Cause, err1)
	require.ErrorIs(t, err.Cause, err2)
}

func TestErrorf_MiddleErrorIsRedacted(t *testing.T) {
	err1 := errors.New("first secret")
	err2 := errors.New("second secret")

	err := Errorf(ErrorCodeInternal, "start %v then %w end", err1, err2)

	require.Equal(t, "start (redacted) then (redacted) end", err.Description)
	require.NotContains(t, err.Description, "first secret")
	require.NotContains(t, err.Description, "second secret")
	require.ErrorIs(t, err.Cause, err1)
	require.ErrorIs(t, err.Cause, err2)
}

func TestErrorf_LeadingErrorRemovesColonAndSpace(t *testing.T) {
	cause := errors.New("secret")

	err := Errorf(ErrorCodeInternal, "%w: trailing context", cause)

	require.Equal(t, "trailing context", err.Description)
	require.Equal(t, cause, err.Cause)
}

func TestErrorf_DescriptionIsTrimmed(t *testing.T) {
	cause := errors.New("boom")

	err := Errorf(ErrorCodeInternal, "failed:   %w   ", cause)

	require.Equal(t, "failed", err.Description)
}

func TestErrorf_ErrorStringIncludesCause(t *testing.T) {
	cause := errors.New("boom")

	err := Errorf(ErrorCodeInternal, "failed to connect: %w", cause)

	require.Equal(t, "Internal: failed to connect (caused by boom)", err.Error())
}
