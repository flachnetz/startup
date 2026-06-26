package api

import (
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"strings"
)

// ErrorCode is a stable, machine-readable identifier for a class of error.
// Unlike the human-facing Description, it is safe for clients to switch on.
type ErrorCode string

const (
	ErrorCodeBadRequest ErrorCode = "BadRequest"
	ErrorCodeConflict   ErrorCode = "Conflict"
	ErrorCodeInternal   ErrorCode = "Internal"
	ErrorCodeNotFound   ErrorCode = "NotFound"
	ErrorCodeTimeout    ErrorCode = "Timeout"
)

// Attributes carries additional, structured context about an error,
// e.g. for logging or returning machine-readable details to the caller.
type Attributes map[string]any

// Predefined errors for the most common failure modes. They are meant to be
// used as templates and customized via the With* methods, which return copies
// and never mutate the shared value.
var (
	ErrBadRequest = Error{
		Code:           ErrorCodeBadRequest,
		Description:    "bad request",
		HttpStatusCode: http.StatusBadRequest,
	}

	ErrInternalServerError = Error{
		Code:           ErrorCodeInternal,
		Description:    "internal server error",
		HttpStatusCode: http.StatusInternalServerError,
	}

	ErrTimeout = Error{
		Code:           ErrorCodeTimeout,
		Description:    "request timeout",
		HttpStatusCode: http.StatusInternalServerError,
	}

	ErrCanceled = Error{
		Code:           "ErrCanceled",
		Description:    "client has canceled the request",
		HttpStatusCode: 499,
	}
)

// Error is the canonical error type used across the API layer. It bundles a
// machine-readable Code, a human-readable Description, optional structured
// Attributes and an underlying Cause. It implements the error interface and
// supports errors.Is/As/Unwrap via Cause.
//
// Instances are treated as immutable: the With* helpers return a modified copy
// rather than mutating the receiver.
type Error struct {
	// Code is the stable, machine-readable error classification.
	Code ErrorCode

	// Description is a human-readable message safe to expose to the caller.
	Description string

	// Attributes holds optional structured context about the error.
	Attributes Attributes

	// Explicitly set the status code of the response
	HttpStatusCode int

	// The error that caused this error. Can be unwrapped to using Unwrap()
	Cause error
}

// WithDescription returns a copy of e with the Description replaced by the
// formatted message.
func (e Error) WithDescription(format string, args ...any) Error {
	e.Description = fmt.Sprintf(format, args...)
	return e
}

// WithCause returns a copy of e with the underlying Cause set to cause.
func (e Error) WithCause(cause error) Error {
	e.Cause = cause
	return e
}

// WithAttribute returns a copy of e with key set to value. The receiver's
// Attributes map is cloned, so the original error is left unchanged.
func (e Error) WithAttribute(key string, value any) Error {
	merged := maps.Clone(e.Attributes)
	if merged == nil {
		merged = Attributes{}
	}

	merged[key] = value

	e.Attributes = merged
	return e
}

// WithAttributes returns a copy of e with newValues merged into its
// Attributes. Existing keys are overwritten and the original error is left
// unchanged.
func (e Error) WithAttributes(newValues Attributes) Error {
	merged := maps.Clone(e.Attributes)
	if merged == nil {
		merged = Attributes{}
	}

	maps.Insert(merged, maps.All(newValues))

	e.Attributes = merged
	return e
}

// Error implements the error interface. The Cause, when present, is appended
// for diagnostics; do not rely on this format for client-facing output.
func (e Error) Error() string {
	if e.Cause == nil {
		return string(e.Code) + ": " + e.Description
	}

	return fmt.Sprintf("%s: %s (caused by %s)", e.Code, e.Description, e.Cause)
}

// Unwrap returns the underlying Cause, enabling errors.Is and errors.As.
func (e Error) Unwrap() error {
	return e.Cause
}

// toErrorResponse converts a structured Error into an ErrorResponse,
// falling back to a 500 status code if the error carries an out-of-range HTTP
// status code.
func (e Error) toErrorResponse() ErrorResponse {
	statusCode := e.HttpStatusCode
	if statusCode < 400 || statusCode >= 600 {
		// invalid, use default
		statusCode = http.StatusInternalServerError
	}

	return ErrorResponse{
		Code:        e.Code,
		Description: e.Description,
		Attributes:  e.Attributes,
		StatusCode:  statusCode,
	}
}

// Errorf builds an Error with the given code and a formatted description.
//
// Any error passed as an argument is kept as the Cause (via the standard %w
// wrapping, but also via %s or %v) but is scrubbed from the human-readable
// Description so that internal details are never leaked to the caller. Depending
// on where the error appears in the message it is either dropped entirely (at
// the start or end, together with an adjacent colon and whitespace) or replaced
// with a "(redacted)" placeholder (in the middle).
func Errorf(code ErrorCode, format string, args ...any) Error {
	var desc string

	var causes []error

	// collect all error causes and replace all errors with markers so we
	// do not expose any internals
	for idx := range args {
		if err, ok := args[idx].(error); ok {
			causes = append(causes, err)
			args[idx] = errMarker
		}
	}

	if len(causes) > 0 {
		// format again but with all errors replaced by markers
		desc = fmt.Errorf(format, args...).Error()

		// markers at the start are removed completely, including a trailing colon and space
		desc = leadingMarker.ReplaceAllLiteralString(desc, "")

		// markers at the end are removed completely, including a leading colon and space
		desc = trailingMarker.ReplaceAllLiteralString(desc, "")

		// markers in the middle are replaced with a redaction placeholder
		desc = innerMarker.ReplaceAllLiteralString(desc, "(redacted)")
	} else {
		// use the error string directly
		desc = fmt.Errorf(format, args...).Error()
	}

	return Error{
		Code:        code,
		Description: strings.TrimSpace(desc),
		Cause:       join(causes),
	}
}

func join(errs []error) error {
	switch len(errs) {
	case 0:
		return nil

	case 1:
		return errs[0]

	default:
		return errors.Join(errs...)
	}
}

var (
	// errMarker is substituted for any error argument before re-formatting, and
	// the marker regexes below locate those substitutions so they can be removed
	// or redacted. The token is intentionally distinctive to avoid colliding with
	// regular message text.
	errMarker = errors.New("ERRORMARKER")

	leadingMarker  = regexp.MustCompile(`^\s*ERRORMARKER\s*:?\s*`) // marker at the start, plus following colon/space
	trailingMarker = regexp.MustCompile(`\s*:?\s*ERRORMARKER\s*$`) // marker at the end, plus preceding colon/space
	innerMarker    = regexp.MustCompile(`ERRORMARKER`)             // any remaining marker in the middle
)
