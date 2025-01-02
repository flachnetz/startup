package api

import (
	"fmt"
	"net/http"
)

var (
	ErrSiteMissing         = Error{ErrorCode: "SITE_MISSING", ErrorDescription: "missing site", HttpStatusCode: http.StatusBadRequest}
	ErrUnknown             = Error{ErrorCode: "UNKNOWN_ERROR", ErrorDescription: "error is unknown", HttpStatusCode: http.StatusInternalServerError}
	ErrTimeout             = Error{ErrorCode: "TIMEOUT", ErrorDescription: "request timeout", HttpStatusCode: 499}
	ErrInternalServerError = Error{ErrorCode: "INTERNAL_SERVER_ERROR", ErrorDescription: "internal server error", HttpStatusCode: http.StatusInternalServerError}
)

type Error struct {
	ErrorCode        string                  `json:"errorCode"`
	ErrorDescription string                  `json:"errorDescription"`
	Field            *string                 `json:"field,omitempty"`
	Info             *map[string]interface{} `json:"info,omitempty"`
	HttpStatusCode   int                     `json:"-"`
	BaseError        error                   `json:"-"`
}

func (e Error) WithBaseError(err error) Error {
	e.BaseError = err
	return e
}

func (e Error) WithDescription(format string, args ...any) Error {
	e.ErrorDescription = fmt.Sprintf(format, args...)
	return e
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode, e.ErrorDescription)
}

func (e Error) ToErrorResponse() ErrorResponse {
	return ErrorResponse{
		MainError: e,
		AllErrors: []Error{e},
	}
}

func (e Error) Unwrap() error {
	if e.BaseError == nil {
		return e.BaseError
	}
	return nil
}

func Errorf(code string, format string, args ...any) Error {
	return Error{
		ErrorCode:        code,
		ErrorDescription: fmt.Sprintf(format, args...),
	}
}

type ErrorResponse struct {
	MainError Error   `json:"mainError"`
	AllErrors []Error `json:"allErrors"`
}

func (e ErrorResponse) Error() string {
	return e.MainError.Error()
}

func NewErrorResponse(errorCode string, errorDescription string, field string, info map[string]interface{}) ErrorResponse {
	e := Error{
		ErrorCode:        errorCode,
		ErrorDescription: errorDescription,
		Field:            &field,
		Info:             &info,
	}

	return ErrorResponse{
		MainError: e,
		AllErrors: []Error{e},
	}
}
