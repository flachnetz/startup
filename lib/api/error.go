package api

import "fmt"

var (
	ErrSiteMissing         = Error{ErrorCode: "SITE_MISSING", ErrorDescription: "missing site"}
	ErrUnknown             = Error{ErrorCode: "UNKNOWN_ERROR", ErrorDescription: "error is unknown"}
	ErrTimeout             = Error{ErrorCode: "TIMEOUT", ErrorDescription: "request timeout"}
	ErrInternalServerError = Error{ErrorCode: "INTERNAL_SERVER_ERROR", ErrorDescription: "internal server error"}
)

type Error struct {
	ErrorCode        string                  `json:"errorCode"`
	ErrorDescription string                  `json:"errorDescription"`
	Field            *string                 `json:"field,omitempty"`
	Info             *map[string]interface{} `json:"info,omitempty"`
}

func (e Error) WithDescription(msg string) Error {
	e.ErrorDescription = msg
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

func Errorf(code string, format string, args ...interface{}) Error {
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
