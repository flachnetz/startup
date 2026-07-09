package api

// ErrorResponse is the JSON body returned to clients when a request fails.
// It wraps an ErrorResponse object.
type ErrorResponseWrapper struct {
	Error ErrorResponse `json:"error"`
}

// ErrorResponse is the error returned to clients when a request fails.
// It mirrors the public fields of Error and additionally carries the HTTP
// status code to use for the response.
type ErrorResponse struct {
	Code        ErrorCode  `json:"code"`
	Description string     `json:"description"`
	Attributes  Attributes `json:"attributes"`

	// The status code, defaults to http.StatusInternalServerError if not set
	StatusCode int `json:"-"`
}
