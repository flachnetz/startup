package startup_http

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"

	. "github.com/flachnetz/startup/v2/startup_logrus"

	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"gopkg.in/go-playground/validator.v9"

	"github.com/go-json-experiment/json"
)

type ResponseValue struct {
	StatusCode int
	Headers    http.Header
	Body       interface{}
}

type ErrorResponse struct {
	Status     int    `json:"status"`
	ErrorValue string `json:"error"`
}

func (err ErrorResponse) Error() string {
	return err.ErrorValue
}

func WriteJSON(ctx context.Context, w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)

	err := json.MarshalWrite(w, value, json.FormatNilSliceAsNull(false), json.FormatNilMapAsNull(false))
	if err != nil {
		LoggerOf(ctx).Warnf("Failed to write json response: %s", err)
	}
}

// WriteError formats an error value. The default implementation wraps the error text into
// a ErrorResponse object and serializes it to json.
//
// You can set this variable to do your own custom error mapping.
var WriteError = func(ctx context.Context, writer http.ResponseWriter, statusCode int, err error) {
	LoggerOf(ctx).Warnf("Writing response error: %s", err)

	if statusCode == 0 {
		statusCode = MapErrorToStatusCode(err)
	}

	WriteJSON(ctx, writer, statusCode, ErrorResponse{
		Status:     statusCode,
		ErrorValue: err.Error(),
	})
}

// MapErrorToStatusCode maps an error to a status code. The default implementation
// converts [sql.ErrNoRows] into status code 404, and returns [http.StatusInternalServerError]
// in all other cases.
//
// You can set this variable to do your own custom error mapping.
var MapErrorToStatusCode = func(err error) int {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return http.StatusNotFound

	default:
		return http.StatusInternalServerError
	}
}

func WriteBody(ctx context.Context, writer http.ResponseWriter, statusCode int, contentType string, content []byte) {
	writer.Header().Set("Content-Length", strconv.Itoa(len(content)))

	if contentType != "" {
		writer.Header().Set("Content-Type", contentType)
	}

	// use default status code if not set
	if statusCode == 0 {
		statusCode = 200
	}

	// fix status code to 204 if body is empty
	if statusCode == 200 && len(content) == 0 {
		statusCode = 204
	}

	// write header writes the status code and all previously set headers.
	writer.WriteHeader(statusCode)

	_, err := writer.Write(content)

	if err != nil {
		// We failed to write our response. The client probably disconnected
		LoggerOf(ctx).Warnf("Failed to write response body: %s", err)
	}
}

func WriteResponseValue(ctx context.Context, w http.ResponseWriter, value interface{}, err error) {
	if err != nil {
		// handle ErrorResponse errors correctly
		if errorValue := (ErrorResponse{}); errors.As(err, &errorValue) {
			WriteError(ctx, w, errorValue.Status, errorValue)
			return
		}

		// map error to status code
		WriteError(ctx, w, MapErrorToStatusCode(err), err)
		return
	}

	// now check special response value type
	if responseValue, ok := value.(ResponseValue); ok {
		// copy headers to response
		for name, values := range responseValue.Headers {
			w.Header()[name] = values
		}

		if responseValue.Body == nil {
			WriteBody(ctx, w, responseValue.StatusCode, "", []byte(""))
			return
		}

		if body, ok := responseValue.Body.([]byte); ok {
			WriteBody(ctx, w, responseValue.StatusCode, "", body)
			return
		}

		// fallback to json encoding
		WriteJSON(ctx, w, responseValue.StatusCode, responseValue.Body)
		return
	}

	// pointer to a response value need to be handled
	if responseValue, ok := value.(*ResponseValue); ok {
		// recurse to te code above
		WriteResponseValue(ctx, w, *responseValue, nil)
		return
	}

	// re-use the code from above
	responseValue := ResponseValue{StatusCode: 200, Body: value}
	WriteResponseValue(ctx, w, responseValue, nil)
}

// ExtractAndCall extracts and validates the path and request parameters before
// calling the given method.
func ExtractAndCall(target interface{}, w http.ResponseWriter, r *http.Request, params httprouter.Params, handler func() (interface{}, error)) {
	ctx := r.Context()

	if target != nil {
		// parse 'path' and 'url' parameters
		err := ExtractParameters(target, r, params)
		if err != nil {
			WriteError(ctx, w, http.StatusBadRequest, err)
			return
		}
	}

	// run the handler and write the response values
	value, err := handler()
	WriteResponseValue(ctx, w, value, err)
}

// ExtractAndCallWithBody parses the body, path and request parameters and
// then calls the given handler function.
func ExtractAndCallWithBody(
	target interface{},
	body interface{},
	w http.ResponseWriter,
	r *http.Request,
	params httprouter.Params,
	handler func() (interface{}, error),
) {
	ExtractAndCall(target, w, r, params, func() (interface{}, error) {
		if byteSlice, ok := body.(*[]byte); ok {
			var err error

			// read the body into the provided slice
			*byteSlice, err = io.ReadAll(r.Body)
			if err != nil {
				return nil, errors.WithMessage(err, "reading body to bytes")
			}

		} else {
			if err := json.UnmarshalRead(r.Body, body); err != nil {
				return nil, errors.WithMessage(err, "parsing request body as json")
			}

			if reflect.ValueOf(body).Elem().Kind() == reflect.Struct {
				if err := parameterValidator.Struct(body); err != nil {
					return nil, errors.WithMessage(err, "validating request body")
				}
			}
		}

		return handler()
	})
}

var parameterValidator = validator.New()

type paramsSource httprouter.Params

func (s paramsSource) Get(key string) (string, bool) {
	value := httprouter.Params(s).ByName(key)
	return value, value != ""
}

type valuesSource url.Values

func (s valuesSource) Get(key string) (string, bool) {
	value := url.Values(s).Get(key)
	return value, value != ""
}

// ExtractParameters parses the 'path' parameters as well as the 'query' parameters into the given object.
// Returns an error, if parsing failed.
func ExtractParameters(target interface{}, r *http.Request, params httprouter.Params) error {
	if err := Map("path", paramsSource(params), target); err != nil {
		return errors.WithMessage(err, "mapping path parameters")
	}

	if err := Map("query", valuesSource(r.URL.Query()), target); err != nil {
		return errors.WithMessage(err, "mapping query parameters")
	}

	if err := parameterValidator.Struct(target); err != nil {
		return errors.WithMessage(err, fmt.Sprintf("validating parameters: %+v", target))
	}

	return nil
}
