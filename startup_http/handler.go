package startup_http

import (
	"context"
	"database/sql"
	"encoding/json"
	. "github.com/flachnetz/startup/startup_logrus"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"

	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"gopkg.in/go-playground/validator.v9"
	"net/url"
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

func ErrorNotFound(err error) error {
	return ErrorResponse{Status: http.StatusNotFound, ErrorValue: err.Error()}
}

func ErrorBadRequest(err error) error {
	return ErrorResponse{Status: http.StatusBadRequest, ErrorValue: err.Error()}
}

func ErrorInternalServerError(err error) error {
	return ErrorResponse{Status: http.StatusInternalServerError, ErrorValue: err.Error()}
}

var ErrorMapper = func(err error) int {
	switch errors.Cause(err) {
	case sql.ErrNoRows:
		return http.StatusNotFound

	default:
		return http.StatusInternalServerError
	}
}

func WriteJSON(w http.ResponseWriter, status int, value interface{}) {
	body, err := json.Marshal(value)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, err)
		return
	}

	WriteBody(w, status, "application/json; charset=utf-8", body)
}

func WriteError(writer http.ResponseWriter, status int, err error) {
	WriteErrorContext(context.Background(), writer, status, err)
}

func WriteErrorContext(ctx context.Context, writer http.ResponseWriter, status int, err error) {
	GetLogger(ctx, "httpd").Warn("Writing response error: ", err)

	if status < 400 {
		status = http.StatusInternalServerError
	}

	WriteJSON(writer, status, ErrorResponse{
		Status:     status,
		ErrorValue: err.Error(),
	})
}

func WriteBody(writer http.ResponseWriter, statusCode int, contentType string, content []byte) {
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

	// We failed to write our response, what can we do here?
	// Not much, we will just panic.
	if err != nil {
		panic(err)
	}
}

func WriteGenericError(w http.ResponseWriter, err error) {
	WriteResponseValue(w, nil, err)
}

func marshalResponseValue(body interface{}) ([]byte, string, error) {
	if body == nil {
		return []byte{}, "", nil
	}

	if body, ok := body.([]byte); ok {
		return body, "", nil
	}

	if isSliceAndNil(body) {
		return []byte("[]"), "application/json", nil
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, "", err
	}

	return data, "application/json", nil
}

func WriteResponseValue(w http.ResponseWriter, value interface{}, err error) {
	if err != nil {
		// handle ErrorResponse errors correctly
		if errorValue, ok := err.(ErrorResponse); ok {
			WriteError(w, errorValue.Status, errorValue)
			return
		}

		// map error to status code
		WriteError(w, ErrorMapper(err), err)
		return
	}

	// now check special response value type
	if responseValue, ok := value.(ResponseValue); ok {
		data, contentType, err := marshalResponseValue(responseValue.Body)
		if err != nil {
			WriteError(w, http.StatusInternalServerError, err)
			return
		}

		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}

		// set headers
		for name, values := range responseValue.Headers {
			w.Header()[name] = values
		}

		WriteBody(w, responseValue.StatusCode, "", data)
		return
	}

	// pointer to a response value need to be handled
	if responseValue, ok := value.(*ResponseValue); ok {
		// recurse to te code above
		WriteResponseValue(w, *responseValue, nil)
		return
	}

	// re-use the code from above
	responseValue := ResponseValue{StatusCode: 200, Body: value}
	WriteResponseValue(w, responseValue, nil)
}

// for a value that is wrapped into an interface, this method
// tries to check, if the value is a slice and if that one is nil.
func isSliceAndNil(value interface{}) bool {
	return value != nil && reflect.TypeOf(value).Kind() == reflect.Slice && reflect.ValueOf(value).IsNil()
}

// Extracts and validates the path and request parameters before
// calling the given method.
func ExtractAndCall(target interface{}, w http.ResponseWriter, r *http.Request, params httprouter.Params, handler func() (interface{}, error)) {
	if target != nil {
		// parse 'path' and 'url' parameters
		err := ExtractParameters(target, r, params)
		if err != nil {
			WriteErrorContext(r.Context(), w, http.StatusBadRequest, err)
			return
		}
	}

	// run the handler and write the response values
	value, err := handler()
	WriteResponseValue(w, value, err)
}

// Parses the body, path and request parameters and
// then calls the given handler function.
func ExtractAndCallWithBody(
	target interface{},
	body interface{},
	w http.ResponseWriter,
	r *http.Request,
	params httprouter.Params,
	handler func() (interface{}, error)) {

	ExtractAndCall(target, w, r, params, func() (interface{}, error) {
		if byteSlice, ok := body.(*[]byte); ok {
			var err error

			// read the body into the provided slice
			*byteSlice, err = ioutil.ReadAll(r.Body)
			if err != nil {
				return nil, errors.WithMessage(err, "reading body to bytes")
			}

		} else {
			if err := json.NewDecoder(r.Body).Decode(body); err != nil {
				return nil, errors.WithMessage(err, "parsing request body as json")
			}

			if err := parameterValidator.Struct(body); err != nil {
				return nil, errors.WithMessage(err, "validating request body")
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

// Parses the 'path' parameters as well as the 'query' parameters into the given object.
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
