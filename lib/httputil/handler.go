package httputil

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"reflect"
	"strconv"

	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/flachnetz/startup/lib/mapper"
	"gopkg.in/go-playground/validator.v9"
	"net/url"
)

type ErrorResponse struct {
	Status int    `json:"status"`
	Error  string `json:"error"`
}

type Header struct {
	Key     string
	Value   string
	Replace bool
}

var ErrorMapping = map[error]int{
	sql.ErrNoRows: http.StatusNotFound,
	sql.ErrTxDone: http.StatusInternalServerError,
}

func WriteJSON(w http.ResponseWriter, status int, value interface{}, headers ...Header) error {
	body, err := json.Marshal(value)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, err)
		return err
	}

	return WriteBody(w, status, "application/json; charset=utf-8", body, headers...)
}

func WriteError(writer http.ResponseWriter, status int, err error) error {
	logrus.Warn("Writing response error: ", err)

	return WriteJSON(writer, status, ErrorResponse{
		Status: status,
		Error:  err.Error(),
	})
}

func WriteBody(writer http.ResponseWriter, status int, contentType string, content []byte, headers ...Header) error {
	writer.Header().Set("Content-Length", strconv.Itoa(len(content)))
	writer.Header().Set("Content-Type", contentType)

	// write extra response headers
	for _, header := range headers {
		if header.Replace {
			writer.Header().Set(header.Key, header.Value)
		} else {
			writer.Header().Add(header.Key, header.Value)
		}
	}

	// write header writes the status code and all previously set headers.
	writer.WriteHeader(status)

	_, err := writer.Write(content)
	return err
}

func WriteGenericError(w http.ResponseWriter, err error) {
	WriteResponseValue(w, nil, err)
}

func WriteResponseValue(w http.ResponseWriter, value interface{}, err error, headers ...Header) {
	WriteResponseValueStatus(w, http.StatusOK, value, err, headers...)
}

func WriteResponseValueStatus(w http.ResponseWriter, statusCode int, value interface{}, err error, headers ...Header) {
	if err == nil {
		if value == nil {
			if statusCode == http.StatusOK {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(statusCode)
			}

		} else if isSliceAndNil(value) {
			WriteJSON(w, statusCode, []string{}, headers...)

		} else {
			WriteJSON(w, statusCode, value, headers...)
		}
	} else {
		errorCode := ErrorMapping[errors.Cause(err)]
		if errorCode == 0 {
			errorCode = http.StatusInternalServerError
		}

		WriteError(w, errorCode, err)
	}
}

// for a value that is wrapped into an interface, this method
// tries to check, if the value is a slice and if that one is nil.
func isSliceAndNil(value interface{}) bool {
	return value != nil && reflect.TypeOf(value).Kind() == reflect.Slice && reflect.ValueOf(value).IsNil()
}

func ExtractAndCall(target interface{}, w http.ResponseWriter, r *http.Request, params httprouter.Params, handler func() (interface{}, error)) {
	err := ExtractParameters(target, r, params)
	if err != nil {
		WriteError(w, http.StatusBadRequest, err)
		return
	}

	// run the handler and write the response values
	value, err := handler()
	WriteResponseValue(w, value, err)
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

func ExtractParameters(target interface{}, r *http.Request, params httprouter.Params) error {
	if err := mapper.Map("path", paramsSource(params), target); err != nil {
		return errors.WithMessage(err, "mapping path parameters")
	}

	if err := mapper.Map("query", valuesSource(r.URL.Query()), target); err != nil {
		return errors.WithMessage(err, "mapping query parameters")
	}

	if err := parameterValidator.Struct(target); err != nil {
		return errors.WithMessage(err, fmt.Sprintf("validating parameters: %+v", target))
	}

	return nil
}

// Wraps a normal http.Handler middleware and wraps it so it can  be used
// with httprouter.Handle functions.
func AdaptMiddleware(w func(http.Handler) http.Handler) func(handle httprouter.Handle) httprouter.Handle {
	return func(handle httprouter.Handle) httprouter.Handle {
		return func(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
			wrapped := w(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				handle(writer, request, params)
			}))

			wrapped.ServeHTTP(writer, request)
		}
	}
}
