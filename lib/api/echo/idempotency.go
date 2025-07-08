package echo

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/flachnetz/startup/v2/lib/api"

	"github.com/flachnetz/startup/v2/lib/api/idempotency"
	"github.com/flachnetz/startup/v2/lib/ql"
	"github.com/flachnetz/startup/v2/startup_logrus"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
)

const IdempotencyKey = "Idempotency-Key"

// responseWriterInterceptor captures the response body and status code.
type responseWriterInterceptor struct {
	http.ResponseWriter
	body       *bytes.Buffer
	header     http.Header
	statusCode int
}

func (w *responseWriterInterceptor) Write(b []byte) (int, error) {
	if w.body == nil {
		w.body = bytes.NewBufferString("")
	}
	w.body.Write(b)
	return w.ResponseWriter.Write(b)
}

func (w *responseWriterInterceptor) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	for key, values := range w.header {
		for _, value := range values {
			w.ResponseWriter.Header().Add(key, value)
		}
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

// Header returns the response headers.
func (w *responseWriterInterceptor) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

// IdempotencyMiddlewareEcho provides idempotency for Echo handlers.
func IdempotencyMiddlewareEcho(store idempotency.IdempotencyStore) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// Only apply to methods that change state
			method := c.Request().Method
			if method != http.MethodPost && method != http.MethodPatch && method != http.MethodPut {
				return next(c)
			}

			ctx := c.Request().Context()

			idempotencyKey := c.Request().Header.Get(IdempotencyKey)
			loggerOf := startup_logrus.LoggerOf(ctx)
			if idempotencyKey == "" {
				return api.ErrBadRequest.WithDescription("missing idempotency key")
			}
			loggerOf = loggerOf.WithField("idempotency_key", idempotencyKey)

			err := ql.InNewTransaction(ctx, store.DB(), func(ctx ql.TxContext) error {
				reqRecord, err := store.Get(ctx, idempotencyKey)
				if err != nil {
					return errors.New("failed to retrieve idempotency record: " + err.Error())
				}

				// Handle existing requests
				if reqRecord != nil {
					switch reqRecord.Status {
					case idempotency.Completed:
						loggerOf.Debugf("idempotency key '%s' already processed. Returning saved response", idempotencyKey)

						var headers http.Header
						if err := json.Unmarshal(reqRecord.ResponseHeaders, &headers); err == nil {
							for key, values := range headers {
								for _, value := range values {
									c.Response().Header().Add(key, value)
								}
							}
						}
						// add idempotency key to response headers
						c.Response().Header().Set(IdempotencyKey, idempotencyKey)

						// Use Blob to write the raw body with the correct status code and content type
						contentType := headers.Get("Content-Type")
						return c.Blob(int(reqRecord.ResponseCode.Int64), contentType, reqRecord.ResponseBody)

					case idempotency.Error:
						loggerOf.Debugf("idempotency key '%s' resulted in an error, will retry business logic", idempotencyKey)
					case idempotency.Pending:
						// if it is still pending for more than 2 minutes, we can assume it is stuck
						if time.Since(reqRecord.CreatedAt) > 2*time.Minute {
							return errors.Errorf("idempotency key '%s' is stuck in pending state", idempotencyKey)
						}
						return errors.Errorf("idempotency key '%s' is still pending, please retry later", idempotencyKey)
					}
				}

				// Handle new requests: Create pending record
				if err := store.Create(ctx, idempotencyKey); err != nil {
					return errors.Errorf("failed to create idempotency record for key '%s': %q", idempotencyKey, err)
				}

				// Call the actual handler and capture the response
				originalWriter := c.Response().Writer
				interceptor := &responseWriterInterceptor{
					ResponseWriter: originalWriter,
					body:           bytes.NewBufferString(""),
					statusCode:     http.StatusOK, // Default
					header:         make(http.Header),
				}
				c.Response().Writer = interceptor
				req := c.Request().Clone(ctx)
				c.SetRequest(req)
				handlerErr := next(c)
				c.Response().Writer = originalWriter

				headersBytes, err := json.Marshal(c.Response().Header())
				if err != nil {
					// Log the error but do not return it to avoid breaking the response flow
					loggerOf.Errorf("Failed to marshal response headers: %v", err)
				}

				if handlerErr != nil || interceptor.statusCode >= 400 {
					err = store.Error(ctx, idempotencyKey, interceptor.statusCode, headersBytes, interceptor.body.Bytes())
					return handlerErr
				}

				err = store.Update(ctx, idempotencyKey, interceptor.statusCode, headersBytes, interceptor.body.Bytes())
				if err != nil {
					// Log the error but do not return it to avoid breaking the response flow
					loggerOf.Errorf("Failed to update idempotency record for key '%s': %q", idempotencyKey, err)
				}
				return nil
			})

			return err
		}
	}
}
