package echox

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v5"
)

// Validator implements echo.Validator and can be set as validator
// in echo. It is probably better to use BindAndValidate though.
var Validator = makeValidator()

type echoValidator struct {
	Validator *validator.Validate
}

func (e echoValidator) Validate(i any) error {
	return e.Validator.Struct(i)
}

// BindAndValidate binds the request data to a new instance of type T
// using c.Bind and then validates the struct using Validator.
func BindAndValidate[T any](c *echo.Context) (T, error) {
	ctx := c.Request().Context()

	var payload T
	if err := c.Bind(&payload); err != nil {
		var tZero T
		return tZero, fmt.Errorf("bind %T: %w", tZero, err)
	}

	if err := Validator.Validator.StructCtx(ctx, &payload); err != nil {
		var tZero T
		return tZero, fmt.Errorf("validate %T: %w", tZero, err)
	}

	return payload, nil
}

func makeValidator() echoValidator {
	v := validator.New()

	v.RegisterTagNameFunc(func(fld reflect.StructField) string {
		// Split the JSON tag at the first comma
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]

		// If the JSON tag is "-" (meaning "don't include this field in JSON"),
		// return an empty string so the validator uses the original field name
		// or potentially skips it based on other rules.
		if name == "-" {
			// Returning an empty string tells the validator to skip this tag name transformation
			// and potentially fall back to the default field name or other registered tag names.
			// For the specific goal of using JSON names, returning empty for '-' is appropriate.
			return ""
		}

		// Otherwise, return the JSON field name
		return name
	})

	return echoValidator{Validator: v}
}
