package echo

import (
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func BasicAuthValidator(basicAuthUser string, basicAuthPassword string) middleware.BasicAuthValidator {
	return func(user string, password string, context echo.Context) (bool, error) {
		if user == basicAuthUser && password == basicAuthPassword {
			return true, nil
		}
		return false, nil
	}
}
