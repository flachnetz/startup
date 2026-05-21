package echo

import (
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
)

type RestUsers struct {
	RestUser map[string]string `long:"http-rest-user" env:"HTTP_REST_USER" description:"Usernames for REST API access."`
}

func BasicAuthValidator(basicAuthUser string, basicAuthPassword string) middleware.BasicAuthValidator {
	return func(c *echo.Context, user string, password string) (bool, error) {
		if user == basicAuthUser && password == basicAuthPassword {
			return true, nil
		}
		return false, nil
	}
}

func BasicAuthUsersValidator(users map[string]string) middleware.BasicAuthValidator {
	return func(c *echo.Context, user string, password string) (bool, error) {
		if pass, ok := users[user]; ok && pass == password {
			return true, nil
		}
		return false, nil
	}
}
