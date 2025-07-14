package echo

import (
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type RestUsers struct {
	RestUser map[string]string `long:"http-rest-user" description:"Usernames for REST API access."`
}

func BasicAuthValidator(basicAuthUser string, basicAuthPassword string) middleware.BasicAuthValidator {
	return func(user string, password string, context echo.Context) (bool, error) {
		if user == basicAuthUser && password == basicAuthPassword {
			return true, nil
		}
		return false, nil
	}
}

func BasicAuthUsersValidator(users map[string]string) middleware.BasicAuthValidator {
	return func(user string, password string, context echo.Context) (bool, error) {
		if pass, ok := users[user]; ok && pass == password {
			return true, nil
		}
		return false, nil
	}
}
