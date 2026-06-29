package echo

import (
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
)

type (
	User     string
	Password string
)

type RestUsers struct {
	RestUser map[User]Password `long:"http-rest-user" env:"HTTP_REST_USER" description:"Usernames for REST API access."`
}

func BasicAuthValidator(user User, password Password) middleware.BasicAuthValidator {
	return func(c *echo.Context, inUser string, inPassword string) (bool, error) {
		if inUser == string(user) && inPassword == string(password) {
			return true, nil
		}
		return false, nil
	}
}

func BasicAuthUsersValidator(users map[User]Password) middleware.BasicAuthValidator {
	return func(c *echo.Context, user string, password string) (bool, error) {
		if pass, ok := users[User(user)]; ok && string(pass) == password {
			return true, nil
		}
		return false, nil
	}
}
