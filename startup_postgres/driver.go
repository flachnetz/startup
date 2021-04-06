package startup_postgres

import (
	"context"
	"database/sql/driver"
	"github.com/jackc/pgx/v4/stdlib"
)

var _driver driver.Driver = stdlib.GetDefaultDriver()

type Middleware func(driver.Driver) driver.Driver

func Use(middleware Middleware) {
	_driver = middleware(_driver)
}

func openConnector(dsn string) (driver.Connector, error) {
	if driverContext, ok := _driver.(driver.DriverContext); ok {
		return driverContext.OpenConnector(dsn)
	} else {
		return &driverConnector{_driver, dsn}, nil
	}
}

type driverConnector struct {
	driver driver.Driver
	dsn    string
}

func (s *driverConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return s.driver.Open(s.dsn)
}

func (s *driverConnector) Driver() driver.Driver {
	return s.driver
}
