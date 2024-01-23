package ql

import (
	"database/sql/driver"

	"github.com/jackc/pgx/v5/pgtype"
)

var pgtypeMap = pgtype.NewMap()

type StringArray []string

func (s *StringArray) Value() (driver.Value, error) {
	if s == nil || len(*s) == 0 {
		return nil, nil
	}
	return s, nil
}

func (s *StringArray) Scan(src any) error {
	return pgtypeMap.SQLScanner((*[]string)(s)).Scan(src)
}

func NewStringArray(value []string) StringArray {
	return value
}
