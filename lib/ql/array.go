package ql

import (
	"github.com/jackc/pgx/v5/pgtype"
)

var pgtypeMap = pgtype.NewMap()

type StringArray []string

func (s *StringArray) Scan(src any) error {
	return pgtypeMap.SQLScanner((*[]string)(s)).Scan(src)
}

func NewStringArray(value []string) StringArray {
	return value
}
