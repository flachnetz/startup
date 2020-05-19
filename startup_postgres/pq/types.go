package pq

import (
	"encoding/json"
	"github.com/jackc/pgtype"
)

type StringArray struct {
	pgtype.TextArray
}

func (s StringArray) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.Elements)
}

func NewStringArray(value []string) StringArray {
	result := pgtype.TextArray{}
	_ = result.Set(value) // string slice should never fail
	return StringArray{result}
}

func (s StringArray) Get(i int) string {
	return s.Elements[i].String
}

func (s StringArray) Len() int {
	return len(s.Elements)
}

func (s StringArray) AsSlice() []string {
	var result []string
	for _, v := range s.Elements {
		result = append(result, v.String)
	}
	return result
}

type Int64Array struct {
	pgtype.Int8Array
}

func (i Int64Array) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.Elements)
}

func NewInt64Array(value []int64) Int64Array {
	result := pgtype.Int8Array{}
	_ = result.Set(value) // string slice should never fail
	return Int64Array{result}
}

func (s Int64Array) AsSlice() []int64 {
	var result []int64
	for _, v := range s.Elements {
		result = append(result, v.Int)
	}
	return result
}
