package startup_logrus

import (
	"errors"
	"github.com/sirupsen/logrus"
)

type FieldError struct {
	Err    error
	Fields logrus.Fields
}

func (e *FieldError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return "unknown error with fields"
}

func (e *FieldError) Unwrap() error {
	return e.Err
}

func WithFields(err error, fields logrus.Fields) *FieldError {
	if err == nil {
		return nil
	}
	var fe *FieldError
	if errors.As(err, &fe) {
		for k, v := range fields {
			fe.Fields[k] = v
		}
		return fe
	}
	return &FieldError{
		Err:    err,
		Fields: fields,
	}
}
