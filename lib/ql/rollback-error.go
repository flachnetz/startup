package ql

import "fmt"

type rollbackError struct {
	err  error
	rerr error
}

func (r rollbackError) Error() string {
	return fmt.Sprintf("error during rollback: %q due to error: %s", r.rerr, r.err)
}

func (r rollbackError) Unwrap() error {
	return r.err
}

type commitError struct {
	err  error
	cerr error
}

func (r commitError) Error() string {
	if r.err != nil {
		return fmt.Sprintf("error during commit: %q, original error was: %s", r.cerr, r.err)
	} else {
		return fmt.Sprintf("error during commit: %s", r.cerr)
	}
}

func (r commitError) Unwrap() error {
	return r.err
}
