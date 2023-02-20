package ql

import (
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"
)

// Get runs the given query and parses the result into an object of type T.
// If not object can be found the method will return sql.ErrNoRows and a value of nil.
func Get[T any](ctx TxContext, query string, args ...interface{}) (*T, error) {
	var resultValue T

	err := sqlx.GetContext(ctx, ctx, &resultValue, query, args...)
	if err != nil {
		return nil, err
	}

	return &resultValue, nil
}

// FirstOrNil is similar to Get. It will only scan the first row of the result. If the query does
// not return any row, this method returns a value of nil and no error.
func FirstOrNil[T any](ctx TxContext, query string, args ...any) (*T, error) {
	result, err := Get[T](ctx, query, args...)
	switch {
	case err == nil, errors.Is(err, sql.ErrNoRows):
		return result, nil
	default:
		return nil, err
	}
}

// Select scans the result into a slice of element type T using sqlx.SelectContext.
func Select[T any](ctx TxContext, query string, args ...any) ([]T, error) {
	var resultValues []T

	if err := sqlx.SelectContext(ctx, ctx, &resultValues, query, args...); err != nil {
		return nil, err
	}

	return resultValues, nil
}

// ExecNamed just execute the given statement in the provided transaction.
func ExecNamed(ctx TxContext, stmt string, args any) error {
	_, err := sqlx.NamedExecContext(ctx, ctx, stmt, args)
	return err
}

// ExecNamedAffected executes the given statement and returns the number of rows that were affected by the statement.
func ExecNamedAffected(ctx TxContext, stmt string, args any) (int, error) {
	res, err := sqlx.NamedExecContext(ctx, ctx, stmt, args)
	if err != nil {
		return 0, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(affected), nil
}

// Exec just execute the given statement in the provided transaction.
func Exec(ctx TxContext, stmt string, args ...any) error {
	_, err := ctx.ExecContext(ctx, stmt, args...)
	return err
}

// ExecAffected executes the given statement and returns the number of rows that were affected by the statement.
// This is especially useful with an `UPDATE` statement.
func ExecAffected(ctx TxContext, stmt string, args ...any) (int, error) {
	res, err := ctx.ExecContext(ctx, stmt, args...)
	if err != nil {
		return 0, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(affected), nil
}

// Iter returns a typed iterator over the rows of the given query. It is the callers
// responsibility to close the returned iterator.
// Most of the time, you want to use Select. Only use this, if you are expecting millions of rows.
func Iter[T any](ctx TxContext, query string, args ...any) (*QueryIter[T], error) {
	rows, err := ctx.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &QueryIter[T]{rows}, nil
}

type QueryIter[T any] struct {
	rows *sqlx.Rows
}

func (q *QueryIter[T]) Close() error {
	return q.rows.Close()
}

func (q *QueryIter[T]) Next() (T, error) {
	var value T
	err := q.rows.Scan(&value)
	return value, err
}

func (q *QueryIter[T]) HasNext() bool {
	return q.rows.Next()
}

func (q *QueryIter[T]) ForEach(consumer func(row T) error) error {
	for q.HasNext() {
		value, err := q.Next()
		if err != nil {
			return err
		}

		if err := consumer(value); err != nil {
			return err
		}
	}

	return nil
}
