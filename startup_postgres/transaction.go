package startup_postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
)

type BeginTxer interface {
	Beginx() (*sqlx.Tx, error)
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
}

type Helper struct {
	*sqlx.DB
}

func New(db *sqlx.DB) Helper {
	return Helper{
		DB: db,
	}
}

func (h *Helper) WithTransaction(fn func(tx *sqlx.Tx) error) (err error) {
	return WithTransaction(h.DB, fn)
}

// Ends the given transaction. This method will either commit the transaction if
// the given recoverValue is nil, or rollback the transaction if it is non nil.
func WithTransaction(db BeginTxer, fn func(tx *sqlx.Tx) error) (err error) {

	var tx *sqlx.Tx

	tx, err = db.Beginx()
	if err != nil {
		return errors.WithMessage(err, "begin transaction")
	}

	defer func() {
		r := recover()
		if r == nil && err == nil {
			metrics.GetOrRegisterTimer("pq.transaction.commit", nil).Time(func() {
				// commit the transaction
				if err = tx.Commit(); err != nil {
					err = errors.WithMessage(err, "commit")
				}
			})

		} else {
			metrics.GetOrRegisterTimer("pq.transaction.rollback", nil).Time(func() {
				if err := tx.Rollback(); err != nil {
					logrus.Warnf("Could not rollback transaction: %s", err)
				}
			})

			// convert recovered value into an error instance
			var ok bool
			if r != nil {
				if err, ok = r.(error); !ok {
					err = fmt.Errorf("%#v", err)
				}
			}

			// and give context to the error
			err = errors.WithMessage(err, "transaction")
		}
	}()

	err = fn(tx)
	return err
}
