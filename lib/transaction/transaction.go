package transaction

import (
	"fmt"
	"time"
	"github.com/flachnetz/startup_tracing"
	"github.com/jmoiron/sqlx"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
)

type Helper struct {
	*sqlx.DB
	log                logrus.FieldLogger
	loggingPrefix      string
	tracingServiceName string
}

func New(db *sqlx.DB, loggingPrefix, tracingServiceName string) Helper {
	return Helper{
		DB:                 db,
		log:                logrus.WithField("prefix", loggingPrefix),
		loggingPrefix:      loggingPrefix,
		tracingServiceName: tracingServiceName,
	}
}


// Ends the given transaction. This method will either commit the transaction if
// the given recoverValue is nil, or rollback the transaction if it is non nil.
func WithTransaction(db *sqlx.DB, fn func(tx *sqlx.Tx) error) (err error) {

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
				tx.Rollback()
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
