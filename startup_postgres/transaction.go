package startup_postgres


import (
	"fmt"
	"time"

	"github.com/flachnetz/startup/lib/tracing"
	"github.com/jmoiron/sqlx"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
)

type TransactionHelper struct {
	*sqlx.DB
	log  logrus.FieldLogger
	loggingPrefix string
	tracingServiceName string
}

func NewTransactionHelper(db *sqlx.DB, loggingPrefix, tracingServiceName string) TransactionHelper {
	return TransactionHelper{
		DB:   db,
		log:  logrus.WithField("prefix", loggingPrefix),
		loggingPrefix: loggingPrefix,
		tracingServiceName: tracingServiceName,
	}
}


func (p *TransactionHelper) WithTransaction(tag string, fn func(tx *sqlx.Tx) error) error {
	var err error

	startTime := time.Now()

	metric := fmt.Sprintf("pq.%s.transaction[tag:%s]", p.loggingPrefix, tag)
	metrics.GetOrRegisterTimer(metric, nil).Time(func() {
		err = p.withTransaction(tag, p.DB, fn)
	})

	p.log.Debugf("Transaction '%s' took %s", tag, time.Since(startTime))

	return err
}

// Ends the given transaction. This method will either commit the transaction if
// the given recoverValue is nil, or rollback the transaction if it is non nil.
func (p *TransactionHelper) withTransaction(tag string, db *sqlx.DB, fn func(tx *sqlx.Tx) error) (err error) {
	return tracing.TraceChild(p.tracingServiceName + "-db", func(span opentracing.Span) error {
		span.SetTag("dd.service", p.tracingServiceName)
		span.SetTag("dd.resource", "tx:"+tag)

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
	})
}