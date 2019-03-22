package startup_tracing_pg

import (
	"fmt"
	"github.com/flachnetz/startup/startup_postgres"
	"github.com/flachnetz/startup/startup_tracing"
	"github.com/jmoiron/sqlx"
	"github.com/opentracing/opentracing-go"
	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
	"time"
)


type TracedHelper struct {
	*sqlx.DB
	log                logrus.FieldLogger
	loggingPrefix      string
	tracingServiceName string
}

func New(db *sqlx.DB, loggingPrefix, tracingServiceName string) TracedHelper {
	return TracedHelper{
		DB:                 db,
		log:                logrus.WithField("prefix", loggingPrefix),
		loggingPrefix:      loggingPrefix,
		tracingServiceName: tracingServiceName,
	}
}

func (p *TracedHelper) WithTransaction(tag string, fn func(tx *sqlx.Tx) error) error {
	var err error

	startTime := time.Now()

	metric := fmt.Sprintf("pq.%s.transaction[tag:%s]", p.loggingPrefix, tag)
	metrics.GetOrRegisterTimer(metric, nil).Time(func() {
		err = startup_tracing.TraceChild(p.tracingServiceName+"-db", func(span opentracing.Span) error {
			span.SetTag("dd.service", p.tracingServiceName)
			span.SetTag("dd.resource", "tx:"+tag)
			return startup_postgres.WithTransaction(p.DB, fn)
		})
	})

	p.log.Debugf("Transaction '%s' took %s", tag, time.Since(startTime))

	return err
}
