package startup_tracing_pg

import (
	"fmt"
	"github.com/flachnetz/startup/lib/transaction"
	"github.com/flachnetz/startup/startup_tracing"
	"github.com/jmoiron/sqlx"
	"github.com/opentracing/opentracing-go"
	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
	"time"
)


type TracedHelper struct {
	transaction.Helper
}

func (p *Helper) WithTransaction(tag string, fn func(tx *sqlx.Tx) error) error {
	var err error

	startTime := time.Now()

	metric := fmt.Sprintf("pq.%s.transaction[tag:%s]", p.loggingPrefix, tag)
	metrics.GetOrRegisterTimer(metric, nil).Time(func() {
		err = startup_tracing.TraceChild(p.tracingServiceName+"-db", func(span opentracing.Span) error {
			span.SetTag("dd.service", p.tracingServiceName)
			span.SetTag("dd.resource", "tx:"+tag)
			return WithTransaction(p.DB, fn)
		})
	})

	p.log.Debugf("Transaction '%s' took %s", tag, time.Since(startTime))

	return err
}
