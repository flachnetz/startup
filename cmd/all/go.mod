module github.com/flachnetz/startup/cmd/all

go 1.12

require (
	github.com/flachnetz/startup v1.6.1
	github.com/flachnetz/startup/lib/events v1.0.1
	github.com/flachnetz/startup/startup_consul v1.0.0
	github.com/flachnetz/startup/startup_events v1.0.0
	github.com/flachnetz/startup/startup_http v0.0.0-20190326145052-c3bfae325350
	github.com/flachnetz/startup/startup_kafka v1.0.0
	github.com/flachnetz/startup/startup_metrics v1.0.0
	github.com/flachnetz/startup/startup_postgres v1.1.2
	github.com/flachnetz/startup/startup_schema v1.0.0
	github.com/flachnetz/startup/startup_tracing v1.6.2
	github.com/flachnetz/startup/startup_tracing_pg v1.1.2
	github.com/jmoiron/sqlx v1.2.0
	github.com/julienschmidt/httprouter v1.2.0
	github.com/mitchellh/go-testing-interface v1.0.0 // indirect
	google.golang.org/genproto v0.0.0-20180831171423-11092d34479b // indirect
)
