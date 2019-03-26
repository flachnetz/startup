module github.com/flachnetz/startup/startup_tracing_pg

go 1.12

require (
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/flachnetz/startup/startup_postgres v1.1.1
	github.com/flachnetz/startup/startup_tracing v1.6.1
	github.com/gchaincl/sqlhooks v1.1.0
	github.com/jmoiron/sqlx v1.2.0
	github.com/lib/pq v1.0.0
	github.com/mattn/go-isatty v0.0.7 // indirect
	github.com/opentracing/opentracing-go v1.0.2
	golang.org/x/crypto v0.0.0-20190320223903-b7391e95e576 // indirect
	golang.org/x/net v0.0.0-20190324223953-e3b2ff56ed87 // indirect
	golang.org/x/sys v0.0.0-20190322080309-f49334f85ddc // indirect
)

replace github.com/gchaincl/sqlhooks v1.1.0 => github.com/coordcity/sqlhooks v1.1.1-0.20190314160841-345b1ec84db5
