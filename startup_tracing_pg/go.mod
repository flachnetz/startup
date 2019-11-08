module github.com/flachnetz/startup/startup_tracing_pg

go 1.12

require (
	github.com/flachnetz/startup/startup_base v1.0.1
	github.com/flachnetz/startup/startup_postgres v1.1.6
	github.com/flachnetz/startup/startup_tracing v1.6.6
	github.com/gchaincl/sqlhooks v1.1.0
	github.com/lib/pq v1.2.0
	github.com/opentracing/opentracing-go v1.1.0
)

replace (
	github.com/flachnetz/startup/startup_base => ../startup_base
	github.com/flachnetz/startup/startup_postgres => ../startup_postgres
	github.com/flachnetz/startup/startup_tracing => ../startup_tracing
	github.com/gchaincl/sqlhooks v1.1.0 => github.com/coordcity/sqlhooks v1.1.1-0.20190314160841-345b1ec84db5
)
