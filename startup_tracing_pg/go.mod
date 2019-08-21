module github.com/flachnetz/startup/startup_tracing_pg

go 1.12

require (
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/flachnetz/startup/startup_postgres v1.1.5
	github.com/flachnetz/startup/startup_tracing v1.6.6
	github.com/gchaincl/sqlhooks v1.1.0
	github.com/go-sql-driver/mysql v1.4.1 // indirect
	github.com/gobuffalo/packr v1.30.1 // indirect
	github.com/lib/pq v1.2.0
	github.com/mattn/go-sqlite3 v1.11.0 // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/rubenv/sql-migrate v0.0.0-20190618074426-f4d34eae5a5c // indirect
	google.golang.org/appengine v1.6.1 // indirect
)

replace (
	github.com/flachnetz/startup/startup_base => ../startup_base
	github.com/flachnetz/startup/startup_postgres => ../startup_postgres
	github.com/flachnetz/startup/startup_tracing => ../startup_tracing
	github.com/gchaincl/sqlhooks v1.1.0 => github.com/coordcity/sqlhooks v1.1.1-0.20190314160841-345b1ec84db5
)
