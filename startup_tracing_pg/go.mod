module github.com/flachnetz/startup/startup_tracing_pg

go 1.12

require (
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/flachnetz/startup/startup_postgres v1.1.4
	github.com/flachnetz/startup/startup_tracing v1.6.2
	github.com/gchaincl/sqlhooks v1.1.0
	github.com/go-sql-driver/mysql v1.4.1 // indirect
	github.com/gobuffalo/packr v1.30.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.9.3 // indirect
	github.com/lib/pq v1.1.1
	github.com/mattn/go-colorable v0.1.2 // indirect
	github.com/mattn/go-sqlite3 v1.10.0 // indirect
	github.com/opentracing/opentracing-go v1.0.2
	github.com/rubenv/sql-migrate v0.0.0-20190618074426-f4d34eae5a5c // indirect
	golang.org/x/crypto v0.0.0-20190701094942-4def268fd1a4 // indirect
	golang.org/x/net v0.0.0-20190628185345-da137c7871d7 // indirect
	golang.org/x/sys v0.0.0-20190626221950-04f50cda93cb // indirect
	google.golang.org/appengine v1.6.1 // indirect
	google.golang.org/genproto v0.0.0-20190701230453-710ae3a149df // indirect
	google.golang.org/grpc v1.22.0 // indirect
)

replace github.com/gchaincl/sqlhooks v1.1.0 => github.com/coordcity/sqlhooks v1.1.1-0.20190314160841-345b1ec84db5
