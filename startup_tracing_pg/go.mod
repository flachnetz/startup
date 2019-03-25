module github.com/flachnetz/startup/startup_tracing_pg

go 1.12

require (
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/flachnetz/startup/startup_postgres v1.0.0
	github.com/flachnetz/startup/startup_tracing v1.5.11
	github.com/gchaincl/sqlhooks v1.1.0
	github.com/gobuffalo/packr v1.24.0 // indirect
	github.com/jmoiron/sqlx v1.2.0
	github.com/lib/pq v1.0.0
	github.com/opentracing/opentracing-go v1.0.2
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a
	github.com/rubenv/sql-migrate v0.0.0-20190212093014-1007f53448d7 // indirect
	github.com/sirupsen/logrus v1.4.0
	github.com/ziutek/mymysql v1.5.4 // indirect
	gopkg.in/gorp.v1 v1.7.2 // indirect
)

replace github.com/gchaincl/sqlhooks v1.1.0 => github.com/coordcity/sqlhooks v1.1.1-0.20190314160841-345b1ec84db5
