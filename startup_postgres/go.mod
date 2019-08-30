module github.com/flachnetz/startup/startup_postgres

go 1.12

require (
	github.com/benbjohnson/clock v0.0.0-20161215174838-7dc76406b6d3
	github.com/flachnetz/startup/startup_base v1.0.1
	github.com/go-sql-driver/mysql v1.4.1 // indirect
	github.com/jmoiron/sqlx v1.2.0
	github.com/kr/pretty v0.1.0 // indirect
	github.com/kr/pty v1.1.8 // indirect
	github.com/lib/pq v1.2.0
	github.com/mattn/go-sqlite3 v1.11.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/rubenv/sql-migrate v0.0.0-20190717103323-87ce952f7079
	github.com/sirupsen/logrus v1.4.2
	github.com/ziutek/mymysql v1.5.4 // indirect
	google.golang.org/appengine v1.6.2 // indirect
	gopkg.in/gorp.v1 v1.7.2 // indirect
)

replace github.com/flachnetz/startup/startup_base => ../startup_base
