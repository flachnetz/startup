module github.com/flachnetz/startup/startup_postgres

go 1.12

require (
	github.com/benbjohnson/clock v0.0.0-20161215174838-7dc76406b6d3
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/go-sql-driver/mysql v1.4.1 // indirect
	github.com/golang/protobuf v1.3.2 // indirect
	github.com/jmoiron/sqlx v1.2.0
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/lib/pq v1.2.0
	github.com/mattn/go-colorable v0.1.2 // indirect
	github.com/mattn/go-isatty v0.0.9 // indirect
	github.com/mattn/go-sqlite3 v1.11.0 // indirect
	github.com/onsi/ginkgo v1.9.0 // indirect
	github.com/onsi/gomega v1.6.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/rubenv/sql-migrate v0.0.0-20190717103323-87ce952f7079
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0 // indirect
	github.com/ziutek/mymysql v1.5.4 // indirect
	golang.org/x/crypto v0.0.0-20190820162420-60c769a6c586 // indirect
	golang.org/x/net v0.0.0-20190813141303-74dc4d7220e7 // indirect
	google.golang.org/appengine v1.6.1 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/gorp.v1 v1.7.2 // indirect
)

replace github.com/flachnetz/startup/startup_base => ../startup_base
