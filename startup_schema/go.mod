module github.com/flachnetz/startup/startup_schema

go 1.12

require (
	github.com/Landoop/schema-registry v0.0.0-20190327143759-50a5701c1891
	github.com/flachnetz/startup v1.6.4
	github.com/flachnetz/startup/lib/schema v1.1.1
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/flachnetz/startup/startup_consul v1.0.1
	github.com/sirupsen/logrus v1.4.1
)

replace (
	github.com/flachnetz/startup/lib/schema => ../lib/schema
	github.com/flachnetz/startup/startup_base => ../startup_base
	github.com/flachnetz/startup/startup_consul => ../startup_consul
)
