module github.com/flachnetz/startup/v2

require (
	github.com/DataDog/datadog-go/v5 v5.3.0
	github.com/Landoop/schema-registry v0.0.0-20190327143759-50a5701c1891
	github.com/Unleash/unleash-client-go/v3 v3.7.4
	github.com/benbjohnson/clock v1.3.5
	github.com/confluentinc/confluent-kafka-go v1.9.2
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/flachnetz/go-admin v1.5.3
	github.com/flachnetz/go-datadog v1.3.1
	github.com/goji/httpauth v0.0.0-20160601135302-2da839ab0f4d
	github.com/gorilla/handlers v1.5.1
	github.com/hashicorp/consul/api v1.22.0
	github.com/jackc/pgx/v5 v5.4.1
	github.com/jessevdk/go-flags v1.5.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/julienschmidt/httprouter v1.3.0
	github.com/lestrrat-go/jwx v1.2.26
	github.com/linkedin/goavro/v2 v2.12.0
	github.com/mattn/go-isatty v0.0.19
	github.com/oklog/ulid v1.3.1
	github.com/opentracing/opentracing-go v1.2.0
	github.com/openzipkin-contrib/zipkin-go-opentracing v0.5.0
	github.com/openzipkin/zipkin-go v0.4.1
	github.com/pkg/errors v0.9.1
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/rubenv/sql-migrate v1.5.1
	github.com/sirupsen/logrus v1.9.3
	go.uber.org/atomic v1.11.0
	golang.org/x/exp v0.0.0-20230626212559-97b1e661b5df
	gopkg.in/go-playground/validator.v9 v9.31.0
)

require (
	github.com/Masterminds/semver/v3 v3.2.1 // indirect
	github.com/Microsoft/go-winio v0.6.1 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.2.0 // indirect
	github.com/fatih/color v1.15.0 // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/go-gorp/gorp/v3 v3.1.0 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.5.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/serf v0.10.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0 // indirect
	github.com/leodido/go-urn v1.2.4 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.1 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/option v1.0.1 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/opentracing-contrib/go-observer v0.0.0-20170622124052-a52f23424492 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/stretchr/testify v1.8.4 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	golang.org/x/crypto v0.10.0 // indirect
	golang.org/x/mod v0.11.0 // indirect
	golang.org/x/sys v0.9.0 // indirect
	golang.org/x/text v0.10.0 // indirect
	golang.org/x/tools v0.10.0 // indirect
	google.golang.org/grpc v1.56.1 // indirect
	gopkg.in/go-playground/assert.v1 v1.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/sirupsen/logrus => github.com/flachnetz/logrus2slog v1.0.5

go 1.21
