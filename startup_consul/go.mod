module github.com/flachnetz/startup/startup_consul

go 1.12

require (
	github.com/flachnetz/startup/lib/schema v1.1.1
	github.com/flachnetz/startup/startup_base v1.0.0
	github.com/google/btree v1.0.0 // indirect
	github.com/hashicorp/consul/api v1.0.1
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/hashicorp/serf v0.8.3 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/miekg/dns v1.1.8 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/pascaldekloe/goe v0.1.0 // indirect
)

replace (
	github.com/flachnetz/startup/lib/schema => ../lib/schema
	github.com/flachnetz/startup/startup_base => ../startup_base
)
