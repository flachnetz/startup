package startup_unleash

import (
	"net/http"
	"net/url"
	"sync"

	"github.com/Unleash/unleash-go-sdk/v5"
	"github.com/flachnetz/startup/v2"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_tracing"
)

var initUnleashOnce sync.Once

type Unleash struct {
	Inputs struct {
		Listener any
	}

	BaseUrl  startup.URL `long:"unleash-base-url" env:"UNLEASH_BASE_URL" default:"http://unleash.shared.svc.cluster.local/api" description:"Unleash base URL"`
	ApiToken string      `long:"unleash-api-token" env:"UNLEASH_API_TOKEN" default:"default:production.unleash-insecure-api-token" description:"Unleash API token"`
}

func (o Unleash) Initialize(base startup_base.BaseOptions) {
	InitUnleash(
		base.ServiceName,
		o.BaseUrl.URL,
		o.ApiToken,
		o.Inputs.Listener,
	)
}

func InitUnleash(appName string, unleashUrl *url.URL, token string, listener any) {
	initUnleashOnce.Do(func() {
		err := unleash.Initialize(
			unleash.WithListener(listener),
			unleash.WithAppName(appName),
			unleash.WithUrl(unleashUrl.String()),
			unleash.WithCustomHeaders(http.Header{"Authorization": {token}}),
			unleash.WithHttpClient(startup_tracing.WithSpanPropagation(&http.Client{})),
		)
		startup_base.PanicOnError(err, "init unleash")
	})
}
