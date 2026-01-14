package startup_unleash

import (
	"net/http"
	"net/url"

	"github.com/Unleash/unleash-go-sdk/v5"
	"github.com/flachnetz/startup/v2"
	"github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_tracing"
)

type Unleash struct {
	BaseUrl  startup.URL `long:"unleash-base-url" default:"http://unleash.shared.svc.cluster.local/api" description:"Unleash base URL"`
	ApiToken string      `long:"unleash-api-token" default:"default:production.unleash-insecure-api-token" description:"Unleash API token"`
}

func InitUnleash(appName string, unleashUrl *url.URL, token string, listener interface{}) {
	err := unleash.Initialize(
		unleash.WithListener(listener),
		unleash.WithAppName(appName),
		unleash.WithUrl(unleashUrl.String()),
		unleash.WithCustomHeaders(http.Header{"Authorization": {token}}),
		unleash.WithHttpClient(startup_tracing.WithSpanPropagation(&http.Client{})),
	)
	startup_base.PanicOnError(err, "init unleash")
}
