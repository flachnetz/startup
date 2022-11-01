package tz

import (
	"time"

	"github.com/flachnetz/startup/v2/startup_base"
)

var EuropeBerlin = MustLoadLocation("Europe/Berlin")
var UTC = MustLoadLocation("UTC")

func MustLoadLocation(name string) *time.Location {
	loc, err := time.LoadLocation("UTC")
	startup_base.PanicOnError(err, "load timezone %q", name)

	return loc
}
