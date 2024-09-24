package unleash

import (
	"github.com/Unleash/unleash-client-go/v4"
	logrus "github.com/sirupsen/logrus"
)

var logger = logrus.WithField("prefix", "unleash")

// DebugListener is an implementation of all the listener interfaces that simply logs
// debug info. It is meant for debugging purposes and an example of implementing
// the listener interfaces.
type DebugListener struct {
	Verbose bool
}

// OnError prints out errors.
func (l DebugListener) OnError(err error) {
	logger.Errorf("%s", err.Error())
}

// OnWarning prints out warning.
func (l DebugListener) OnWarning(warning error) {
	logger.Warnf("%s", warning.Error())
}

// OnReady prints to the console when the repository is ready.
func (l DebugListener) OnReady() {
	logger.Infof("READY")
}

// OnCount prints to the console when the feature is queried.
func (l DebugListener) OnCount(name string, enabled bool) {
	if l.Verbose {
		logger.Infof("Counted '%s'  as enabled? %v", name, enabled)
	}
}

// OnSent prints to the console when the server has uploaded metrics.
func (l DebugListener) OnSent(payload unleash.MetricsData) {
	if l.Verbose {
		logger.Infof("Sent: %+v", payload)
	}
}

// OnRegistered prints to the console when the client has registered.
func (l DebugListener) OnRegistered(payload unleash.ClientData) {
	logger.Infof("Registered: %+v", payload)
}
