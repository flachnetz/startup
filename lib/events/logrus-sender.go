package events

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
	"strings"
)

type LogrusEventSender struct {
	logrus.FieldLogger
}

func (l LogrusEventSender) Init(events []Event) error {
	// noop
	return nil
}

func (l LogrusEventSender) Send(event Event) {
	var buf strings.Builder

	if err := json.NewEncoder(&buf).Encode(event); err != nil {
		l.Warnf("Could not encode event: %+v", event)
		return
	}

	l.Info(buf.String())
}

func (LogrusEventSender) Close() error {
	return nil
}
