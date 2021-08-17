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
	err := l.SendBlocking(event)
	if err != nil {
		log.Errorf("Failed to sent event %+w to kafka: %s", event, err)
	}
}

func (l LogrusEventSender) SendBlocking(event Event) error {
	var buf strings.Builder

	if err := json.NewEncoder(&buf).Encode(event); err != nil {
		l.Errorf("Could not encode event: %+v", event)
		return err
	}

	l.Info(buf.String())
	return nil
}

func (LogrusEventSender) Close() error {
	return nil
}
