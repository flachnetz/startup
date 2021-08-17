package events

import (
	"encoding/json"
	"github.com/pkg/errors"
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
	if err := l.SendBlocking(event); err != nil {
		log.Errorf("Failed to log event %+v: %s", event, err)
	}
}

func (l LogrusEventSender) SendBlocking(event Event) error {
	var buf strings.Builder

	if err := json.NewEncoder(&buf).Encode(event); err != nil {
		return errors.WithMessage(err, "encode event")
	}

	l.Info(buf.String())
	return nil
}

func (LogrusEventSender) Close() error {
	return nil
}
