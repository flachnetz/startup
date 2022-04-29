package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"strings"
)

type LogrusEventSender struct {
	logrus.FieldLogger
}

func (l LogrusEventSender) Send(event Event) {
	var buf strings.Builder

	if err := json.NewEncoder(&buf).Encode(event); err != nil {
		l.Warnf("Failed to encode event: %s", err)
		return
	}

	l.Info("Non transactional event:", buf.String())
}

func (l LogrusEventSender) SendInTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var buf strings.Builder

	if err := json.NewEncoder(&buf).Encode(event); err != nil {
		return errors.WithMessage(err, "encode event")
	}

	l.Info("Transactional event:", buf.String())
	return nil
}

func (LogrusEventSender) Close() error {
	return nil
}
