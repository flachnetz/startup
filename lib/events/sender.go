package events

import (
	"compress/gzip"
	"encoding/json"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"strings"
)

type LogrusEventSender struct {
	logrus.FieldLogger
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

type WriterEventSender struct {
	io.Writer
}

func (sender WriterEventSender) Send(event Event) {
	bytes, _ := json.Marshal(event)
	_, _ = sender.Write(bytes)
}

func (sender WriterEventSender) Close() error {
	if closer, ok := sender.Writer.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}

type NoopEventSender struct{}

func (NoopEventSender) Send(event Event) {
}

func (NoopEventSender) Close() error {
	return nil
}

type gzipEventSender struct {
	closeCh chan error
	events  chan Event
}

func GZIPEventSender(filename string) (*gzipEventSender, error) {
	fp, err := os.Create(filename)
	if err != nil {
		return nil, errors.WithMessage(err, "open file")
	}

	gz, _ := gzip.NewWriterLevel(fp, gzip.BestSpeed)

	sender := &gzipEventSender{
		events:  make(chan Event, 1024),
		closeCh: make(chan error),
	}

	go func() {
		defer close(sender.closeCh)

		for event := range sender.events {
			bytes, _ := json.Marshal(event)
			_, _ = gz.Write(bytes)
		}

		_ = gz.Close()

		if err := fp.Close(); err != nil {
			sender.closeCh <- err
		}

	}()

	return sender, nil
}

func (f *gzipEventSender) Send(event Event) {
	f.events <- event
}

func (f *gzipEventSender) Close() error {
	close(f.events)
	return <-f.closeCh
}

// A slice of event senders that is also an event sender.
type EventSenders []EventSender

func (senders EventSenders) Send(event Event) {
	for _, sender := range senders {
		sender.Send(event)
	}
}

func (senders EventSenders) Close() error {
	var result error

	for _, sender := range senders {
		if err := sender.Close(); err != nil {
			result = multierror.Append(result, sender.Close())
		}
	}

	return result
}
