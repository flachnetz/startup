package events

import (
	"compress/gzip"
	"encoding/json"
	"github.com/pkg/errors"
	"os"
)

type gzipEventSender struct {
	closeCh chan error
	events  chan Event
}

func (f *gzipEventSender) Init(event []Event) error {
	return nil
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
	_ = f.SendBlocking(event)
}

func (f *gzipEventSender) SendBlocking(event Event) error {
	f.events <- event
	return nil
}

func (f *gzipEventSender) Close() error {
	close(f.events)
	return <-f.closeCh
}
