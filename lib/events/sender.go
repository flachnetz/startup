package events

import (
	"compress/gzip"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"strings"
	"sync"
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
	io.WriteCloser
}

func (sender WriterEventSender) Send(event Event) {
	bytes, _ := json.Marshal(event)
	sender.Write(bytes)
}

type NoopEventSender struct{}

func (NoopEventSender) Send(event Event) {
}

func (NoopEventSender) Close() error {
	return nil
}

type gzipEventSender struct {
	wg     sync.WaitGroup
	events chan Event
}

func GZIPEventSender(filename string) (*gzipEventSender, error) {
	fp, err := os.Create(filename)
	if err != nil {
		return nil, errors.WithMessage(err, "open file")
	}

	gz, _ := gzip.NewWriterLevel(fp, gzip.BestSpeed)

	sender := &gzipEventSender{
		events: make(chan Event, 1024),
	}

	sender.wg.Add(1)
	go func() {
		defer sender.wg.Done()

		for event := range sender.events {
			bytes, _ := json.Marshal(event)
			gz.Write(bytes)
		}

		gz.Close()
		fp.Close()
	}()

	return sender, nil
}

func (f *gzipEventSender) Send(event Event) {
	f.events <- event
}

func (f *gzipEventSender) Close() error {
	close(f.events)
	f.wg.Wait()
	return nil
}
