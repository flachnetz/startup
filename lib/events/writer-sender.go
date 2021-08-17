package events

import (
	"encoding/json"
	"io"
)

type WriterEventSender struct {
	io.Writer
}

func (sender WriterEventSender) Init(event []Event) error {
	return nil
}

func (sender WriterEventSender) Send(event Event) {
	if err := sender.SendBlocking(event); err != nil {
		log.Errorf("Failed to write event %v: %s", event, err)
	}
}

func (sender WriterEventSender) SendBlocking(event Event) error {
	bytes, _ := json.Marshal(event)

	_, err := sender.Write(bytes)
	return err
}

func (sender WriterEventSender) Close() error {
	if closer, ok := sender.Writer.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}
