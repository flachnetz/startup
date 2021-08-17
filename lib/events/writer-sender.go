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
	err := sender.SendBlocking(event)
	if err != nil {
		log.Errorf("Failed to sent event %+w to kafka: %s", event, err)
	}
}

func (sender WriterEventSender) SendBlocking(event Event) error {
	bytes, _ := json.Marshal(event)
	_, err := sender.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

func (sender WriterEventSender) Close() error {
	if closer, ok := sender.Writer.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}
