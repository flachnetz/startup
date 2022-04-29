package events

import (
	"encoding/json"
	"io"
)

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
