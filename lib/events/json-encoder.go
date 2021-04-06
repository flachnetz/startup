package events

import (
	"encoding/json"
)

type jsonEncoder struct{}

func NewJSONEncoder() Encoder {
	return jsonEncoder{}
}

func (jsonEncoder) Encode(event Event) ([]byte, error) {
	return json.Marshal(event)
}

func (jsonEncoder) Close() error {
	return nil
}
