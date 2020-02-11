package events

import (
	"bytes"
	"github.com/flachnetz/startup/v2/lib/schema"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type avroEncoder struct {
	log      logrus.FieldLogger
	registry schema.Registry
}

func NewAvroEncoder(registry schema.Registry) Encoder {
	return &avroEncoder{
		log:      logrus.WithField("prefix", "avro"),
		registry: registry,
	}
}

func (avro *avroEncoder) Encode(event Event) ([]byte, error) {
	var buffer bytes.Buffer

	hash, err := avro.registerSchema(event.Schema())
	if err != nil {
		return nil, errors.WithMessage(err, "register avro schema")
	}

	// write the hash first, directly followed by the event itself.
	buffer.WriteString(hash)

	if err := event.Serialize(&buffer); err != nil {
		return nil, errors.WithMessage(err, "encoding avro event")
	}

	return buffer.Bytes(), nil
}

func (avro *avroEncoder) registerSchema(schema string) (string, error) {

	key, err := avro.registry.Set(schema)
	if err != nil {
		return "", errors.WithMessage(err, "setting schema in registry")
	}

	return key, nil
}

func (avro *avroEncoder) Close() error {
	return avro.registry.Close()
}
